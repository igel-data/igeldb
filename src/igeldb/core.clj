(ns igeldb.core
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :refer [info]]
            [igeldb.commit :as commit]
            [igeldb.compaction :as compaction]
            [igeldb.config :as config]
            [igeldb.data :as data]
            [igeldb.flush :as f]
            [igeldb.lock :as db-lock]
            [igeldb.memtable :refer [init-memtable]]
            [igeldb.scan :as scan-impl]
            [igeldb.sstable :as sstable :refer [restore-tree-store]]
            [igeldb.store :as store]
            [igeldb.tx :as tx]
            [igeldb.wal :as wal]
            [igeldb.write :as write-path])
  (:gen-class))

;; The Coordinator is the control plane the foreground (KVS) uses to interact with
;; the background workers. The workers run on their own (the flush writer and
;; compaction worker are real threads, the WAL worker a go-loop); the Coordinator
;; drives them through channels and keeps each worker's completion channel
;; (`worker-chans`) so `close!` can join on shutdown -- see below.
;;
;; `poison` holds the fatal exception once an IO error trips fail-stop (nil while
;; healthy). The WAL worker and flush writer set it; every write/delete checks it.
;; `ready` is delivered once the initial flush has established the first WAL
;; generation (true on success, false if poisoned during init).
;; `stall-monitor` is the back-pressure condition: writers wait on it while L0
;; is saturated; the compaction worker and the poison watch notify it.
;; `worker-chans` are the `async/thread`/`go-loop` channels of the three background
;; workers (flush writer, WAL worker, compaction worker); each closes when its
;; worker exits, so `close!` can join on them and return only once no worker is
;; still touching files.
(defrecord Coordinator [flush-req-chan compaction-req-chan poison ready
                        stall-monitor worker-chans])

(defn spawn-bg-workers
  [memtable immutable-memtable tree registry sstable-id wal-id config]
  (let [flush-req-chan (async/chan)
        flush-wal-chan (async/chan)
        ;; sliding-buffer so a flush never blocks signaling and signals coalesce
        compaction-req-chan (async/chan (async/sliding-buffer 1))
        compact-pointers (atom {})
        poison (atom nil)
        ready (promise)
        stall-monitor (Object.)]
    ;; Fail-stop must wake stalled writers: when the store is poisoned (by any
    ;; worker), notify the back-pressure monitor so blocked writers re-check and
    ;; return the error instead of hanging.
    (add-watch poison ::wake-stalled-writers
               (fn [_ _ old new]
                 (when (and (nil? old) new)
                   (locking stall-monitor (.notifyAll stall-monitor)))))
    ;; Spawn the workers; keep each one's completion channel so `close!` can join
    ;; on it (a worker's channel closes when the worker exits).
    (let [flush-done (f/spawn-flush-writer memtable immutable-memtable tree
                                           sstable-id wal-id poison ready
                                           flush-req-chan flush-wal-chan
                                           compaction-req-chan config)
          wal-done (wal/spawn-wal-writer registry poison flush-req-chan
                                         flush-wal-chan config)
          compaction-done (compaction/spawn-compaction-worker
                           tree sstable-id compact-pointers registry poison
                           stall-monitor compaction-req-chan config)]
      (->Coordinator flush-req-chan compaction-req-chan poison ready stall-monitor
                     [flush-done wal-done compaction-done]))))

(defn- terminate-workers
  [coordinator]
  (async/close! (:flush-req-chan coordinator))
  (async/close! (:compaction-req-chan coordinator)))

(defn- reject-if-closed!
  "Reads reject once the store is closed (a fully unusable store). A *fault*-
  poisoned store (poison is a Throwable, not `:closed`) still serves reads: the
  committed data on disk / in memory is intact, and Phase 1's fail-stop rejects
  only writes."
  [coordinator op]
  (when (= :closed @(:poison coordinator))
    (throw (ex-info (str (name op) " rejected: the store is closed") {}))))

(defrecord KVS [config memtable immutable-memtable tree registry commit-lock
                coordinator directory-locks]
  store/IStoreRead
  (select
    [_ k snapshot-seq]
    (reject-if-closed! coordinator :select)
    ;; Read precedence: mutable memtable -> immutable memtable (the one being
    ;; flushed, if a flush is in progress) -> version. Each returns the newest
    ;; version of k with seq <= snapshot-seq. The immutable-memtable consult
    ;; closes the flush-visibility gap: while `switch-memtable!` has installed a
    ;; fresh memtable but the flush is not yet committed to a version, the
    ;; switched-out data is still visible here.
    (let [imm @immutable-memtable
          data (or (store/select @memtable k snapshot-seq)
                   (when imm (store/select imm k snapshot-seq))
                   (store/select tree k snapshot-seq))]
      (when (data/is-valid? data) (:value data))))
  (scan
    [_ from-key to-key snapshot-seq]
    (reject-if-closed! coordinator :scan)
    (let [imm @immutable-memtable
          mem-ret (store/scan @memtable from-key to-key snapshot-seq)
          imm-ret (when imm (store/scan imm from-key to-key snapshot-seq))
          tree-ret (store/scan tree from-key to-key snapshot-seq)]
      ;; Merge in descending precedence: mutable > immutable > version. Each
      ;; merge keeps the first argument's value on equal user_keys, so chaining
      ;; preserves that precedence.
      (->> (scan-impl/merge-results
            (scan-impl/merge-results mem-ret imm-ret)
            tree-ret)
           (filter (fn [[_ data]] (data/is-valid? data)))
           (map (fn [[k data]] [k (:value data)])))))
  (latest-seq
    [_ k]
    ;; the newest committed seq for k across mutable memtable -> immutable memtable
    ;; -> tree. Higher-precedence sources hold newer versions (post-flush writes get
    ;; higher seqs than the flushed data), so the first source that contains k gives
    ;; its latest seq. nil if k was never written. Drives conflict detection.
    (or (store/latest-seq @memtable k)
        (when-let [imm @immutable-memtable] (store/latest-seq imm k))
        (store/latest-seq tree k)))

  store/IStoreMutate
  (write!
    [this k v]
    (write-path/run-mutation! coordinator tree config :write
                              #(commit/auto-commit! this [[k (data/new-data v)]])))
  (delete!
    [this k]
    (write-path/run-mutation! coordinator tree config :delete
                              #(commit/auto-commit! this [[k (data/deleted-data)]])))
  (write-data! [_ _ _] (throw (UnsupportedOperationException.)))
  (write-batch! [_ _] (throw (UnsupportedOperationException.))))

;; ==== Main APIs ====

(defn close!
  "Shut the store down: reject further writes, then stop the background workers.
  Explicit shutdown (rather than an `Object.finalize` hook -- GC finalization is
  unreliable and unsupported under Babashka's SCI records).

  Closing marks the store unusable via the same `poison` atom used for fail-stop
  (with a `:closed` marker instead of a fault), so every subsequent operation --
  `write!`/`delete!` and `select`/`scan` -- fails fast with a clear \"store is
  closed\" error, and any stalled writer is woken (the poison watch fires).
  `compare-and-set!` leaves an existing fault in place so a poisoned-then-closed
  store still reports its original cause.

  `close!` is synchronous: after signaling shutdown it JOINS the background workers
  (each worker's channel closes when it exits), so once `close!` returns no flush or
  compaction thread is still reading/writing SSTable or WAL files -- a caller may
  safely delete the data directory immediately."
  [^KVS kvs]
  (info "KVS is shutting down...")
  (let [coordinator (:coordinator kvs)]
    (compare-and-set! (:poison coordinator) nil :closed)
    (terminate-workers coordinator)
    ;; Join every worker without a deadline. Returning while even one worker is
    ;; still alive would violate close!'s synchronization guarantee and allow a
    ;; caller to reopen or delete the data directory while it is still in use.
    (doseq [done (:worker-chans coordinator)]
      (async/<!! done))
    (try
      ;; Workers are stopped, so nothing is mid-read: release the cached SSTable
      ;; read channels. Leaking these would leak file descriptors.
      (sstable/close-all-channels! (:tree kvs))
      (finally
        ;; Release ownership last. Once this happens, another IgelDB instance may
        ;; open these directories.
        (db-lock/release-all! (:directory-locks kvs))))))

(def ^:private ^:const INIT_TIMEOUT_MS 30000)

(defn gen-kvs
  "Open an IgelDB store from `config-path`.

  The configured SSTable and WAL directories are held with exclusive filesystem
  locks until `close!` returns. If either directory is already owned by another
  IgelDB instance or process, throws an ex-info tagged
  `:igeldb/directory-locked`."
  [config-path]
  (let [config (config/load-config config-path)
        directory-locks (db-lock/acquire-all!
                         [(:sstable-dir config) (:wal-dir config)])]
    (try
      (let [[tree sstable-id manifest-max-seq] (restore-tree-store config)
            ;; global commit-order / MVCC state (shared across memtable generations):
            ;; the seq counter + the active-tx snapshot set. init-memtable seeds the
            ;; counter from the replayed WAL's max seq; then fold in the manifest's
            ;; max-seq so next-seq = max(manifest max-seq, WAL max-seq) + 1. The
            ;; manifest part is essential: a clean shutdown right after a flush
            ;; leaves an empty WAL, so the newest seq can live only in SSTables.
            registry (tx/create-registry)
            [wal-id memtable-val] (init-memtable (async/chan) registry config)
            _ (tx/seed-seq! registry (max manifest-max-seq
                                          (tx/current-seq registry)))
            memtable (atom memtable-val)
            ;; Holds the memtable currently being flushed (nil when no flush is in
            ;; progress). Reads consult it between the mutable memtable and the
            ;; version, closing the flush-visibility gap (see KVS/select).
            immutable-memtable (atom nil)
            ;; serializes the commit-handler's {conflict-check -> seq assign ->
            ;; enqueue} critical section (see `commit/commit!`)
            commit-lock (Object.)
            coordinator (spawn-bg-workers memtable immutable-memtable tree registry
                                          (atom sstable-id) (atom wal-id) config)
            kvs (->KVS config memtable immutable-memtable tree registry commit-lock
                       coordinator directory-locks)]
        ;; Block until the initial flush has established the first WAL generation
        ;; (and, on restart, committed the replayed WAL to an SSTable).
        (when-not (true? (deref (:ready coordinator) INIT_TIMEOUT_MS :timeout))
          ;; Do not release directory ownership until the partially started workers
          ;; have stopped.
          (let [cause @(:poison coordinator)]
            (close! kvs)
            (throw (ex-info "Initializing KVS failed" {} cause))))
        kvs)
      (catch Throwable e
        (db-lock/release-all! directory-locks)
        (throw e)))))

(defn select
  "Read the value corresponding to the given key.
  If the key doesn't exist, it returns nil."
  [^KVS kvs ^bytes k]
  (store/select kvs k Long/MAX_VALUE))

(defn scan
  "Read the key-value pairs between the `from-key` and the `to-key`.
  This range should include `from-key` and not include `to-key`.
  It returns key-value pair vectors like [[k0 v0] [k1 v1]].
  The keys should be ordered by ascending."
  [^KVS kvs ^bytes from-key ^bytes to-key]
  (store/scan kvs from-key to-key Long/MAX_VALUE))

(defn write!
  "Write the new value correponding to the given key."
  [^KVS kvs ^bytes k ^bytes v]
  (store/write! kvs k v))

(defn delete!
  "Delete the given key from the key-value store."
  [^KVS kvs ^bytes k]
  (store/delete! kvs k))

;; ==== Transactions (snapshot isolation) ====
;;
;; A transaction pins a snapshot = a fixed seq at `begin-tx` (NOT a held version --
;; reads take the current version briefly and filter by this seq; the GC floor keeps
;; every version the snapshot needs alive, see `igeldb.tx`). Reads/writes buffer into
;; a write-set; `commit-tx` runs the whole write-set through the commit-handler under
;; standard snapshot isolation (write-write conflicts detected, first-committer-wins;
;; write skew is allowed). A read-only tx (empty write-set) commits for free.

(defn- empty-write-set
  "A write-set is a sorted-map keyed by user_key (content equality via the
  byte-array comparator), value a `Data` (a value or a tombstone). Same-key writes
  fold to the last one."
  []
  (sorted-map-by (data/byte-array-comparator)))

(defrecord Tx [kvs snapshot-seq write-set finished?])

(defn begin-tx
  "Start a transaction: pin the snapshot seq, register it in the active set (so the
  GC floor preserves the versions it can see), and start with an empty write-set.

  The snapshot is pinned to `applied` -- the max seq the memtable has actually
  applied -- NOT `current-seq`. Every seq <= `applied` is materializable by the
  read path, so `tx-get` always sees a consistent snapshot; a seq assigned but not
  yet applied is not externally committed (commits block until apply), so leaving
  it out is correct, not stale. Pin + register happen under the commit lock (the
  version pin and lock are unchanged); the floor is only made more conservative,
  since applied snapshots are <= the old current-seq ones (see Bug-2 spec)."
  [^KVS kvs]
  (reject-if-closed! (:coordinator kvs) :begin-tx)
  (let [registry (:registry kvs)
        commit-lock (:commit-lock kvs)
        snapshot-seq (locking commit-lock
                       (let [s (tx/current-applied registry)]
                         (tx/register-snapshot! registry s)
                         s))]
    (->Tx kvs snapshot-seq (atom (empty-write-set)) (atom false))))

(defn- finish!
  "Mark the tx finished and deregister its snapshot exactly once."
  [^Tx tx]
  (when (compare-and-set! (:finished? tx) false true)
    (tx/deregister-snapshot! (:registry (:kvs tx)) (:snapshot-seq tx))))

(defn- reject-if-finished!
  [^Tx tx op]
  (when @(:finished? tx)
    (throw (ex-info (str (name op) " rejected: the transaction is finished")
                    {:igeldb/tx-closed true
                     :op op
                     :retriable false}))))

(defn tx-get
  "Read `k` inside the tx: the tx's own buffered write wins (read-your-writes),
  otherwise the store at the tx's snapshot seq. Returns the value bytes, or nil for
  an absent key or the tx's own tombstone. Rejects a finished transaction."
  [^Tx tx ^bytes k]
  (locking tx
    (reject-if-finished! tx :tx-get)
    (let [ws @(:write-set tx)]
      (if (contains? ws k)
        (let [d (get ws k)] (when (data/is-valid? d) (:value d)))
        (store/select (:kvs tx) k (:snapshot-seq tx))))))

(defn tx-put
  "Buffer a write of `k`=`v` into the tx's write-set (folding an earlier write of
  the same key). Not durable until `commit-tx`. Rejects a finished transaction."
  [^Tx tx ^bytes k ^bytes v]
  (locking tx
    (reject-if-finished! tx :tx-put)
    (swap! (:write-set tx) assoc k (data/new-data v))
    nil))

(defn tx-delete
  "Buffer a delete of `k` into the tx's write-set (a tombstone). Not durable until
  `commit-tx`. Rejects a finished transaction."
  [^Tx tx ^bytes k]
  (locking tx
    (reject-if-finished! tx :tx-delete)
    (swap! (:write-set tx) assoc k (data/deleted-data))
    nil))

(defn commit-tx
  "Commit the transaction. An empty write-set (a read-only tx) is a no-op. Otherwise
  the whole write-set goes through the commit-handler at the tx's snapshot seq
  (write-write conflict detection, first-committer-wins). Returns `:committed`; on a
  conflict throws an ex-info tagged `:igeldb/conflict` (retriable) that the caller
  may catch to retry -- the tx is rolled back (deregistered) either way. The snapshot
  is always deregistered exactly once. Rejects a finished transaction."
  [^Tx tx]
  (locking tx
    (reject-if-finished! tx :commit-tx)
    (let [{:keys [kvs snapshot-seq write-set]} tx
          ws @write-set]
      (try
        (if (empty? ws)
          :committed
          (let [result (write-path/run-mutation!
                        (:coordinator kvs) (:tree kvs) (:config kvs) :commit-tx
                        #(commit/tx-commit-attempt! kvs snapshot-seq (vec ws)))]
            (if (= result :conflict)
              (throw (ex-info "Transaction conflict; the write-set was committed by another tx"
                              {:igeldb/conflict true :retriable true}))
              :committed)))
        (finally (finish! tx))))))

(defn rollback-tx
  "Abort the transaction: discard the buffered write-set and deregister the snapshot.
  Rejects a finished transaction."
  [^Tx tx]
  (locking tx
    (reject-if-finished! tx :rollback-tx)
    (reset! (:write-set tx) (empty-write-set))
    (finish! tx)
    nil))

(defmacro with-tx
  "Run `body` in a transaction bound to `tx-sym`: begin, evaluate the body, then
  commit on normal completion (returning the body's value) or roll back if the body
  throws (re-throwing). A commit conflict throws an `:igeldb/conflict` ex-info out of
  `with-tx`; it is NOT auto-retried -- catch it to retry the whole transaction."
  [[tx-sym kvs] & body]
  `(let [~tx-sym (begin-tx ~kvs)]
     (try
       (let [result# (do ~@body)]
         (commit-tx ~tx-sym)
         result#)
       (catch Throwable e#
         ;; A failed commit finishes the tx in its `finally`; do not replace its
         ;; conflict/IO exception with a secondary "transaction is finished".
         (when-not @(:finished? ~tx-sym)
           (rollback-tx ~tx-sym))
         (throw e#)))))
