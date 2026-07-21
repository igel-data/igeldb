(ns igeldb.core
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :refer [info]]
            [igeldb.compaction :as compaction]
            [igeldb.config :as config]
            [igeldb.data :as data]
            [igeldb.flush :as f]
            [igeldb.memtable :refer [init-memtable]]
            [igeldb.sstable :refer [restore-tree-store]]
            [igeldb.store :as store]
            [igeldb.tx :as tx]
            [igeldb.wal :as wal])
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
          wal-done (wal/spawn-wal-writer poison flush-req-chan flush-wal-chan config)
          compaction-done (compaction/spawn-compaction-worker
                           tree sstable-id compact-pointers registry poison
                           stall-monitor compaction-req-chan config)]
      (->Coordinator flush-req-chan compaction-req-chan poison ready stall-monitor
                     [flush-done wal-done compaction-done]))))

(defn- terminate-workers
  [coordinator]
  (async/close! (:flush-req-chan coordinator))
  (async/close! (:compaction-req-chan coordinator)))

(defn- merge-scan-results
  [mem-ret tree-ret]
  (loop [pairs (transient [])
         m-pairs mem-ret
         t-pairs tree-ret]
    (cond
      (and (empty? m-pairs) (empty? t-pairs))
      (persistent! pairs)
      (and (seq m-pairs) (empty? t-pairs))
      (reduce #(conj %1 %2) (persistent! pairs) m-pairs)
      (and (empty? m-pairs) (seq t-pairs))
      (reduce #(conj %1 %2) (persistent! pairs) t-pairs)
      :else
      (let [[m-key m-data] (first m-pairs)
            [t-key t-data] (first t-pairs)
            [updated m-rest t-rest] (cond
                                      (data/byte-array-equals? m-key t-key)
                                      [(conj! pairs [m-key m-data])
                                       (rest m-pairs) (rest t-pairs)]
                                      (data/byte-array-smaller? m-key t-key)
                                      [(conj! pairs [m-key m-data])
                                       (rest m-pairs) t-pairs]
                                      (data/byte-array-smaller? t-key m-key)
                                      [(conj! pairs [t-key t-data])
                                       m-pairs (rest t-pairs)])]
        (recur updated m-rest t-rest)))))

(defn- await-l0-capacity!
  "Back-pressure: block before enqueueing a write while L0 holds at least
  `l0-stall-threshold` tables, until compaction drains it below the threshold or
  the store is poisoned. The compaction worker notifies `monitor` after each
  compaction; a watch on `poison` notifies it on fail-stop, so a poisoned store
  wakes stalled writers rather than deadlocking. The timed wait is a safety net
  against a missed notification. Returns when unblocked; the caller re-checks
  `poison` and surfaces the error."
  [^Object monitor current-version poison threshold]
  (locking monitor
    (loop []
      (when (and (nil? @poison)
                 (>= (count (first @current-version)) threshold))
        (.wait monitor 200)
        (recur)))))

(defn- unusable-ex
  "Build the rejection exception for an unusable store. `poisoned` is either the
  `:closed` marker (clean shutdown) or the fatal `Throwable` (an IO fault, kept as
  the cause -- but only a Throwable can be an `ex-info` cause, hence the split)."
  [poisoned op]
  (if (= poisoned :closed)
    (ex-info (str (name op) " rejected: the store is closed") {:retriable false})
    (ex-info (str (name op) " rejected: the store is poisoned")
             {:retriable false} poisoned)))

(defn- run-mutation!
  "Run a memtable mutation, applying the settled error classes:

  - Unusable store (fail-stop): if the store has been poisoned by an IO error, or
    closed via `close!`, reject the mutation immediately -- and also if it becomes
    unusable mid-flight. `poison` holds the fatal `Throwable` (a fault) or the
    `:closed` marker; a fault is preserved as the exception cause.
  - Class C (retriable): a collision with a memtable switch throws
    {:retriable true}; retry up to `write-retries` times, then give up. The last
    retriable failure is preserved as the cause.

  Non-retriable exceptions propagate unchanged. Returns `mutate!`'s value on
  success (nil for auto-commit writes; the commit outcome for a tx commit). `op` is
  :write / :delete / :commit-tx, used only for the message."
  [coordinator tree config op mutate!]
  (let [poison (:poison coordinator)
        monitor (:stall-monitor coordinator)
        threshold (:l0-stall-threshold config)]
    (loop [retries (:write-retries config)]
      (when-let [p @poison]
        (throw (unusable-ex p op)))
      ;; Stall before enqueueing while L0 is saturated (woken by a compaction, or
      ;; by the store becoming unusable -- poisoned or closed).
      (await-l0-capacity! monitor (:current-version tree) poison threshold)
      (when-let [p @poison]
        (throw (unusable-ex p op)))
      (let [outcome (try
                      {:ok (mutate!)}
                      (catch clojure.lang.ExceptionInfo e
                        (cond
                          ;; a closed wal-chan looks like a retriable memtable
                          ;; switch; if the store is actually unusable, surface that
                          @poison (throw (unusable-ex @poison op))
                          (-> e ex-data :retriable) {:err e}
                          :else (throw e))))]
        (if (contains? outcome :ok)
          (:ok outcome)
          (if (pos? retries)
            (do (Thread/sleep 100)
                (recur (dec retries)))
            (throw (ex-info (str (name op) " failed repeatedly")
                            {:retriable false} (:err outcome)))))))))

(defn- reject-if-closed!
  "Reads reject once the store is closed (a fully unusable store). A *fault*-
  poisoned store (poison is a Throwable, not `:closed`) still serves reads: the
  committed data on disk / in memory is intact, and Phase 1's fail-stop rejects
  only writes."
  [coordinator op]
  (when (= :closed @(:poison coordinator))
    (throw (ex-info (str (name op) " rejected: the store is closed") {}))))

;; ---- Commit-handler (Step 4) ---------------------------------------------

(defn- conflict?
  "Write-write conflict (first-committer-wins): true if ANY write-set key has a
  committed version newer than the tx's snapshot (its latest committed seq >
  snapshot seq). Blind writes are included -- every key is looked up via the read
  path (`store/latest-seq` on the KVS, bloom-filtered; 0 if never written). Runs
  inside the commit lock."
  [kvs snapshot-seq entries]
  (some (fn [[k _]] (> (or (store/latest-seq kvs k) 0) snapshot-seq)) entries))

(defn commit!
  "The commit-handler: serialize commits under the store's commit lock, holding it
  over {conflict-check -> seq assign -> enqueue one WAL record}. fsync + memtable
  apply stay in the group-commit worker (durability stays there). Assigning the seq
  and enqueuing in the same locked region makes enqueue order == seq order, so the
  worker's FIFO gives WAL order == seq order (the seq analogue of Phase 1's
  WAL-order == apply-order invariant).

  `entries` are folded [user_key data] pairs (one per user_key). `snapshot-seq` is
  the tx's pinned snapshot, or nil for a non-tx auto-commit op -- which skips the
  conflict-check phase (it writes at latest; last-write-wins is always correct, so
  there is no snapshot->commit gap for a conflict to occur in). Returns:
    :committed  durably fsynced and applied
    :conflict   (tx only) a write-set key was committed after the snapshot
    :switched   the memtable switched mid-enqueue; the caller may retry
  Throws {:retriable false} if the worker failed the commit (e.g. an fsync fault;
  the store is then poisoned and the caller's wrapper surfaces the real cause)."
  [kvs snapshot-seq entries]
  (let [registry (:registry kvs)
        commit-lock (:commit-lock kvs)
        memtable (:memtable kvs)
        comp-chan (async/chan)
        outcome (locking commit-lock
                  (if (and snapshot-seq (conflict? kvs snapshot-seq entries))
                    :conflict
                    (let [s (tx/next-seq! registry)
                          record (mapv (fn [[k data]] [(data/->ikey k s) data]) entries)]
                      (if (async/>!! (:wal-chan @memtable) [s record comp-chan])
                        :enqueued
                        :switched))))]
    (case outcome
      :conflict :conflict
      :switched :switched
      :enqueued (case (async/<!! comp-chan)
                  :done :committed
                  ;; :error (fsync fault) or a closed channel: the store is being
                  ;; poisoned; surface a non-retriable failure so run-mutation!
                  ;; re-checks poison and reports the real cause.
                  (throw (ex-info "Commit failed" {:retriable false}))))))

(defn- auto-commit!
  "Non-tx auto-commit of a folded write-set (no snapshot -> no conflict check).
  Adapts `commit!` to `run-mutation!`'s contract: nil on success, a retriable
  ex-info on a memtable switch."
  [kvs entries]
  (case (commit! kvs nil entries)
    :committed nil
    :switched (throw (ex-info "Write failed due to memtable switching"
                              {:retriable true}))))

(defrecord KVS [config memtable immutable-memtable tree registry commit-lock
                coordinator]
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
      (->> (merge-scan-results (merge-scan-results mem-ret imm-ret) tree-ret)
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
    (run-mutation! coordinator tree config :write
                   #(auto-commit! this [[k (data/new-data v)]])))
  (delete!
    [this k]
    (run-mutation! coordinator tree config :delete
                   #(auto-commit! this [[k (data/deleted-data)]])))
  (write-data! [_ _ _] (throw (UnsupportedOperationException.)))
  (write-batch! [_ _] (throw (UnsupportedOperationException.))))

;; ==== Main APIs ====

(def ^:private ^:const SHUTDOWN_TIMEOUT_MS 30000)

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
  safely delete the data directory immediately. The join is bounded by
  `SHUTDOWN_TIMEOUT_MS` so a wedged worker cannot hang `close!` forever."
  [^KVS kvs]
  (info "KVS is shutting down...")
  (let [coordinator (:coordinator kvs)]
    (compare-and-set! (:poison coordinator) nil :closed)
    (terminate-workers coordinator)
    ;; Join the workers. A single shared timeout bounds the total wait: once it
    ;; fires, remaining joins fall through immediately.
    (let [deadline (async/timeout SHUTDOWN_TIMEOUT_MS)]
      (doseq [done (:worker-chans coordinator)]
        (async/alt!!
          done ([_] nil)
          deadline ([_] (info "A background worker did not stop before the shutdown timeout")))))))

(def ^:private ^:const INIT_TIMEOUT_MS 30000)

(defn gen-kvs
  [config-path]
  (let [config (config/load-config config-path)
        [tree sstable-id manifest-max-seq] (restore-tree-store config)
        ;; global commit-order / MVCC state (shared across memtable generations):
        ;; the seq counter + the active-tx snapshot set. init-memtable seeds the
        ;; counter from the replayed WAL's max seq; then fold in the manifest's
        ;; max-seq so next-seq = max(manifest max-seq, WAL max-seq) + 1. The manifest
        ;; part is essential: a clean shutdown right after a flush leaves an empty
        ;; WAL, so the newest seq can live only in SSTables.
        registry (tx/create-registry)
        [wal-id memtable-val] (init-memtable (async/chan) registry config)
        _ (tx/seed-seq! registry (max manifest-max-seq (tx/current-seq registry)))
        memtable (atom memtable-val)
        ;; Holds the memtable currently being flushed (nil when no flush is in
        ;; progress). Reads consult it between the mutable memtable and the
        ;; version, closing the flush-visibility gap (see KVS/select).
        immutable-memtable (atom nil)
        ;; serializes the commit-handler's {conflict-check -> seq assign ->
        ;; enqueue} critical section (see `commit!`)
        commit-lock (Object.)
        coordinator (spawn-bg-workers memtable immutable-memtable tree registry
                                      (atom sstable-id) (atom wal-id) config)]
    ;; Block until the initial flush has established the first WAL generation
    ;; (and, on restart, committed the replayed WAL to an SSTable).
    (when-not (true? (deref (:ready coordinator) INIT_TIMEOUT_MS :timeout))
      (throw (ex-info "Initializing KVS failed" {} @(:poison coordinator))))
    (->KVS config memtable immutable-memtable tree registry commit-lock
           coordinator)))

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
  Reading `current-seq` and registering happen under the commit lock so no commit
  can advance the seq in between -- while the lock is held the seq is frozen, so a
  concurrent GC-floor computation that misses the registration still sees no version
  newer than this snapshot, keeping the floor safe."
  [^KVS kvs]
  (reject-if-closed! (:coordinator kvs) :begin-tx)
  (let [registry (:registry kvs)
        commit-lock (:commit-lock kvs)
        snapshot-seq (locking commit-lock
                       (let [s (tx/current-seq registry)]
                         (tx/register-snapshot! registry s)
                         s))]
    (->Tx kvs snapshot-seq (atom (empty-write-set)) (atom false))))

(defn- finish!
  "Deregister the tx's snapshot exactly once (idempotent via CAS), so an explicit
  commit/rollback plus `with-tx`'s finally never double-decrement the active set."
  [^Tx tx]
  (when (compare-and-set! (:finished? tx) false true)
    (tx/deregister-snapshot! (:registry (:kvs tx)) (:snapshot-seq tx))))

(defn tx-get
  "Read `k` inside the tx: the tx's own buffered write wins (read-your-writes),
  otherwise the store at the tx's snapshot seq. Returns the value bytes, or nil for
  an absent key or the tx's own tombstone."
  [^Tx tx ^bytes k]
  (let [ws @(:write-set tx)]
    (if (contains? ws k)
      (let [d (get ws k)] (when (data/is-valid? d) (:value d)))
      (store/select (:kvs tx) k (:snapshot-seq tx)))))

(defn tx-put
  "Buffer a write of `k`=`v` into the tx's write-set (folding an earlier write of
  the same key). Not durable until `commit-tx`."
  [^Tx tx ^bytes k ^bytes v]
  (swap! (:write-set tx) assoc k (data/new-data v))
  nil)

(defn tx-delete
  "Buffer a delete of `k` into the tx's write-set (a tombstone). Not durable until
  `commit-tx`."
  [^Tx tx ^bytes k]
  (swap! (:write-set tx) assoc k (data/deleted-data))
  nil)

(defn- tx-commit-attempt!
  "One commit-handler attempt for a tx: :committed / :conflict, or a retriable throw
  on a memtable switch (so `run-mutation!` retries the enqueue)."
  [kvs snapshot-seq entries]
  (case (commit! kvs snapshot-seq entries)
    :committed :committed
    :conflict :conflict
    :switched (throw (ex-info "commit hit a memtable switch" {:retriable true}))))

(defn commit-tx
  "Commit the transaction. An empty write-set (a read-only tx) is a no-op. Otherwise
  the whole write-set goes through the commit-handler at the tx's snapshot seq
  (write-write conflict detection, first-committer-wins). Returns `:committed`; on a
  conflict throws an ex-info tagged `:igeldb/conflict` (retriable) that the caller
  may catch to retry -- the tx is rolled back (deregistered) either way. The snapshot
  is always deregistered exactly once."
  [^Tx tx]
  (let [{:keys [kvs snapshot-seq write-set]} tx
        ws @write-set]
    (try
      (if (empty? ws)
        :committed
        (let [result (run-mutation! (:coordinator kvs) (:tree kvs) (:config kvs)
                                    :commit-tx
                                    #(tx-commit-attempt! kvs snapshot-seq (vec ws)))]
          (if (= result :conflict)
            (throw (ex-info "Transaction conflict; the write-set was committed by another tx"
                            {:igeldb/conflict true :retriable true}))
            :committed)))
      (finally (finish! tx)))))

(defn rollback-tx
  "Abort the transaction: discard the buffered write-set and deregister the snapshot.
  Idempotent with `commit-tx` (the snapshot is deregistered exactly once)."
  [^Tx tx]
  (reset! (:write-set tx) (empty-write-set))
  (finish! tx)
  nil)

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
         (rollback-tx ~tx-sym)
         (throw e#)))))
