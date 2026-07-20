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
            [igeldb.wal :as wal])
  (:gen-class))

;; The Coordinator is the control plane the foreground (KVS) uses to interact
;; with the background workers. It deliberately does NOT hold the workers
;; themselves: the flush writer is a real thread and the WAL worker is a go-loop
;; parked on reachable channels, so both stay alive on their own.
;;
;; `poison` holds the fatal exception once an IO error trips fail-stop (nil while
;; healthy). The WAL worker and flush writer set it; every write/delete checks it.
;; `ready` is delivered once the initial flush has established the first WAL
;; generation (true on success, false if poisoned during init).
;; `stall-monitor` is the back-pressure condition: writers wait on it while L0
;; is saturated; the compaction worker and the poison watch notify it.
(defrecord Coordinator [flush-req-chan compaction-req-chan poison ready
                        stall-monitor])

(defn spawn-bg-workers
  [memtable tree sstable-id wal-id config]
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
    ;; Spawn the workers for their side effects; their return channels are
    ;; unused (see the Coordinator note above).
    (f/spawn-flush-writer memtable tree sstable-id wal-id poison ready
                          flush-req-chan flush-wal-chan compaction-req-chan config)
    (wal/spawn-wal-writer poison flush-req-chan flush-wal-chan config)
    (compaction/spawn-compaction-worker tree sstable-id compact-pointers poison
                                        stall-monitor compaction-req-chan config)
    (->Coordinator flush-req-chan compaction-req-chan poison ready stall-monitor)))

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

  Non-retriable exceptions propagate unchanged. `op` is :write or :delete,
  used only for the message."
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
      (let [err (try
                  (mutate!)
                  nil
                  (catch clojure.lang.ExceptionInfo e
                    (cond
                      ;; a closed wal-chan looks like a retriable memtable switch;
                      ;; if the store is actually unusable, surface that instead
                      @poison (throw (unusable-ex @poison op))
                      (-> e ex-data :retriable) e
                      :else (throw e))))]
        (when err
          (if (pos? retries)
            (do (Thread/sleep 100)
                (recur (dec retries)))
            (throw (ex-info (str (name op) " failed repeatedly")
                            {:retriable false} err))))))))

(defn- reject-if-closed!
  "Reads reject once the store is closed (a fully unusable store). A *fault*-
  poisoned store (poison is a Throwable, not `:closed`) still serves reads: the
  committed data on disk / in memory is intact, and Phase 1's fail-stop rejects
  only writes."
  [coordinator op]
  (when (= :closed @(:poison coordinator))
    (throw (ex-info (str (name op) " rejected: the store is closed") {}))))

(defrecord KVS [config memtable tree coordinator]
  store/IStoreRead
  (select
    [_ k snapshot-seq]
    (reject-if-closed! coordinator :select)
    ;; memtable (newest data) wins over the tree; each returns the newest version
    ;; of k with seq <= snapshot-seq
    (let [data (or (store/select @memtable k snapshot-seq)
                   (store/select tree k snapshot-seq))]
      (when (data/is-valid? data) (:value data))))
  (scan
    [_ from-key to-key snapshot-seq]
    (reject-if-closed! coordinator :scan)
    (->> (merge-scan-results (store/scan @memtable from-key to-key snapshot-seq)
                             (store/scan tree from-key to-key snapshot-seq))
         (filter (fn [[_ data]] (data/is-valid? data)))
         (map (fn [[k data]] [k (:value data)]))))

  store/IStoreMutate
  (write!
    [_ k v]
    (run-mutation! coordinator tree config :write
                   #(store/write! @memtable k v)))
  (delete!
    [_ k]
    (run-mutation! coordinator tree config :delete
                   #(store/delete! @memtable k)))
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
  store still reports its original cause."
  [^KVS kvs]
  (info "KVS is shutting down...")
  (compare-and-set! (:poison (:coordinator kvs)) nil :closed)
  (terminate-workers (:coordinator kvs)))

(def ^:private ^:const INIT_TIMEOUT_MS 30000)

(defn gen-kvs
  [config-path]
  (let [config (config/load-config config-path)
        [tree sstable-id] (restore-tree-store config)
        ;; global commit-order sequence counter (shared across memtable
        ;; generations); init-memtable seeds it from the replayed WAL's max seq
        seq-counter (atom 0)
        [wal-id memtable-val] (init-memtable (async/chan) seq-counter config)
        memtable (atom memtable-val)
        coordinator (spawn-bg-workers memtable tree (atom sstable-id)
                                      (atom wal-id) config)]
    ;; Block until the initial flush has established the first WAL generation
    ;; (and, on restart, committed the replayed WAL to an SSTable).
    (when-not (true? (deref (:ready coordinator) INIT_TIMEOUT_MS :timeout))
      (throw (ex-info "Initializing KVS failed" {} @(:poison coordinator))))
    (->KVS config memtable tree coordinator)))

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
