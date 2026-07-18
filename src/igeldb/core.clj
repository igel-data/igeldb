(ns igeldb.core
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :refer [info]]
            [igeldb.config :as config]
            [igeldb.data :as data]
            [igeldb.flush :as f]
            [igeldb.memtable :refer [init-memtable]]
            [igeldb.sstable :refer [restore-tree-store]]
            [igeldb.store :as store]
            [igeldb.wal :as wal])
  (:gen-class))

;; `poison` holds the fatal exception once an IO error trips fail-stop (nil while
;; healthy). The WAL worker and flush writer set it; every write/delete checks it.
;; `ready` is delivered once the initial flush has established the first WAL
;; generation (true on success, false if poisoned during init).
(defrecord BgWorkers [wal-handler flush-writer flush-req-chan poison ready])

(defn spawn-bg-workers
  [memtable tree sstable-id wal-id config]
  (let [flush-req-chan (async/chan)
        flush-wal-chan (async/chan)
        poison (atom nil)
        ready (promise)]
    (->BgWorkers
     (f/spawn-flush-writer memtable tree sstable-id wal-id poison ready
                           flush-req-chan
                           flush-wal-chan
                           config)
     (wal/spawn-wal-writer poison
                           flush-req-chan
                           flush-wal-chan
                           config)
     flush-req-chan
     poison
     ready)))

(defn- terminate-flush-writer
  [coordinator]
  (async/close! (:flush-req-chan coordinator)))

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

(defn- run-mutation!
  "Run a memtable mutation, applying the two settled error classes:

  - Class B (fail-stop): if the store has been poisoned by an IO error, reject
    the mutation immediately, and also if it becomes poisoned mid-flight. The
    original fsync/flush failure is preserved as the exception cause.
  - Class C (retriable): a collision with a memtable switch throws
    {:retriable true}; retry up to `write-retries` times, then give up. The last
    retriable failure is preserved as the cause.

  Non-retriable exceptions propagate unchanged. `op` is :write or :delete,
  used only for the message."
  [poison config op mutate!]
  (loop [retries (:write-retries config)]
    (when-let [e @poison]
      (throw (ex-info (str (name op) " rejected: the store is poisoned")
                      {:retriable false} e)))
    (let [err (try
                (mutate!)
                nil
                (catch clojure.lang.ExceptionInfo e
                  (cond
                    @poison
                    (throw (ex-info (str (name op) " failed: the store is poisoned")
                                    {:retriable false} @poison))
                    (-> e ex-data :retriable) e
                    :else (throw e))))]
      (when err
        (if (pos? retries)
          (do (Thread/sleep 100)
              (recur (dec retries)))
          (throw (ex-info (str (name op) " failed repeatedly")
                          {:retriable false} err)))))))

(defrecord KVS [config memtable tree workers]
  store/IStoreRead
  (select
    [_ k]
    (let [data (or (store/select @memtable k) (store/select tree k))]
      (when (data/is-valid? data) (:value data))))
  (scan
    [_ from-key to-key]
    (->> (merge-scan-results (store/scan @memtable from-key to-key)
                             (store/scan tree from-key to-key))
         (filter (fn [[_ data]] (data/is-valid? data)))
         (map (fn [[k data]] [k (:value data)]))))

  store/IStoreMutate
  (write!
    [_ k v]
    (run-mutation! (:poison workers) config :write
                   #(store/write! @memtable k v)))
  (delete!
    [_ k]
    (run-mutation! (:poison workers) config :delete
                   #(store/delete! @memtable k)))
  (write-data! [_ _ _] (throw (UnsupportedOperationException.)))

  Object
  (finalize [_]
    (info "KVS is shutting down...")
    (terminate-flush-writer workers)))

;; ==== Main APIs ====

(def ^:private ^:const INIT_TIMEOUT_MS 30000)

(defn gen-kvs
  [config-path]
  (let [config (config/load-config config-path)
        [tree sstable-id] (restore-tree-store config)
        [wal-id memtable-val] (init-memtable (async/chan) config)
        memtable (atom memtable-val)
        workers (spawn-bg-workers memtable tree (atom sstable-id) (atom wal-id)
                                  config)]
    ;; Block until the initial flush has established the first WAL generation
    ;; (and, on restart, committed the replayed WAL to an SSTable).
    (when-not (true? (deref (:ready workers) INIT_TIMEOUT_MS :timeout))
      (throw (ex-info "Initializing KVS failed" {} @(:poison workers))))
    (->KVS config memtable tree workers)))

(defn select
  "Read the value corresponding to the given key.
  If the key doesn't exist, it returns nil."
  [^KVS kvs ^bytes k]
  (store/select kvs k))

(defn scan
  "Read the key-value pairs between the `from-key` and the `to-key`.
  This range should include `from-key` and not include `to-key`.
  It returns key-value pair vectors like [[k0 v0] [k1 v1]].
  The keys should be ordered by ascending."
  [^KVS kvs ^bytes from-key ^bytes to-key]
  (store/scan kvs from-key to-key))

(defn write!
  "Write the new value correponding to the given key."
  [^KVS kvs ^bytes k ^bytes v]
  (store/write! kvs k v))

(defn delete!
  "Delete the given key from the key-value store."
  [^KVS kvs ^bytes k]
  (store/delete! kvs k))
