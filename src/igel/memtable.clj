(ns igel.memtable
  (:require [clojure.core.async :as async]
            [igel.data :as data]
            [igel.store :as store :refer [select scan write! delete!]]
            [igel.wal :as wal]))

(defrecord MemStore [^java.util.TreeMap mem]
  store/IStoreRead
  (select
    [_ k]
    (let [data (.get mem k)]
      (when (seq data)
        (if (data/is-valid? data) data (data/deleted-data)))))
  (scan
    [_ from-key to-key]
    (some->> (.subMap mem from-key true to-key false)
             .entrySet
             (map
              (fn [e]
                (let [k (.getKey e)
                      data (.getValue e)]
                  (if (data/is-valid? data)
                    [k data]
                    [k (data/deleted-data)]))))))

  store/IStoreMutate
  (write!
    [_ k v]
    (.put mem k (data/new-data v)))
  (write-data!
    [_ k data]
    (.put mem k data))
  (delete!
    [_ k]
    (.put mem k (data/deleted-data))))

(defrecord Memtable [mem wal-chan size]
  store/IStoreRead
  ;; TODO: need concurrent BTreeMap
  (select
    [_ k]
    (locking mem
      (select mem k)))
  (scan
    [_ from-key to-key]
    (locking mem
      (scan mem from-key to-key)))

  store/IStoreMutate
  ;; Writers only enqueue to the WAL channel and wait for the group-commit
  ;; worker to signal completion. The worker (a single thread) appends to the
  ;; WAL, fsyncs, then applies the entry to the memtable and updates `size` --
  ;; the writer never touches the memtable or holds a lock across the fsync.
  ;; This keeps WAL-append order == memtable-apply order and makes rollback on
  ;; fsync failure unnecessary (nothing is applied until fsync succeeds).
  (write!
    [_ k v]
    (let [comp-chan (async/chan)]
      (when-not (async/>!! wal-chan [k (data/new-data v) comp-chan])
        (throw (ex-info "Write failed due to memtable switching"
                        {:retriable true})))
      (case (async/<!! comp-chan)
        :done nil
        nil (throw (ex-info "Write failed due to memtable switching"
                            {:retriable true}))
        (throw (ex-info "Write failed" {:retriable false})))))
  (write-data! [_ _ _] (throw (UnsupportedOperationException.)))
  (delete!
    [_ k]
    (let [comp-chan (async/chan)]
      (when-not (async/>!! wal-chan [k (data/deleted-data) comp-chan])
        (throw (ex-info "Delete failed due to memtable switching"
                        {:retriable true})))
      (case (async/<!! comp-chan)
        :done nil
        nil (throw (ex-info "Delete failed due to memtable switching"
                            {:retriable true}))
        (throw (ex-info "Delete failed" {:retriable false}))))))

(defn create-memtable
  "Create the new memtable"
  [wal-chan]
  (->Memtable (->MemStore (new java.util.TreeMap (data/byte-array-comparator)))
              wal-chan
              (atom 0)))

(defn init-memtable
  "Initialize the memtable. Restore data from WAL to the new memtable."
  [wal-chan config]
  (let [store (->MemStore (new java.util.TreeMap (data/byte-array-comparator)))
        wal-pairs (wal/load-existing-wal config)
        memtable-size (if (empty? wal-pairs) 0 (:memtable-size config))]
    ;; `load-existing-wal` returns [k data] entries (data is a value or a
    ;; tombstone). Apply them in WAL order so the replayed memtable matches the
    ;; pre-crash state.
    (doseq [[k data] wal-pairs]
      (store/write-data! store k data))
    (->Memtable store wal-chan (atom memtable-size))))

(defn entry-set
  [^Memtable memtable]
  (->> memtable :mem :mem .entrySet
       (map (fn [e] [(.getKey e) (.getValue e)]))))
