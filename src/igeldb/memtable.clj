(ns igeldb.memtable
  (:require [clojure.core.async :as async]
            [igeldb.data :as data]
            [igeldb.store :as store :refer [select scan]]
            [igeldb.wal :as wal]))

;; The memtable store is an atom holding an *immutable* sorted map (sorted by the
;; same unsigned byte-array comparator as the SSTables). It is swapped, never
;; mutated in place: readers deref a snapshot lock-free, and the group-commit
;; worker publishes each batch with a single atomic `swap!`, so a reader sees the
;; pre-batch or post-batch map, never a torn mid-batch view. (A Babashka-friendly
;; requirement too: bb has no java.util.TreeMap.)
(defrecord MemStore [store]
  store/IStoreRead
  (select
    [_ k]
    ;; the stored value is a Data (a live value or a tombstone) or nil; a
    ;; tombstone must be returned (truthy) so it shadows deeper levels
    (get @store k))
  (scan
    [_ from-key to-key]
    ;; from-inclusive, to-exclusive -- matches TreeMap's subMap(from,true,to,false)
    (mapv (fn [[k data]] [k data])
          (subseq @store >= from-key < to-key)))

  store/IStoreMutate
  (write!
    [_ k v]
    (swap! store assoc k (data/new-data v)))
  (write-data!
    [_ k data]
    (swap! store assoc k data))
  (write-batch!
    [_ entries]
    ;; apply the whole batch in one atomic swap, in order (last wins)
    (swap! store (fn [m] (reduce (fn [m [k data]] (assoc m k data)) m entries))))
  (delete!
    [_ k]
    (swap! store assoc k (data/deleted-data))))

(defrecord Memtable [mem wal-chan size]
  store/IStoreRead
  ;; Reads are lock-free: the store is an immutable snapshot behind an atom, so
  ;; there is nothing to guard against the (single) writer.
  (select
    [_ k]
    (select mem k))
  (scan
    [_ from-key to-key]
    (scan mem from-key to-key))

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
  (write-batch! [_ _] (throw (UnsupportedOperationException.)))
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

(defn- empty-store []
  (->MemStore (atom (sorted-map-by (data/byte-array-comparator)))))

(defn create-memtable
  "Create the new memtable"
  [wal-chan]
  (->Memtable (empty-store) wal-chan (atom 0)))

(defn init-memtable
  "Initialize the memtable, restoring data from the WAL. Returns
  `[wal-id memtable]`: `wal-id` is the ID of the replayed WAL (0 if none), used
  to seed the WAL counter.

  The `size` is the *actual* byte size of the replayed entries -- no fabricated
  value. This makes the zero-check in `flush!` correct (an empty store skips the
  initial flush; a restored one flushes immediately to commit the WAL) and lines
  up with the flush threshold check."
  [wal-chan config]
  (let [store (empty-store)
        [wal-id wal-pairs] (wal/load-existing-wal config)]
    ;; `load-existing-wal` returns [k data] entries (data is a value or a
    ;; tombstone). Apply them in WAL order so the replayed memtable matches the
    ;; pre-crash state.
    (store/write-batch! store wal-pairs)
    (let [size (reduce (fn [acc [k data]] (+ acc (wal/entry-size k data)))
                       0 wal-pairs)]
      [wal-id (->Memtable store wal-chan (atom size))])))

(defn entry-set
  "The memtable's [k data] entries in key order (an immutable snapshot)."
  [^Memtable memtable]
  (mapv (fn [[k data]] [k data]) @(:store (:mem memtable))))
