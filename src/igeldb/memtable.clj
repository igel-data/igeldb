(ns igeldb.memtable
  (:require [clojure.core.async :as async]
            [igeldb.data :as data]
            [igeldb.store :as store]
            [igeldb.tx :as tx]
            [igeldb.wal :as wal]))

;; The memtable store is an atom holding an *immutable* sorted map keyed by
;; InternalKey (user_key asc, seq desc). It is swapped, never mutated in place:
;; readers deref a snapshot lock-free, and the group-commit worker publishes each
;; batch with a single atomic `swap!`, so a reader sees the pre-batch or
;; post-batch map, never a torn mid-batch view. (A Babashka-friendly requirement
;; too: bb has no java.util.TreeMap.)
;;
;; Multiple versions of a user_key can coexist (one per committing tx). A read at
;; `snapshot-seq` seeks `(->ikey user_key snapshot-seq)`: the first entry
;; at-or-after it with the same user_key is the newest version with seq <=
;; snapshot (seq-desc ordering puts newer versions first).

(defn- newest-visible
  "The [ikey data] of the newest version of user_key `k` with seq <= snapshot in
  the sorted map `m`, or nil."
  [m ^bytes k snapshot-seq]
  (when-let [[ikey data] (first (subseq m >= (data/->ikey k snapshot-seq)))]
    (when (data/byte-array-equals? (:user-key ikey) k)
      [ikey data])))

(defrecord MemStore [store]
  store/IStoreRead
  (select
    [_ k snapshot-seq]
    ;; return the Data of the newest visible version (a value or a tombstone,
    ;; which must be truthy so it shadows deeper levels), or nil
    (second (newest-visible @store k snapshot-seq)))
  (scan
    [_ from-key to-key snapshot-seq]
    ;; [from, to) on user_keys; for each user_key its newest version <= snapshot.
    ;; Entries are user_key-asc / seq-desc, so within a user_key group the first
    ;; entry with seq <= snapshot wins.
    (loop [es (subseq @store
                      >= (data/->ikey from-key Long/MAX_VALUE)
                      < (data/->ikey to-key Long/MAX_VALUE))
           acc (transient [])
           emitted nil]
      (if (empty? es)
        (persistent! acc)
        (let [[ikey data] (first es)
              uk (:user-key ikey)]
          (cond
            (and emitted (data/byte-array-equals? uk emitted)) (recur (rest es) acc emitted)
            (> (:seq ikey) snapshot-seq) (recur (rest es) acc emitted)
            :else (recur (rest es) (conj! acc [uk data]) uk))))))

  store/IStoreMutate
  ;; The worker applies whole batches via write-batch!; the single-op mutators are
  ;; not the memtable's write path (seq is assigned in `Memtable`, above the store).
  (write! [_ _ _] (throw (UnsupportedOperationException.)))
  (write-data! [_ _ _] (throw (UnsupportedOperationException.)))
  (write-batch!
    [_ entries]
    ;; entries are [ikey data]; apply the whole batch in one atomic swap, in order
    (swap! store (fn [m] (reduce (fn [m [ikey d]] (assoc m ikey d)) m entries))))
  (delete! [_ _] (throw (UnsupportedOperationException.))))

(defrecord Memtable [mem wal-chan size registry]
  store/IStoreRead
  ;; Reads are lock-free over an immutable snapshot behind an atom.
  (select
    [_ k snapshot-seq]
    (store/select mem k snapshot-seq))
  (scan
    [_ from-key to-key snapshot-seq]
    (store/scan mem from-key to-key snapshot-seq))

  store/IStoreMutate
  ;; Writers assign a seq, then enqueue an InternalKey entry to the WAL channel and
  ;; wait for the group-commit worker. The worker (a single thread) appends to the
  ;; WAL, fsyncs, then applies the batch to the memtable -- keeping WAL-append
  ;; order == memtable-apply order == seq order, and making rollback on fsync
  ;; failure unnecessary. (Step 1: seq is a global counter per write; Step 4 moves
  ;; seq assignment into the commit-handler.)
  (write!
    [_ k v]
    (let [ikey (data/->ikey k (tx/next-seq! registry))
          comp-chan (async/chan)]
      (when-not (async/>!! wal-chan [ikey (data/new-data v) comp-chan])
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
    (let [ikey (data/->ikey k (tx/next-seq! registry))
          comp-chan (async/chan)]
      (when-not (async/>!! wal-chan [ikey (data/deleted-data) comp-chan])
        (throw (ex-info "Delete failed due to memtable switching"
                        {:retriable true})))
      (case (async/<!! comp-chan)
        :done nil
        nil (throw (ex-info "Delete failed due to memtable switching"
                            {:retriable true}))
        (throw (ex-info "Delete failed" {:retriable false}))))))

(defn- empty-store []
  (->MemStore (atom (sorted-map-by (data/internal-key-comparator)))))

(defn create-memtable
  "Create a new (empty) memtable sharing the global tx `registry`."
  [wal-chan registry]
  (->Memtable (empty-store) wal-chan (atom 0) registry))

(defn init-memtable
  "Initialize the memtable, restoring data from the WAL. Returns `[wal-id
  memtable]`. Seeds the shared `registry`'s commit-order counter to the max
  replayed seq so new writes get higher seqs (Step 7 also folds in the manifest
  max-seq). The `size` is the actual byte size of the replayed entries."
  [wal-chan registry config]
  (let [store (empty-store)
        [wal-id wal-pairs] (wal/load-existing-wal config)]
    ;; `load-existing-wal` returns [ikey data] entries; apply them in WAL order so
    ;; the replayed memtable matches the pre-crash state.
    (store/write-batch! store wal-pairs)
    (tx/seed-seq! registry (reduce (fn [m [ikey _]] (max m (:seq ikey))) 0 wal-pairs))
    (let [size (reduce (fn [acc [ikey data]] (+ acc (wal/entry-size ikey data)))
                       0 wal-pairs)]
      [wal-id (->Memtable store wal-chan (atom size) registry)])))

(defn entry-set
  "The memtable's [ikey data] entries in InternalKey order (an immutable snapshot)."
  [^Memtable memtable]
  (mapv (fn [[ikey data]] [ikey data]) @(:store (:mem memtable))))
