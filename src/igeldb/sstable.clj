(ns igeldb.sstable
  (:require [blossom.core :as blossom]
            [clojure.tools.logging :as logging]
            [igeldb.data :as data]
            [igeldb.io :as io]
            [igeldb.manifest :as manifest]
            [igeldb.store :as store])
  (:import (java.util.concurrent.locks ReentrantReadWriteLock)))

(defn get-sstable-path
  [id dir]
  (str dir "/" id ".sst"))

;; SSTable metadata is managed as `[table-id info]`, where `info` is a map
;; `{:head-key :tail-key :bloom-filter :size}` (the bloom filter is a live
;; object, `:size` is the on-disk byte size). The `sstables` in TreeStore group
;; these by level:
;;   [[[id8 info8] [id10 info10]] [[id3 info3]] [[id7 info7]]]
;;    <------- Level 0 --------->  <- Level 1 ->  <- Level 2 ->
;; Within a level the pairs are ordered by ascending ID; a larger ID is newer.
;; The durable source of truth is the manifest; `sstables` is the in-memory
;; projection of the replayed + committed edits.

(defrecord TreeStore [dir sstables rw-lock manifest]
  store/IStoreRead
  (select
    [_ k]
    (let [read-lock (.readLock rw-lock)]
      (.lock read-lock)
      (try
        (loop [tables (reverse (reduce into [] @sstables))]
          ;; Stop once the tables are exhausted: return nil without touching
          ;; the disk. Only read a value when the Bloom filter reports a hit,
          ;; so a negative filter never triggers a file read.
          (when-let [[id table] (first tables)]
            (if-let [v (and (blossom/hit? (:bloom-filter table) k)
                            (io/read-value (get-sstable-path id dir) k))]
              v
              (recur (next tables)))))
        (finally (.unlock read-lock)))))
  (scan
    [_ from-key to-key]
    (let [read-lock (.readLock rw-lock)]
      (.lock read-lock)
      (try
        (loop [pairs (new java.util.TreeMap (data/byte-array-comparator))
               tables (reduce into [] @sstables)]
          (if (empty? tables)
            (->> pairs .entrySet (map (fn [e] [(.getKey e) (.getValue e)])))
            (let [[id table] (first tables)
                  head-key (:head-key table)
                  tail-key (:tail-key table)
                  sstable-path (get-sstable-path id dir)]
              (recur
               (if (and (data/byte-array-smaller? head-key to-key)
                        (data/byte-array-smaller-or-equal? from-key tail-key))
                 (reduce
                  (fn [tree-map [k d]]
                    (.put tree-map k d)
                    tree-map)
                  pairs
                  (io/scan-pairs sstable-path from-key to-key))
                 pairs)
               (rest tables)))))
        (finally (.unlock read-lock))))))

;; ---- Version edits -------------------------------------------------------

(defn- entry->pair
  [entry]
  [(:id entry) (select-keys entry [:head-key :tail-key :bloom-filter :size])])

(defn apply-edit
  "Fold one version edit (bloom filters as *objects*) into the per-level table
  vector: drop every `:deleted` ID from all levels, then insert each `:added`
  table at its `:level` (levels kept dense; within a level ordered by ID)."
  [sstables {:keys [added deleted]}]
  (let [deleted? (set deleted)
        pruned (mapv (fn [level] (filterv (fn [[id _]] (not (deleted? id))) level))
                     sstables)
        max-level (reduce max -1 (map :level added))
        grown (into pruned (repeat (max 0 (- (inc max-level) (count pruned))) []))]
    (reduce (fn [levels entry]
              (update levels (:level entry)
                      (fn [lvl] (vec (sort-by first (conj lvl (entry->pair entry)))))))
            grown
            added)))

(defn- serialize-edit-blooms
  "Convert an edit's added bloom filters from objects to bytes (for the manifest)."
  [edit]
  (update edit :added
          (fn [added]
            (mapv #(update % :bloom-filter blossom/serialize-filter) added))))

(defn- deserialize-edit-blooms
  "Convert an edit's added bloom filters from bytes to objects (from the manifest)."
  [edit bloom-config]
  (update edit :added
          (fn [added]
            (mapv #(update % :bloom-filter blossom/deserialize-filter bloom-config)
                  added))))

(defn commit-edit!
  "The single durable commit path for flush and compaction. Serialized across
  callers by the manifest lock: append+fsync the edit to the manifest (the commit
  point), then apply it to the in-memory table set under the write lock. The edit
  carries bloom filters as objects; the manifest copy stores them as bytes. Any
  IO failure propagates so the caller's fail-stop wrapper poisons the store."
  [tree edit]
  (let [manifest (:manifest tree)
        commit-lock (:lock manifest)]
    (locking commit-lock
      (manifest/append-edit! manifest (serialize-edit-blooms edit))
      (let [write-lock (.writeLock (:rw-lock tree))]
        (.lock write-lock)
        (try
          (swap! (:sstables tree) apply-edit edit)
          (finally (.unlock write-lock)))))))

;; ---- Startup recovery ----------------------------------------------------

(defn restore-tree-store
  "Rebuild the table set by replaying the manifest (no directory scan). Returns
  `[tree next-sstable-id]`. Files on disk not referenced by the replayed state
  are ignored. `next-sstable-id` is one past the highest ID ever assigned (from
  any edit's `:added`) so a deleted table's ID is never reused."
  [{:keys [sstable-dir] :as config}]
  (io/make-dir sstable-dir)
  (let [raw-edits (manifest/read-edits (manifest/manifest-path config))
        edits (map #(deserialize-edit-blooms % (:bloom-filter config)) raw-edits)
        sstables (reduce apply-edit [[]] edits)
        max-id (reduce (fn [m e] (reduce (fn [m a] (max m (:id a))) m (:added e)))
                       -1 raw-edits)
        sstable-id (inc max-id)]
    (logging/info "Restored table set from manifest; next SSTable id" sstable-id)
    [(->TreeStore sstable-dir
                  (atom sstables)
                  (ReentrantReadWriteLock. true)
                  (manifest/open-manifest config))
     sstable-id]))
