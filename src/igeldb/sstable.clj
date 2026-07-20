(ns igeldb.sstable
  (:require [blossom.core :as blossom]
            [clojure.tools.logging :as logging]
            [igeldb.data :as data]
            [igeldb.io :as io]
            [igeldb.manifest :as manifest]
            [igeldb.store :as store])
  (:import (java.io File)
           (java.util.concurrent Semaphore)))

(defn get-sstable-path
  [id dir]
  (str dir "/" id ".sst"))

;; Reader/writer lock built on a Semaphore (Babashka has no
;; ReentrantReadWriteLock). A reader takes one permit; a writer takes them all,
;; so it excludes every reader. Fair mode keeps a waiting writer (a file
;; deletion) from being starved by a stream of readers.
(def ^:const ^:private RW_PERMITS 1000000)

(defn- make-rw-lock [] (Semaphore. RW_PERMITS true))
(defn- read-lock! [^Semaphore s] (.acquire s 1))
(defn- read-unlock! [^Semaphore s] (.release s 1))
(defn- write-lock! [^Semaphore s] (.acquire s RW_PERMITS))
(defn- write-unlock! [^Semaphore s] (.release s RW_PERMITS))

(defn next-id!
  "Atomically allocate the next monotonic SSTable id. Safe against concurrent
  callers (the flush worker and the compaction worker both allocate ids)."
  [sstable-id]
  (dec (long (swap! sstable-id inc))))

(defn write-entry!
  "Write one key/data entry (value or tombstone) to an SSTable output stream and
  record the key in the bloom filter. Shared by flush and compaction."
  [out-stream bf ^bytes k data]
  (io/write-bytes! out-stream k)
  (if (:deleted? data)
    (io/write-tombstone! out-stream)
    (io/write-bytes! out-stream (:value data)))
  (blossom/add bf k))

;; SSTable metadata is managed as `[table-id info]`, where `info` is a map
;; `{:head-key :tail-key :bloom-filter :size}` (the bloom filter is a live
;; object, `:size` is the on-disk byte size). A *version* is an immutable
;; snapshot of the whole table set, grouped by level:
;;   [[[id8 info8] [id10 info10]] [[id3 info3]] [[id7 info7]]]
;;    <------- Level 0 --------->  <- Level 1 ->  <- Level 2 ->
;; Within a level the pairs are ordered by ascending ID; a larger ID is newer.
;; The durable source of truth is the manifest; `current-version` is the
;; in-memory projection of the replayed + committed edits.
;;
;; Read precedence is by level: shallower = newer. L0 tables overlap, so within
;; L0 a larger ID wins; L1+ tables within a level do not overlap, so their order
;; among themselves is irrelevant.

(defn- select-order
  "Tables in descending read precedence (highest first): L0 newest-first, then
  each deeper level. `select` returns on the first bloom hit that yields a value."
  [version]
  (concat (reverse (first version)) (apply concat (rest version))))

(defn- scan-order
  "Tables in ascending read precedence (lowest first): deepest levels first, L0
  last (ascending ID). `scan` merges into a sorted map where the last assoc wins,
  so the newest value must be applied last."
  [version]
  (apply concat (reverse version)))

(defn with-version
  "Run `(f version-snapshot)` while holding the read lock, releasing it in a
  finally. The snapshot is an immutable value, so a concurrent commit swapping in
  a new version is never observed half-applied. Holding the read lock also blocks
  file deletion (Step 5) until this reader is done with the version's files."
  [tree f]
  (let [sem (:rw-lock tree)]
    (read-lock! sem)
    (try
      (f @(:current-version tree))
      (finally (read-unlock! sem)))))

(defrecord TreeStore [dir current-version rw-lock manifest]
  store/IStoreRead
  (select
    [this k]
    (with-version
      this
      (fn [version]
        (loop [tables (select-order version)]
          ;; Stop once the tables are exhausted: return nil without touching the
          ;; disk. Only read a value when the Bloom filter reports a hit, so a
          ;; negative filter never triggers a file read.
          (when-let [[id table] (first tables)]
            (if-let [v (and (blossom/hit? (:bloom-filter table) k)
                            (io/read-value (get-sstable-path id dir) k))]
              v
              (recur (next tables))))))))
  (scan
    [this from-key to-key]
    (with-version
      this
      (fn [version]
        (loop [pairs (sorted-map-by (data/byte-array-comparator))
               tables (scan-order version)]
          (if (empty? tables)
            (seq pairs)
            (let [[id table] (first tables)
                  head-key (:head-key table)
                  tail-key (:tail-key table)
                  sstable-path (get-sstable-path id dir)]
              (recur
               (if (and (data/byte-array-smaller? head-key to-key)
                        (data/byte-array-smaller-or-equal? from-key tail-key))
                 ;; last assoc wins, mirroring TreeMap `.put` -- scan-order feeds
                 ;; tables lowest-precedence first, so the newest value survives
                 (reduce (fn [m [k d]] (assoc m k d))
                         pairs
                         (io/scan-pairs sstable-path from-key to-key))
                 pairs)
               (rest tables)))))))))

;; ---- Version edits -------------------------------------------------------

(defn- entry->pair
  [entry]
  [(:id entry) (select-keys entry [:head-key :tail-key :bloom-filter :size])])

(defn apply-edit
  "Fold one version edit (bloom filters as *objects*) into the per-level table
  vector: drop every `:deleted` ID from all levels, then insert each `:added`
  table at its `:level` (levels kept dense; within a level ordered by ID)."
  [version {:keys [added deleted]}]
  (let [deleted? (set deleted)
        pruned (mapv (fn [level] (filterv (fn [[id _]] (not (deleted? id))) level))
                     version)
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
  point), then atomically swap the new version into `current-version`.

  The swap needs no lock -- readers grab `@current-version` once (an immutable
  snapshot), so they never see a half-applied switch. The read/write lock instead
  arbitrates readers vs. physical file deletion (Step 5), not this swap.

  The edit carries bloom filters as objects; the manifest copy stores them as
  bytes. Any IO failure propagates so the caller's fail-stop wrapper poisons the
  store."
  [tree edit]
  (let [manifest (:manifest tree)
        commit-lock (:lock manifest)]
    (locking commit-lock
      (manifest/append-edit! manifest (serialize-edit-blooms edit))
      (swap! (:current-version tree) apply-edit edit))))

;; ---- Deferred-safe physical deletion (Step 5) ----------------------------

(defn delete-inputs!
  "Physically delete superseded SSTable files, AFTER the committed version has
  stopped referencing them. Acquires the write lock: once held, every reader has
  released the read lock, so none is mid-read on these files -- the delete is
  safe (no `FileNotFoundException`), with no reference counting.

  A file already gone (a delete lost to a prior crash) is tolerated and logged;
  any other delete failure is a real IO error and propagates (the caller applies
  fail-stop). A lost delete is non-fatal for correctness: startup ignores
  SSTables the manifest does not reference."
  [tree ids]
  (let [dir (:dir tree)
        sem (:rw-lock tree)]
    (write-lock! sem)
    (try
      (doseq [id ids]
        (let [path (get-sstable-path id dir)
              f (File. ^String path)]
          (cond
            (not (.exists f))
            (logging/warn "Superseded SSTable already gone (lost delete?):" path)
            (not (.delete f))
            (throw (ex-info "Failed to delete superseded SSTable" {:path path})))))
      (finally (write-unlock! sem)))))

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
        version (reduce apply-edit [[]] edits)
        max-id (reduce (fn [m e] (reduce (fn [m a] (max m (:id a))) m (:added e)))
                       -1 raw-edits)
        sstable-id (inc max-id)]
    (logging/info "Restored table set from manifest; next SSTable id" sstable-id)
    [(->TreeStore sstable-dir
                  (atom version)
                  (make-rw-lock)
                  (manifest/open-manifest config))
     sstable-id]))
