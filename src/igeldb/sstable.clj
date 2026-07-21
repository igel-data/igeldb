(ns igeldb.sstable
  (:require [blossom.core :as blossom]
            [clojure.tools.logging :as logging]
            [igeldb.data :as data]
            [igeldb.io :as io]
            [igeldb.manifest :as manifest]
            [igeldb.store :as store])
  (:import (java.io File FileOutputStream BufferedOutputStream)
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

;; ---- SSTable writer: entries + sparse block index + footer ----------------
;;
;; Shared by flush and compaction. As entries are appended it tracks the byte
;; offset, the bloom filter (keyed by user_key -- point lookups are per user_key)
;; and the sparse block index; `close-table!` appends the footer and fsyncs.

(defrecord TableWriter [id path file-stream out-stream bloom block-size state])

(defn open-table!
  "Open a new SSTable for writing."
  [id {:keys [sstable-dir bloom-filter sstable-block-size]}]
  (let [path (get-sstable-path id sstable-dir)
        file-stream (FileOutputStream. ^String path)
        out-stream (BufferedOutputStream. file-stream 16384)]
    (io/write-format-byte! out-stream)
    (->TableWriter id path file-stream out-stream
                   (blossom/make-filter bloom-filter)
                   sstable-block-size
                   ;; offset starts at 1: the format byte occupies byte 0
                   (atom {:offset 1 :blocks [] :block-first nil :block-start nil
                          :block-bytes 0 :last-key nil :head nil :tail nil}))))

(defn table-bytes
  "Bytes written so far (entries only, excluding the not-yet-written footer)."
  ^long [writer]
  (:offset @(:state writer)))

(defn write-entry!
  "Append one InternalKey entry (value or tombstone). A block is cut only once the
  accumulated bytes reach `sstable-block-size` AND the user_key changes, so a
  single user_key's versions are never split across blocks (a block may therefore
  exceed the target size when one key has many versions)."
  [writer ikey data]
  (let [st @(:state writer)
        uk (:user-key ikey)
        size (io/entry-size-on-disk ikey data)
        cut? (and (:block-first st)
                  (>= (long (:block-bytes st)) (long (:block-size writer)))
                  (not (data/byte-array-equals? uk (:last-key st))))
        st (if cut?
             (-> st
                 (update :blocks conj [(:block-first st) (:block-start st)])
                 (assoc :block-first nil :block-start nil :block-bytes 0))
             st)
        st (if (nil? (:block-first st))
             (assoc st :block-first uk :block-start (:offset st) :block-bytes 0)
             st)]
    (io/append-entry! (:out-stream writer) [ikey data])
    (blossom/add (:bloom writer) uk)
    (reset! (:state writer)
            (-> st
                (update :offset + size)
                (update :block-bytes + size)
                (assoc :last-key uk)
                (update :head #(or % uk))
                (assoc :tail uk)))
    nil))

(defn close-table!
  "Close the final block, append the index region + trailer, fsync and close.
  Returns the table entry for a manifest edit. `:index` / `:index-start` are the
  in-memory read structure -- derived from the file, NOT stored in the manifest."
  [writer level]
  (let [st @(:state writer)
        blocks (cond-> (:blocks st)
                 (:block-first st) (conj [(:block-first st) (:block-start st)]))
        out-stream ^BufferedOutputStream (:out-stream writer)
        file-stream ^FileOutputStream (:file-stream writer)]
    (io/write-footer! out-stream blocks (:offset st))
    (.flush out-stream)
    (-> file-stream .getChannel (.force true))
    (.close out-stream)
    (.close file-stream)
    {:id (:id writer) :level level
     :head-key (:head st) :tail-key (:tail st)
     :bloom-filter (:bloom writer)
     :index blocks
     :index-start (:offset st)
     :size (.length (File. ^String (:path writer)))}))

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
    [this k snapshot-seq]
    (with-version
      this
      (fn [version]
        ;; select-order is shallowest (newest) first; a shallower table holds the
        ;; newest versions, so the first table whose newest version <= snapshot is
        ;; the answer. `read-value` returns nil for a table whose only versions of
        ;; k are all > snapshot, so the search continues to deeper (older) levels.
        (loop [tables (select-order version)]
          (when-let [[id table] (first tables)]
            (if-let [v (and (blossom/hit? (:bloom-filter table) k)
                            (io/read-value (get-sstable-path id dir) k snapshot-seq table))]
              v
              (recur (next tables))))))))
  (scan
    [this from-key to-key snapshot-seq]
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
                 ;; last assoc wins, keyed by user_key -- scan-order feeds tables
                 ;; deepest (oldest) first, so the shallowest (newest visible)
                 ;; version survives; each `scan-pairs` already picks per user_key
                 ;; the newest version <= snapshot within its file
                 (reduce (fn [m [k d]] (assoc m k d))
                         pairs
                         (io/scan-pairs sstable-path from-key to-key snapshot-seq table))
                 pairs)
               (rest tables))))))))
  (latest-seq
    [this k]
    (with-version
      this
      (fn [version]
        ;; select-order is shallowest (newest) first; the first table (by
        ;; precedence) that contains k holds its newest version, so its seq is the
        ;; latest committed seq for k across the tree.
        (loop [tables (select-order version)]
          (when-let [[id table] (first tables)]
            (if-let [s (and (blossom/hit? (:bloom-filter table) k)
                            (io/read-latest-seq (get-sstable-path id dir) k table))]
              s
              (recur (next tables)))))))))

;; ---- Version edits -------------------------------------------------------

(defn- entry->pair
  [entry]
  [(:id entry) (select-keys entry [:head-key :tail-key :bloom-filter :size :index :index-start])])

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
  `[tree next-sstable-id manifest-max-seq]`. Files on disk not referenced by the
  replayed state are ignored. `next-sstable-id` is one past the highest ID ever
  assigned (from any edit's `:added`) so a deleted table's ID is never reused.
  `manifest-max-seq` is the highest InternalKey seq any edit committed -- part of
  next-seq recovery (the newest seq may live only in SSTables after a clean
  shutdown; see `igeldb.core/gen-kvs`)."
  [{:keys [sstable-dir] :as config}]
  (io/make-dir sstable-dir)
  (let [raw-edits (manifest/read-edits (manifest/manifest-path config))
        edits (map #(deserialize-edit-blooms % (:bloom-filter config)) raw-edits)
        ;; Load each LIVE table's footer (sparse index) after folding the edits --
        ;; a table added by one edit and deleted by a later one has no file left,
        ;; so it must never be opened. The index is derived from the file, not the
        ;; manifest, and lives in memory for the table's lifetime.
        version (mapv (fn [level]
                        (mapv (fn [[id info]]
                                [id (merge info
                                           (io/read-footer!
                                            (get-sstable-path id sstable-dir)))])
                              level))
                      (reduce apply-edit [[]] edits))
        max-id (reduce (fn [m e] (reduce (fn [m a] (max m (:id a))) m (:added e)))
                       -1 raw-edits)
        manifest-max-seq (reduce (fn [m e] (max m (or (:max-seq e) 0))) 0 raw-edits)
        sstable-id (inc max-id)]
    (logging/info "Restored table set from manifest; next SSTable id" sstable-id
                  "manifest max-seq" manifest-max-seq)
    [(->TreeStore sstable-dir
                  (atom version)
                  (make-rw-lock)
                  (manifest/open-manifest config))
     sstable-id
     manifest-max-seq]))
