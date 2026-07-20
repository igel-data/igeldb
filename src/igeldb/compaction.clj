(ns igeldb.compaction
  "Leveled compaction. Keeps each compaction's input bounded (one table from
  L(n) plus the overlapping L(n+1) tables; or, for L0, all L0 tables plus the
  overlapping L1 tables), so compaction work stays small and even.

  Selection: L0 triggers on table COUNT (`l0-compaction-trigger`); L1+ trigger
  on the over-ratio `current_size / max_size`. The candidate with the highest
  score >= 1 is compacted. Within an L1+ level a table is picked round-robin
  (an in-memory compact-pointer that resumes after the previous compaction).

  Merge is newest-wins (shallower level wins; within L0 larger id wins).
  Tombstones are preserved until they reach the bottom-most level, where they
  are dropped (nothing below can hold an older live value).

  The heavy merge work holds no lock. Only the commit -- append+fsync the
  manifest edit, then atomically swap the version -- is guarded (by the manifest
  lock inside `sstable/commit-edit!`). A compaction failure is fail-stop.

  After committing, the superseded input files are physically deleted under the
  write lock (`sstable/delete-inputs!`), which waits out any in-flight readers.

  TODO (memory): the merge materializes all input entries in a TreeMap. A
  streaming k-way merge over the (already sorted) inputs would bound compaction
  memory; deferred for now."
  (:require [blossom.core :as blossom]
            [clojure.java.io :as java-io]
            [clojure.core.async :as async]
            [clojure.tools.logging :as logging]
            [igeldb.data :as data]
            [igeldb.io :as io]
            [igeldb.sstable :as sstable])
  (:import (java.io FileOutputStream BufferedOutputStream File)))

(def ^:private ^:const SEG_OVERHEAD 16) ;; 8-byte length + 8-byte CRC per segment

;; ---- level geometry ------------------------------------------------------

(defn- level-tables [version level] (nth version level []))

(defn- level-bytes
  [version level]
  (reduce + 0 (map (comp :size second) (level-tables version level))))

(defn- max-level-bytes
  [level {:keys [l1-base-size level-size-multiplier]}]
  ;; L1 = l1-base-size, L2 = l1-base-size * mult, L3 = * mult^2, ...
  (reduce * l1-base-size (repeat (dec level) level-size-multiplier)))

(defn- deepest-with-data
  "The deepest (largest-index) level that currently holds any table (0 if none)."
  [version]
  (or (some (fn [lvl] (when (seq (level-tables version lvl)) lvl))
            (range (dec (count version)) -1 -1))
      0))

(defn- head [[_ info]] (:head-key info))
(defn- tail [[_ info]] (:tail-key info))

(defn- ranges-overlap?
  [h1 t1 h2 t2]
  (and (data/byte-array-smaller-or-equal? h1 t2)
       (data/byte-array-smaller-or-equal? h2 t1)))

(defn- byte-min [a b] (if (data/byte-array-smaller? a b) a b))
(defn- byte-max [a b] (if (data/byte-array-smaller? a b) b a))

(defn- overlapping
  "Tables in `level-tables` whose range intersects [h, t]."
  [level-tables h t]
  (filterv (fn [table] (ranges-overlap? h t (head table) (tail table))) level-tables))

(defn pick-round-robin
  "Pick one table from an L1+ level, resuming after `pointer` (the tail-key of
  the last compaction of this level); wrap to the first table at the end. Tables
  are ordered by head-key (L1+ ranges do not overlap, so this is a total order)."
  [tables pointer]
  (let [sorted (sort-by head (data/byte-array-comparator) tables)]
    (or (when pointer
          (first (filter (fn [t] (data/byte-array-smaller? pointer (head t))) sorted)))
        (first sorted))))

;; ---- planning ------------------------------------------------------------

(defn- l0-plan
  [version]
  (let [l0 (level-tables version 0)
        l1 (level-tables version 1)
        h (reduce byte-min (map head l0))
        t (reduce byte-max (map tail l0))
        over (overlapping l1 h t)]
    {:level 0
     :out-level 1
     ;; low->high precedence: L1 (older) then L0 ascending id (version order)
     :inputs-low-to-high (concat over l0)
     :deleted-ids (mapv first (concat l0 over))
     :drop-tombstones? (>= 1 (deepest-with-data version))
     :pointer nil}))

(defn- ln-plan
  [version level compact-pointers]
  (let [chosen (pick-round-robin (level-tables version level)
                                 (get @compact-pointers level))
        h (head chosen)
        t (tail chosen)
        over (overlapping (level-tables version (inc level)) h t)
        out-level (inc level)]
    {:level level
     :out-level out-level
     ;; low->high precedence: L(n+1) (older) then the one L(n) table (newer)
     :inputs-low-to-high (concat over [chosen])
     :deleted-ids (mapv first (cons chosen over))
     :drop-tombstones? (>= out-level (deepest-with-data version))
     :pointer (tail chosen)}))

(defn pick-compaction
  "Decide the next compaction, or nil if none is needed. L0 scores on table
  count; L1+ score on current/max bytes; the highest score >= 1 wins."
  [version compact-pointers config]
  (let [l0-count (count (level-tables version 0))
        l0-score (/ l0-count (:l0-compaction-trigger config))
        deepest (dec (count version))
        ln-scores (for [level (range 1 (inc deepest))
                        :let [sz (level-bytes version level)]
                        :when (pos? sz)]
                    [level (/ sz (max-level-bytes level config))])
        candidates (cond-> []
                     (>= l0-score 1) (conj [:l0 0 l0-score])
                     (seq ln-scores) (into (map (fn [[l s]] [:ln l s]) ln-scores)))
        winner (when (seq candidates)
                 (let [[_ _ score :as best] (apply max-key #(nth % 2) candidates)]
                   (when (>= score 1) best)))]
    (when winner
      (let [[kind level] winner]
        (if (= kind :l0)
          (l0-plan version)
          (ln-plan version level compact-pointers))))))

;; ---- merge + output ------------------------------------------------------

(defn- read-all-pairs
  "Read every [k data] entry from an SSTable file (compaction reads whole tables)."
  [path]
  (with-open [in (java-io/input-stream path)]
    (loop [acc (transient [])]
      (let [entry (io/read-kv-pair! in)]
        (cond
          (= :eof entry) (persistent! acc)
          (or (= :truncated entry) (= :corrupt entry))
          (throw (ex-info "SSTable is corrupted (compaction input)"
                          {:path path :reason entry}))
          :else (recur (conj! acc entry)))))))

(defn merge-inputs
  "Merge input file paths given LOW->HIGH precedence into a key-sorted seq of
  [k data], newest-wins. Drops tombstones when `drop-tombstones?`."
  [paths-low-to-high drop-tombstones?]
  (let [merged (reduce (fn [m path]
                         (reduce (fn [m [k data]] (assoc m k data))
                                 m (read-all-pairs path)))
                       (sorted-map-by (data/byte-array-comparator))
                       paths-low-to-high)]
    (keep (fn [[k data]]
            (when-not (and drop-tombstones? (:deleted? data))
              [k data]))
          merged)))

(defn- entry-bytes
  [^bytes k data]
  (+ SEG_OVERHEAD (count k)
     (if (:deleted? data) 8 (+ SEG_OVERHEAD (count (:value data))))))

(defn- start-table!
  [sstable-id {:keys [sstable-dir bloom-filter]}]
  (let [id (sstable/next-id! sstable-id)
        path (sstable/get-sstable-path id sstable-dir)
        file-stream (FileOutputStream. path)]
    {:id id :path path
     :file-stream file-stream
     :out-stream (BufferedOutputStream. file-stream 16384)
     :bloom (blossom/make-filter bloom-filter)
     :head nil :tail nil :bytes 0}))

(defn- finish-table!
  [t out-level]
  (.flush ^BufferedOutputStream (:out-stream t))
  (-> ^FileOutputStream (:file-stream t) .getChannel (.force true))
  (.close ^BufferedOutputStream (:out-stream t))
  (.close ^FileOutputStream (:file-stream t))
  {:id (:id t) :level out-level
   :head-key (:head t) :tail-key (:tail t)
   :bloom-filter (:bloom t) :size (.length (File. ^String (:path t)))})

(defn- write-output!
  "Write the merged [k data] seq into non-overlapping output SSTables of about
  `sstable-target-size` bytes at `out-level`, allocating fresh ids. Returns the
  vector of edit entries for the new tables."
  [merged out-level sstable-id {:keys [sstable-target-size] :as config}]
  (loop [entries merged
         cur nil
         out []]
    (if (empty? entries)
      (if cur (conj out (finish-table! cur out-level)) out)
      (let [[k data] (first entries)
            cur (or cur (start-table! sstable-id config))
            _ (sstable/write-entry! (:out-stream cur) (:bloom cur) k data)
            cur (-> cur
                    (update :head #(or % k))
                    (assoc :tail k)
                    (update :bytes + (entry-bytes k data)))]
        (if (>= (:bytes cur) sstable-target-size)
          (recur (rest entries) nil (conj out (finish-table! cur out-level)))
          (recur (rest entries) cur out))))))

;; ---- one compaction ------------------------------------------------------

(defn compact!
  "Run one compaction if the store needs it. Returns true if a compaction was
  performed, nil if nothing needed compacting. Throws on IO failure (the worker
  poisons the store)."
  [tree sstable-id compact-pointers config]
  (let [version @(:current-version tree)]
    (when-let [plan (pick-compaction version compact-pointers config)]
      (let [dir (:dir tree)
            paths (map #(sstable/get-sstable-path (first %) dir)
                       (:inputs-low-to-high plan))
            merged (merge-inputs paths (:drop-tombstones? plan))
            out-entries (write-output! merged (:out-level plan) sstable-id config)]
        (logging/info "Compaction L" (:level plan) "-> L" (:out-level plan)
                      ":" (count (:deleted-ids plan)) "inputs ->"
                      (count out-entries) "outputs")
        (sstable/commit-edit! tree {:added out-entries
                                    :deleted (:deleted-ids plan)})
        (when-let [ptr (:pointer plan)]
          (swap! compact-pointers assoc (:level plan) ptr))
        ;; The version no longer references the inputs; delete their files under
        ;; the write lock (Step 5), which waits out any in-flight readers.
        (sstable/delete-inputs! tree (:deleted-ids plan))
        true))))

(defn spawn-compaction-worker
  "Background compaction worker (a real thread, like the flush writer). On each
  `:maybe-compact` signal it compacts until nothing more is needed (a single
  compaction can push the next level over its limit). Fail-stop on error.

  After every compaction it notifies `stall-monitor`: a compaction drains L0, so
  any writers stalled by back-pressure should re-check and may proceed."
  [tree sstable-id compact-pointers poison ^Object stall-monitor req-chan config]
  (letfn [(safe-compact! []
            (try
              (compact! tree sstable-id compact-pointers config)
              (catch Throwable e
                (reset! poison e)
                (logging/error e "Compaction failed; the store is poisoned")
                nil)))]
    (async/thread
      (loop []
        (when (some? (async/<!! req-chan)) ;; nil -> shutdown
          (when (nil? @poison)
            (loop []
              (when (true? (safe-compact!))
                (locking stall-monitor (.notifyAll stall-monitor))
                (recur))))
          (when (nil? @poison)
            (recur)))))))
