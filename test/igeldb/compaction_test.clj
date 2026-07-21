(ns igeldb.compaction-test
  (:require [clojure.java.io :as jio]
            [clojure.test :refer [deftest is testing]]
            [clj-yaml.core :as yaml]
            [igeldb.compaction :as compaction]
            [igeldb.core :as igel]
            [igeldb.data :as data]
            [igeldb.io :as io]
            [igeldb.sstable :as sstable]))

(defn- ->bytes [^String s] (.getBytes s))
(defn- b= [a b] (data/byte-array-equals? a b))

(defn- delete-recursively!
  [file]
  (when (.isDirectory file)
    (doseq [c (.listFiles file)] (delete-recursively! c)))
  (.delete file))

(defn- rm-rf [dir] (delete-recursively! (jio/file dir)))

;; ---- unit: round-robin table selection -----------------------------------

(defn- table [id head tail]
  [id {:head-key (->bytes head) :tail-key (->bytes tail) :size 100}])

(deftest round-robin-advances-and-wraps-test
  (let [tables [(table 1 "a" "c") (table 2 "e" "g") (table 3 "m" "p")]]
    (testing "no pointer -> first table"
      (is (= 1 (first (compaction/pick-round-robin tables nil)))))
    (testing "advances to the first table whose head is past the pointer"
      (is (= 2 (first (compaction/pick-round-robin tables (->bytes "c")))))
      (is (= 3 (first (compaction/pick-round-robin tables (->bytes "g"))))))
    (testing "wraps to the first table once past the end"
      (is (= 1 (first (compaction/pick-round-robin tables (->bytes "p"))))))
    (testing "tables given out of head-key order are sorted internally"
      (is (= 2 (first (compaction/pick-round-robin (reverse tables) (->bytes "c"))))))))

;; ---- unit: over-ratio level selection ------------------------------------

(defn- sized [id size]
  [id {:head-key (->bytes "a") :tail-key (->bytes "z") :size size}])

(deftest pick-compaction-selects-highest-over-ratio-test
  (let [config {:l0-compaction-trigger 4 :l1-base-size 1000 :level-size-multiplier 10}
        ptrs (atom {})]
    (testing "nothing over its limit -> no compaction"
      (is (nil? (compaction/pick-compaction
                 [[(sized 1 100) (sized 2 100)] [(sized 3 500)] [(sized 4 5000)]]
                 ptrs config))))
    (testing "the level with the highest over-ratio is chosen"
      ;; L1 2000/1000 = 2.0 beats L2 11000/10000 = 1.1
      (let [plan (compaction/pick-compaction
                  [[(sized 1 100)] [(sized 3 2000)] [(sized 4 11000)]]
                  ptrs config)]
        (is (= 1 (:level plan)))
        (is (= 2 (:out-level plan)))))
    (testing "L0 competes on count-ratio and can win"
      ;; L0 count 4/4 = 1.0 beats L1 500/1000 = 0.5
      (let [plan (compaction/pick-compaction
                  [[(sized 1 10) (sized 2 10) (sized 3 10) (sized 4 10)]
                   [(sized 5 500)]]
                  ptrs config)]
        (is (= 0 (:level plan)))
        (is (= 1 (:out-level plan)))))))

;; ---- unit: merge precedence + tombstone handling -------------------------

(defn- sst-config
  [dir & [block-size]]
  {:sstable-dir dir
   :bloom-filter {:size 1024}
   :sstable-block-size (or block-size 4096)})

(defn- write-sst!
  "Write [k-str v-str-or-nil] entries as an SSTable at commit `seq` (nil value =
  tombstone), via the production writer so the sparse index + footer are built.
  Entries must be given in user_key order. Returns the file path."
  [dir id seq entries]
  (let [w (sstable/open-table! id (sst-config dir))]
    (doseq [[k v] entries]
      (sstable/write-entry! w (data/->ikey (->bytes k) seq)
                            (if v (data/new-data (->bytes v)) (data/deleted-data))))
    (sstable/close-table! w 0)
    (sstable/get-sstable-path id dir)))

(defn- write-versions!
  "As `write-sst!` but with explicit per-entry seqs: [k-str seq v-str-or-nil].
  Entries should be in user_key-asc / seq-desc order. Returns the file path."
  [dir id entries & [block-size]]
  (let [w (sstable/open-table! id (sst-config dir block-size))]
    (doseq [[k seq v] entries]
      (sstable/write-entry! w (data/->ikey (->bytes k) seq)
                            (if v (data/new-data (->bytes v)) (data/deleted-data))))
    (sstable/close-table! w 0)
    (sstable/get-sstable-path id dir)))

(defn- seqs-of
  "The seqs of the merged entries for user_key `k`, in output order."
  [merged k]
  (->> merged
       (filter #(b= (->bytes k) (:user-key (first %))))
       (mapv (comp :seq first))))

;; ---- Step 6: MVCC GC by the min-active-snapshot floor --------------------

(deftest gc-keeps-floor-survivor-plus-newer-drops-older-test
  (let [dir "./test-data/compaction-gc-floor"]
    (.mkdirs (jio/file dir))
    (let [path (write-versions! dir 0 [["k" 9 "v9"] ["k" 6 "v6"] ["k" 3 "v3"]
                                       ["z" 5 "vz"]])]
      (testing "floor between versions: keep all > floor plus the newest <= floor"
        (is (= [9 6] (seqs-of (vec (compaction/merge-inputs [path] 6 false)) "k"))
            "seq 9 (> floor) and seq 6 (floor survivor) kept; seq 3 dropped"))
      (testing "no live tx (floor >= newest): collapse to the single newest version"
        (is (= [9] (seqs-of (vec (compaction/merge-inputs [path] 100 false)) "k"))))
      (testing "floor below every version: nothing is <= floor, so all survive"
        (is (= [9 6 3] (seqs-of (vec (compaction/merge-inputs [path] 2 false)) "k"))))
      (rm-rf dir))))

(deftest gc-tombstone-dropped-only-at-bottom-level-test
  (let [dir "./test-data/compaction-gc-tombstone"]
    (.mkdirs (jio/file dir))
    ;; key "d": tombstone@8 shadowing value@4; floor high (collapse to newest)
    (let [path (write-versions! dir 0 [["d" 8 nil] ["d" 4 "old"]])]
      (testing "non-bottom level: the surviving tombstone is preserved"
        (let [merged (vec (compaction/merge-inputs [path] 100 false))]
          (is (= [8] (seqs-of merged "d")))
          (is (:deleted? (second (first merged))) "kept entry is the tombstone")))
      (testing "bottom level: the surviving tombstone (and its shadowed value) drop"
        (is (empty? (compaction/merge-inputs [path] 100 true))))
      (rm-rf dir))))

(deftest gc-keeps-above-floor-tombstone-even-at-bottom-test
  ;; A tombstone with seq > floor is a "newer version" and must be kept (a live tx
  ;; may need to see the deletion) -- even at the bottom level, where only the floor
  ;; survivor may be dropped.
  (let [dir "./test-data/compaction-gc-tombstone-above"]
    (.mkdirs (jio/file dir))
    ;; tombstone@10, value@3
    (let [path (write-versions! dir 0 [["d" 10 nil] ["d" 3 "old"]])]
      (let [merged (vec (compaction/merge-inputs [path] 5 true))] ;; floor 5, bottom
        (is (= [10 3] (seqs-of merged "d"))
            "above-floor tombstone kept; value@3 is the floor survivor, kept")
        (is (:deleted? (second (first merged))) "newest kept entry is the tombstone"))
      (rm-rf dir))))

(deftest merge-newest-wins-and-tombstone-at-bottom-test
  (let [dir "./test-data/compaction-merge"]
    (.mkdirs (jio/file dir))
    (let [older (write-sst! dir 0 5 [["alive" "y"] ["gone" "x"]])
          ;; newer (higher seq) tombstone wins
          newer (write-sst! dir 1 10 [["gone" nil]])]
      (testing "bottom level: the tombstone (and the value it shadows) are dropped"
        ;; floor = MAX_VALUE: no live tx, so each key collapses to its newest version
        (let [merged (vec (compaction/merge-inputs [older newer] Long/MAX_VALUE true))]
          (is (= ["alive"] (mapv (comp #(String. %) :user-key first) merged))
              "only the live key survives")))
      (testing "non-bottom level: the tombstone is preserved to shadow deeper data"
        (let [merged (compaction/merge-inputs [older newer] Long/MAX_VALUE false)
              m (into {} (map (fn [[ikey d]] [(String. (:user-key ikey)) (:deleted? d)])
                              merged))]
          (is (= {"alive" false "gone" true} m)))))
    (rm-rf dir)))

;; ---- integration helpers -------------------------------------------------

(defn- config-path!
  [data-dir]
  (rm-rf data-dir)
  (.mkdirs (jio/file data-dir))
  (let [config {:sstable-dir (str data-dir "/sstable")
                :wal-dir (str data-dir "/wal")
                ;; small memtable so a few hundred writes make enough L0 tables
                ;; to trip the compaction trigger and cascade through levels
                :memtable-size 256
                :sync-window-time 10}
        path (str data-dir "/config.yaml")]
    (with-open [w (jio/writer path)]
      (.write w (yaml/generate-string config)))
    path))

(defn- version-of [kvs] @(:current-version (:tree kvs)))

(defn- wait-until
  [pred]
  (let [deadline (+ (System/currentTimeMillis) 15000)]
    (loop []
      (cond
        (pred) true
        (> (System/currentTimeMillis) deadline) false
        :else (do (Thread/sleep 20) (recur))))))

(defn- fill!
  [kvs from n]
  (dotimes [i n]
    (igel/write! kvs (->bytes (format "k%05d" (+ from i))) (->bytes (str i)))))

(defn- l1+-populated? [kvs]
  (some #(seq (nth (version-of kvs) % []))
        (range 1 (count (version-of kvs)))))

(defn- version-ids [kvs] (mapv #(mapv first %) (version-of kvs)))

(defn- wait-stable
  "Wait until the version stops changing (compaction idle), then return it."
  [kvs]
  (let [deadline (+ (System/currentTimeMillis) 15000)]
    (loop [prev ::none]
      (let [ids (version-ids kvs)]
        (if (or (= ids prev) (> (System/currentTimeMillis) deadline))
          ids
          (do (Thread/sleep 300) (recur ids)))))))

(defn- disk-sstable-ids [sstable-dir]
  (->> (io/list-files sstable-dir)
       (filter #(.endsWith (.getName %) ".sst"))
       (map #(Long/parseLong (re-find #"\d+" (.getName %))))
       set))

;; ---- integration: non-overlapping L1+ output -----------------------------

(deftest l0-to-l1-produces-non-overlapping-output-test
  (let [data-dir "./test-data/compaction-nonoverlap"
        kvs (igel/gen-kvs (config-path! data-dir))]
    (fill! kvs 0 500)
    (is (wait-until #(l1+-populated? kvs)) "compaction should populate L1+")
    (let [version (version-of kvs)]
      (doseq [level (range 1 (count version))
              :let [sorted (sort-by (fn [[_ info]] (:head-key info))
                                    (data/byte-array-comparator)
                                    (nth version level))]]
        (doseq [[[_ a] [_ b]] (partition 2 1 sorted)]
          (is (data/byte-array-smaller? (:tail-key a) (:head-key b))
              (str "L" level " tables overlap")))))
    (igel/close! kvs)
    (rm-rf data-dir)))

;; ---- integration: newest value wins across compaction --------------------

(deftest overwrite-survives-compaction-test
  (let [data-dir "./test-data/compaction-overwrite"
        kvs (igel/gen-kvs (config-path! data-dir))]
    (igel/write! kvs (->bytes "hot") (->bytes "old"))
    (fill! kvs 0 400)
    (is (wait-until #(l1+-populated? kvs)))
    (igel/write! kvs (->bytes "hot") (->bytes "new"))
    (fill! kvs 400 400)
    (wait-until #(l1+-populated? kvs))
    (is (b= (->bytes "new") (igel/select kvs (->bytes "hot")))
        "the newest value wins across levels and compaction")
    (igel/close! kvs)
    (rm-rf data-dir)))

;; ---- integration: deletes survive compaction (no resurrection) -----------

(deftest delete-survives-compaction-test
  (let [data-dir "./test-data/compaction-delete"
        kvs (igel/gen-kvs (config-path! data-dir))]
    (igel/write! kvs (->bytes "gone") (->bytes "x"))
    (fill! kvs 0 400)
    (is (wait-until #(l1+-populated? kvs)))
    (igel/delete! kvs (->bytes "gone"))
    (fill! kvs 400 400)
    (wait-until #(l1+-populated? kvs))
    (is (nil? (igel/select kvs (->bytes "gone")))
        "a deleted key stays deleted through compaction (no resurrection)")
    (igel/close! kvs)
    (rm-rf data-dir)))

;; ---- Step 5: deferred-safe physical deletion -----------------------------

(deftest no-file-not-found-under-concurrent-scan-and-compaction-test
  ;; Compaction now physically deletes superseded input files. A reader that
  ;; grabbed a version referencing those files must finish before the delete
  ;; (the delete waits on the write lock). No read should ever see a missing file.
  (let [data-dir "./test-data/compaction-concurrent-delete"
        kvs (igel/gen-kvs (config-path! data-dir))
        stop (atom false)
        errors (atom [])]
    (fill! kvs 0 100)
    (let [readers (doall
                   (for [_ (range 4)]
                     (future
                       (try
                         (while (not @stop)
                           (doall (igel/scan kvs (->bytes "k00000") (->bytes "k99999")))
                           (dotimes [i 100]
                             (igel/select kvs (->bytes (format "k%05d" i)))))
                         (catch Throwable e (swap! errors conj e))))))]
      (fill! kvs 100 800) ;; heavy compaction (+ deletion) churn
      (reset! stop true)
      (doseq [r readers] @r))
    (is (empty? @errors)
        (str "reads threw during concurrent compaction+deletion: "
             (first @errors)))
    (igel/close! kvs)
    (rm-rf data-dir)))

(deftest superseded-input-files-are-deleted-test
  ;; Once compaction settles, no orphan SSTable files remain: the .sst files on
  ;; disk are exactly those the current version references.
  (let [data-dir "./test-data/compaction-cleanup"
        kvs (igel/gen-kvs (config-path! data-dir))
        sstable-dir (str data-dir "/sstable")]
    (fill! kvs 0 600)
    (wait-stable kvs)
    (let [referenced (set (mapcat identity (version-ids kvs)))
          on-disk (disk-sstable-ids sstable-dir)]
      (is (= referenced on-disk)
          (str "superseded files not cleaned / referenced file missing"
               " -- version=" referenced " disk=" on-disk)))
    (igel/close! kvs)
    (rm-rf data-dir)))

;; ---- Step 6: a live tx's version survives MVCC GC ------------------------

(deftest live-tx-version-survives-compaction-test
  ;; The MVCC guarantee: while a tx is alive, the GC floor (= its snapshot) keeps the
  ;; version it can see. So even after that version is flushed and compacted many
  ;; times, the tx still reads its snapshot value -- never a newer one, never nil.
  (let [data-dir "./test-data/compaction-live-tx"
        kvs (igel/gen-kvs (config-path! data-dir))]
    (igel/write! kvs (->bytes "k") (->bytes "v0"))
    (let [tx (igel/begin-tx kvs)]
      ;; churn: overwrite k and write filler so k@v0 is flushed and compacted while
      ;; the tx is alive (floor pinned at the tx's snapshot preserves v0)
      (dotimes [i 500]
        (igel/write! kvs (->bytes "k") (->bytes (str "v" (inc i))))
        (igel/write! kvs (->bytes (format "f%05d" i)) (->bytes "x")))
      (wait-until #(l1+-populated? kvs))
      (wait-stable kvs)
      (is (b= (->bytes "v0") (igel/tx-get tx (->bytes "k")))
          "the live tx still sees v0 after compaction (its version was not GC'd)")
      (is (b= (->bytes "v500") (igel/select kvs (->bytes "k")))
          "a non-tx read still sees the latest value")
      (igel/rollback-tx tx))
    (igel/close! kvs)
    (rm-rf data-dir)))
