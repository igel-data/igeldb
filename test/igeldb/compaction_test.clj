(ns igeldb.compaction-test
  (:require [clojure.java.io :as jio]
            [clojure.test :refer [deftest is testing]]
            [clj-yaml.core :as yaml]
            [igeldb.compaction :as compaction]
            [igeldb.core :as igel]
            [igeldb.data :as data]
            [igeldb.io :as io])
  (:import (java.io FileOutputStream BufferedOutputStream)))

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

(defn- write-sst!
  "Write [k-str v-str-or-nil] entries as a raw SSTable (nil value = tombstone)."
  [path entries]
  (with-open [fs (FileOutputStream. path)
              os (BufferedOutputStream. fs)]
    (doseq [[k v] entries]
      (io/write-bytes! os (->bytes k))
      (if v (io/write-bytes! os (->bytes v)) (io/write-tombstone! os)))
    (.flush os)
    (-> fs .getChannel (.force true))))

(deftest merge-newest-wins-and-tombstone-at-bottom-test
  (let [dir "./test-data/compaction-merge"]
    (.mkdirs (jio/file dir))
    (let [older (str dir "/older.sst")   ;; lower precedence
          newer (str dir "/newer.sst")]  ;; higher precedence
      (write-sst! older [["alive" "y"] ["gone" "x"]])
      (write-sst! newer [["gone" nil]]) ;; newer tombstone shadows gone=x
      (testing "bottom level: the tombstone (and the value it shadows) are dropped"
        (let [merged (vec (compaction/merge-inputs [older newer] true))]
          (is (= ["alive"] (mapv (comp #(String. %) first) merged))
              "only the live key survives")))
      (testing "non-bottom level: the tombstone is preserved to shadow deeper data"
        (let [merged (compaction/merge-inputs [older newer] false)
              m (into {} (map (fn [[k d]] [(String. k) (:deleted? d)]) merged))]
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
