(ns igeldb.channel-cache-test
  "Phase 4.5: the cached SSTable read channels. The whole risk is lifecycle -- a
  cached channel must never outlive its file -- so these tests focus on eviction at
  the compaction delete site, release on shutdown, and correctness under
  concurrent readers across many SSTables."
  (:require [clojure.java.io :as jio]
            [clojure.test :refer [deftest is testing]]
            [clj-yaml.core :as yaml]
            [igeldb.core :as igel]
            [igeldb.data :as data])
  (:import (java.nio.channels FileChannel)))

(defn- ->bytes [^String s] (.getBytes s))
(defn- b= [a b] (data/byte-array-equals? a b))

(defn- delete-recursively!
  [file]
  (when (.isDirectory file)
    (doseq [c (.listFiles file)] (delete-recursively! c)))
  (.delete file))

(defn- rm-rf [dir] (delete-recursively! (jio/file dir)))

(defn- config-path!
  [data-dir]
  (rm-rf data-dir)
  (.mkdirs (jio/file data-dir))
  (let [config {:sstable-dir (str data-dir "/sstable")
                :wal-dir (str data-dir "/wal")
                ;; small memtable so a few hundred writes flush and compact
                :memtable-size 256
                :sync-window-time 10}
        path (str data-dir "/config.yaml")]
    (with-open [w (jio/writer path)]
      (.write w (yaml/generate-string config)))
    path))

(defn- k [i] (->bytes (format "k%05d" i)))
(defn- v [i] (->bytes (str "v" i)))

(defn- fill! [kvs from n]
  (dotimes [i n] (igel/write! kvs (k (+ from i)) (v (+ from i)))))

(defn- version-ids [kvs]
  (set (mapcat #(map first %) @(:current-version (:tree kvs)))))

(defn- wait-stable
  "Wait until the table set stops changing (compaction idle)."
  [kvs]
  (let [deadline (+ (System/currentTimeMillis) 15000)]
    (loop [prev ::none]
      (let [ids (version-ids kvs)]
        (if (or (= ids prev) (> (System/currentTimeMillis) deadline))
          ids
          (do (Thread/sleep 300) (recur ids)))))))

(defn- cached [kvs] @(:channels (:tree kvs)))

;; ---- lifecycle: eviction at the compaction delete site -------------------

(deftest compacted-away-tables-have-their-channels-closed-test
  ;; A cached channel must never outlive its file. Compaction deletes superseded
  ;; SSTables under the write lock; the channels must be closed and evicted there.
  (let [data-dir "./test-data/chan-evict"
        kvs (igel/gen-kvs (config-path! data-dir))]
    (fill! kvs 0 400)
    (wait-stable kvs)
    ;; read everything so channels are cached for the current table set
    (dotimes [i 400] (igel/select kvs (k i)))
    (let [before (cached kvs)]
      (is (seq before) "reads should have cached some channels")
      ;; more churn -> compaction supersedes and deletes tables
      (fill! kvs 400 600)
      (wait-stable kvs)
      (dotimes [i 100] (igel/select kvs (k i)))
      (let [live (version-ids kvs)]
        (testing "no cached channel refers to a table that no longer exists"
          (is (every? live (keys (cached kvs)))
              (str "stale cache entries: "
                   (vec (remove live (keys (cached kvs)))))))
        (testing "channels of compacted-away tables were actually closed"
          (let [gone (remove live (keys before))]
            (is (seq gone) "expected compaction to retire some tables")
            (doseq [id gone]
              (is (not (.isOpen ^FileChannel (get before id)))
                  (str "channel for retired table " id " was left open (fd leak)")))))))
    (testing "data is still correct after all that eviction"
      (doseq [i (range 0 1000 37)]
        (is (b= (v i) (igel/select kvs (k i))) (str "wrong value for k" i))))
    (igel/close! kvs)
    (rm-rf data-dir)))

(deftest close-releases-every-cached-channel-test
  (let [data-dir "./test-data/chan-close"
        kvs (igel/gen-kvs (config-path! data-dir))]
    (fill! kvs 0 300)
    (wait-stable kvs)
    (dotimes [i 300] (igel/select kvs (k i)))
    (let [channels (vals (cached kvs))]
      (is (seq channels) "expected cached channels before close")
      (igel/close! kvs)
      (testing "close! closes every cached channel and empties the cache"
        (doseq [^FileChannel ch channels]
          (is (not (.isOpen ch)) "a cached channel survived close! (fd leak)"))
        (is (empty? (cached kvs)))))
    (rm-rf data-dir)))

;; ---- correctness under concurrent readers --------------------------------

(deftest concurrent-reads-across-many-sstables-test
  ;; One channel per table is shared by all readers via absolute positional reads.
  ;; Every reader must still see correct values, with compaction churning
  ;; underneath (which closes and evicts channels as it goes).
  (let [data-dir "./test-data/chan-concurrent"
        kvs (igel/gen-kvs (config-path! data-dir))
        n 500
        errors (atom [])]
    (fill! kvs 0 n)
    (wait-stable kvs)
    (is (> (count (version-ids kvs)) 1) "expected several SSTables")
    (let [readers (doall
                   (for [t (range 8)]
                     (future
                       (try
                         (dotimes [i 300]
                           (let [idx (mod (* (inc t) (inc i) 7919) n)
                                 got (igel/select kvs (k idx))]
                             (when-not (b= (v idx) got)
                               (swap! errors conj
                                      [idx (some-> got (String.))]))))
                         (catch Throwable e (swap! errors conj (str e)))))))]
      ;; keep compaction (and therefore channel eviction) running underneath
      (fill! kvs n 400)
      (doseq [r readers] @r))
    (is (empty? @errors)
        (str "concurrent reads returned wrong values or threw: "
             (take 3 @errors)))
    (igel/close! kvs)
    (rm-rf data-dir)))
