(ns igel.sstable
  (:require [blossom.core :as blossom]
            [clojure.java.io :as java-io]
            [clojure.tools.logging :as logging]
            [igel.data :as data]
            [igel.io :as io]
            [igel.store :as store])
  (:import (java.io FileOutputStream BufferedOutputStream)
           (java.util.concurrent.locks ReentrantReadWriteLock)))

(defn get-sstable-path
  [id dir]
  (str dir "/" id ".sst"))

(defn get-info-path
  [id dir]
  (str dir "/" id ".info"))

(defrecord TableInfo [bloom-filter head-key tail-key])

(defn write-table-info
  [file-path {:keys [bloom-filter head-key tail-key]} level]
  (with-open [file-stream (FileOutputStream. file-path)]
    (with-open [out-stream (BufferedOutputStream. file-stream)]
      (io/write-bytes! out-stream (io/serialize-long level))
      (io/write-bytes! out-stream (blossom/serialize-filter bloom-filter))
      (io/write-bytes! out-stream head-key)
      (io/write-bytes! out-stream tail-key)
      (.flush out-stream)
      (-> file-stream .getFD .sync))))

;; Returns [Level TableInfo]
(defn read-table-info
  [file-path {:keys [bloom-filter]}]
  (with-open [in-stream (java-io/input-stream file-path)]
    ;; Every field is required; `read-bytes!` throws on any corruption/truncation.
    (let [level (io/deserialize-long (io/read-bytes! in-stream file-path))
          bf (blossom/deserialize-filter (io/read-bytes! in-stream file-path)
                                         bloom-filter)
          head-key (io/read-bytes! in-stream file-path)
          tail-key (io/read-bytes! in-stream file-path)]
      [level (->TableInfo bf head-key tail-key)])))

;; SSTable is managed as `[table-id TableInfo]`.
;; The `sstables` in TreeStore have them grouped by levels.
;; For example, [[[id8 info8] [id10 info10]] [[id3 info3]] [[id7 info7]]]
;;               <------- Level 0 ---------> <- Level 1 -> <- Level 2 ->
;; The ID is assigned by the created order,
;; i.e. the table with a smaller ID is older than tables with larger IDs.

(defrecord TreeStore [dir sstables rw-lock]
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
              (if (empty? tables)
                (->> pairs .entrySet (map (fn [e] [(.getKey e) (.getValue e)])))
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
                 (rest tables))))))
        (finally (.unlock read-lock))))))

(defn- restore-table-info
  [{:keys [sstable-dir] :as config} indexes]
  (->> indexes
       (map (fn [index]
              (let [[level table] (read-table-info
                                   (get-info-path index sstable-dir)
                                   config)]
                [level [index table]])))
       (sort-by first)
       (group-by first)
       (mapv (fn [[_level tables]] (mapv second tables)))))

(defn restore-tree-store
  [{:keys [sstable-dir] :as config}]
  (io/make-dir sstable-dir)
  (let [indexes (->> (io/list-files sstable-dir)
                     (filter #(and (.isFile %)
                                   (.endsWith (.getName %) ".info")))
                     (map #(Integer. (re-find #"\d+" (.getName %))))
                     (remove nil?)
                     sort)
        tables (restore-table-info config indexes)
        sstable-id (if (empty? indexes)
                     0
                     (-> (last indexes) (quot 2) inc (* 2)))]
    (logging/info "Restoring SSTables:" indexes)
    [(->TreeStore sstable-dir
                  (atom (if (empty? tables) [[]] tables))
                  (ReentrantReadWriteLock. true))
     sstable-id]))

(defn- append
  [prev id info]
  (let [level-zero (first prev)
        r (rest prev)]
    (cons (conj level-zero [id info]) r)))

(defn add-new-table!
  [tree new-id new-table-info]
  (let [write-lock (.writeLock (:rw-lock tree))]
    (.lock write-lock)
    (try
      (swap! (:sstables tree) #(append % new-id new-table-info))
      (finally (.unlock write-lock)))))
