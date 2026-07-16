(ns igel.core-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :as io]
            [clj-yaml.core :as yaml]
            [igel.core :as igel]
            [igel.data :as data]
            [igel.wal :as wal]))

;; If this env var is true, test directories will be left for debugging
(def ^:private ^:const LEAVE_TEST_DIR "LEAVE_TEST_DIR")

(defn- make-test-config
  [data-dir]
  {:sstable-dir (str data-dir "/sstable")
   :wal-dir (str data-dir "/wal")
   :memtable-size 1024
   ;; Small window keeps single-threaded tests fast: a lone writer never fills a
   ;; group-commit batch, so each write waits at most this long for its fsync.
   :sync-window-time 20})

(defn- delete-test-dir!
  [file-or-dir force?]
  (when (or force? (not (System/getenv LEAVE_TEST_DIR)))
    (if (.isDirectory file-or-dir)
      (do
        (doseq [i (.list file-or-dir)]
          (delete-test-dir! (io/file (str file-or-dir \/ i)) force?))
        (.delete file-or-dir))
      (.delete file-or-dir))))

(defn- setup-test!
  [data-dir test-config]
  (let [config-path (str data-dir "/config.yaml")]
    ;; setup data-dir
    (let [dir (io/file data-dir)]
      (when (.exists dir)
        (delete-test-dir! dir true))
      (.mkdirs dir))
    ;; make a config file
    (with-open [writer (io/writer config-path)]
      (.write writer (yaml/generate-string test-config)))
    config-path))

(def NUM_ITEMS 512)

(deftest sequencial-crud-test
  (let [data-dir (str "./test-data/sequencial-crud-test")
        test-config (make-test-config data-dir)
        config-path (setup-test! data-dir test-config)
        kvs (igel/gen-kvs config-path)]
    ;; insert
    (doseq [i (range 0 NUM_ITEMS)]
      (let [k (.getBytes (str "key" i))
            v (.getBytes (str "val" i))]
        (igel/write! kvs k v)))
    ;; delete
    (doseq [i (range 0 NUM_ITEMS)]
      (when (zero? (mod i 16))
        (let [k (.getBytes (str "key" i))]
          (igel/delete! kvs k))))
    ;; update
    (doseq [i (range 0 NUM_ITEMS)]
      (when (zero? (mod i 32))
        (let [k (.getBytes (str "key" i))
              v (.getBytes (str "overwritten-val" i))]
          (igel/write! kvs k v))))
    ;; select
    (doseq [i (range 0 NUM_ITEMS)]
      (let [k (.getBytes (str "key" i))
            expected (cond
                       (zero? (mod i 32)) (.getBytes (str "overwritten-val" i))
                       (zero? (mod i 16)) nil
                       :else (.getBytes (str "val" i)))
            actual (igel/select kvs k)]
        (is (data/byte-array-equals? expected actual)
            (str "The result of `select` is wrong: "
                 "\n  expected: " (if (nil? expected)
                                    "nil"
                                    (String. expected))
                 "\n  actual:   " (if (nil? actual)
                                    "nil"
                                    (String. actual))))))
    ;; scan
    (doseq [group (partition 16 (sort-by str (range 0 NUM_ITEMS)))]
      (let [from-key (.getBytes (str "key" (first group)))
            to-key (.getBytes (str "key" (last group) 0))
            expect (filter #(not (nil? %))
                           (for [i group]
                             (cond
                               (zero? (mod i 32)) [(.getBytes (str "key" i))
                                                   (.getBytes
                                                    (str "overwritten-val" i))]
                               (zero? (mod i 16)) nil
                               :else [(.getBytes (str "key" i))
                                      (.getBytes (str "val" i))])))
            expect-results (mapv (fn [[k v]] [(String. k) (String. v)]) expect)
            actual (igel/scan kvs from-key to-key)
            actual-results (mapv (fn [[k v]] [(String. k) (String. v)]) actual)]
        (is (= (count expect) (count actual))
            (str "The number of results was wrong:"
                 "\n  expected: " expect-results
                 "\n  actual:   " actual-results))
        (is (every? true?
                    (map
                     (fn [[k1 v1] [k2 v2]]
                       (and (data/byte-array-equals? k1 k2)
                            (data/byte-array-equals? v1 v2)))
                     expect
                     actual))
            (str "Some results were wrong:"
                 "\n  expected: " expect-results
                 "\n  actual:   " actual-results))))
    (delete-test-dir! (io/file data-dir) false)))

(deftest empty-store-select-test
  ;; A fresh store has no SSTables at all. `select`/`scan` on a missing key must
  ;; return nil / empty rather than throwing (a FileNotFoundException on the
  ;; nonexistent "dir/.sst" path used to escape when the table list was empty).
  (let [data-dir (str "./test-data/empty-store-select-test")
        test-config (make-test-config data-dir)
        config-path (setup-test! data-dir test-config)
        kvs (igel/gen-kvs config-path)]
    (is (nil? (igel/select kvs (.getBytes "no-such-key"))))
    (is (empty? (igel/scan kvs (.getBytes "a") (.getBytes "z"))))
    (.finalize kvs)
    (delete-test-dir! (io/file data-dir) false)))

(deftest restore-test
  (let [data-dir (str "./test-data/restore-test")
        test-config (make-test-config data-dir)
        config-path (setup-test! data-dir test-config)]
    (let [kvs (igel/gen-kvs config-path)]
      ;; insert
      (doseq [i (range 0 NUM_ITEMS)]
        (let [k (.getBytes (str "key" i))
              v (.getBytes (str "val" i))]
          (igel/write! kvs k v)))
      (.finalize kvs))
    ;; drop the current kvs and restart
    (let [kvs (igel/gen-kvs config-path)]
      (doseq [i (range 0 NUM_ITEMS)]
        (let [k (.getBytes (str "key" i))
              expected (.getBytes (str "val" i))
              actual (igel/select kvs k)]
          (is (data/byte-array-equals? expected actual)
              (str "The result of `select` is wrong: "
                   "\n  expected: " (if (nil? expected)
                                      "nil"
                                      (String. expected))
                   "\n  actual:   " (if (nil? actual)
                                      "nil"
                                      (String. actual)))))))
    (delete-test-dir! (io/file data-dir) false)))

(deftest concurrent-distinct-keys-test
  ;; Many threads writing distinct keys concurrently. Every write must be
  ;; applied to the memtable (no lost updates), and the group-commit worker must
  ;; batch multiple entries per fsync rather than one fsync per write.
  (let [data-dir (str "./test-data/concurrent-distinct-keys-test")
        num-threads 32
        per-thread 20
        num-writes (* num-threads per-thread)
        test-config (assoc (make-test-config data-dir)
                           ;; large memtable so no flush interferes with the
                           ;; fsync count; wide window so concurrent writers
                           ;; accumulate into shared batches.
                           :memtable-size (* 1024 1024)
                           :sync-window-time 100
                           :group-commit-limit 100000)
        config-path (setup-test! data-dir test-config)
        kvs (igel/gen-kvs config-path)]
    (reset! wal/fsync-count 0)
    (let [fs (doall
              (for [t (range num-threads)]
                (future
                  (doseq [i (range per-thread)]
                    (igel/write! kvs
                                 (.getBytes (str "k-" t "-" i))
                                 (.getBytes (str "v-" t "-" i)))))))]
      (doseq [f fs] @f))
    ;; every write is visible
    (doseq [t (range num-threads)
            i (range per-thread)]
      (let [expected (.getBytes (str "v-" t "-" i))
            actual (igel/select kvs (.getBytes (str "k-" t "-" i)))]
        (is (data/byte-array-equals? expected actual)
            (str "Lost concurrent write for k-" t "-" i))))
    ;; group commit batched: average batch size >= 2 (a single fsync per write
    ;; would give fsync-count == num-writes).
    (is (< (* 2 @wal/fsync-count) num-writes)
        (str "Group commit did not batch: fsyncs=" @wal/fsync-count
             " writes=" num-writes))
    (.finalize kvs)
    (delete-test-dir! (io/file data-dir) false)))

(deftest concurrent-same-key-invariant-test
  ;; For concurrent writes to the SAME key, the WAL-append order and the
  ;; memtable-apply order are identical (a single worker thread does both), so
  ;; the value observed before a crash must equal the value after WAL replay.
  ;; We verify this directly: race writers on one key, read the winner, restart
  ;; (forcing a WAL replay), and confirm the value is unchanged.
  (dotimes [round 3]
    (let [data-dir (str "./test-data/concurrent-same-key-" round)
          test-config (make-test-config data-dir)
          config-path (setup-test! data-dir test-config)
          k (.getBytes "hot-key")
          num-threads 16
          before (let [kvs (igel/gen-kvs config-path)]
                   (let [fs (doall
                             (for [t (range num-threads)]
                               (future
                                 (igel/write! kvs k (.getBytes (str "val-" t))))))]
                     (doseq [f fs] @f))
                   (let [v (igel/select kvs k)]
                     (.finalize kvs)
                     v))]
      (is (some? before) "A concurrent write to the key should have survived")
      ;; restart -> replay the WAL -> the surviving value must be identical
      (let [kvs (igel/gen-kvs config-path)
            after (igel/select kvs k)]
        (is (data/byte-array-equals? before after)
            (str "Value diverged across restart (round " round "): before="
                 (when before (String. before))
                 " after=" (when after (String. after))))
        (.finalize kvs))
      (delete-test-dir! (io/file data-dir) false))))
