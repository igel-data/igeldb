(ns igel.io-test
  (:require [clojure.java.io :as jio]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [igel.data :as data]
            [igel.io :as io]
            [igel.wal :as wal])
  (:import (java.io FileOutputStream BufferedOutputStream RandomAccessFile)))

(def ^:private ^:const TMP_DIR "test-data/io-test")

(use-fixtures :each
  (fn [t]
    (io/make-dir TMP_DIR)
    (t)
    (doseq [f (.listFiles (jio/file TMP_DIR))] (.delete f))))

(defn- write-wal-file!
  "Write entries to a WAL file. Each entry is [k-bytes v-bytes]; a nil value
  writes a tombstone."
  [path entries]
  (with-open [fs (FileOutputStream. path)
              os (BufferedOutputStream. fs)]
    (doseq [[k v] entries]
      (io/append-wal! os [k (if v (data/new-data v) (data/deleted-data))]))
    (.flush os)
    (-> fs .getFD .sync)))

(defn- truncate-file!
  [path new-len]
  (with-open [raf (RandomAccessFile. (str path) "rw")]
    (.setLength raf new-len)))

(defn- flip-byte!
  [path offset]
  (with-open [raf (RandomAccessFile. (str path) "rw")]
    (.seek raf offset)
    (let [b (.read raf)]
      (.seek raf offset)
      (.write raf (bit-xor b 0xff)))))

(def ^:private entries
  [[(.getBytes "k1") (.getBytes "v1")]
   [(.getBytes "k2") nil]                 ;; tombstone
   [(.getBytes "k3") (.getBytes "v3")]])

(deftest read-data-classification-test
  (let [path (str TMP_DIR "/classify.wal")]
    (write-wal-file! path entries)
    (with-open [in (jio/input-stream path)]
      (testing "a complete live entry, a tombstone, then a clean EOF"
        (let [e1 (io/read-kv-pair! in)
              e2 (io/read-kv-pair! in)
              e3 (io/read-kv-pair! in)
              e4 (io/read-kv-pair! in)]
          (is (data/byte-array-equals? (.getBytes "k1") (first e1)))
          (is (data/is-valid? (second e1)))
          (is (data/byte-array-equals? (.getBytes "v1") (:value (second e1))))
          (is (data/byte-array-equals? (.getBytes "k2") (first e2)))
          (is (not (data/is-valid? (second e2))) "k2 is a tombstone")
          (is (data/byte-array-equals? (.getBytes "k3") (first e3)))
          (is (= :eof e4) "reading past the last entry yields :eof"))))))

(deftest tail-truncation-is-truncated-test
  ;; Cutting bytes off the last entry must surface as :truncated after the valid
  ;; prefix -- the normal "died before fsync" case at a WAL tail.
  (let [path (str TMP_DIR "/trunc.wal")]
    (write-wal-file! path entries)
    (let [len (.length (jio/file path))]
      ;; drop the last 3 bytes: the tail entry's CRC is now incomplete
      (truncate-file! path (- len 3)))
    (with-open [in (jio/input-stream path)]
      (is (vector? (io/read-kv-pair! in)) "k1 intact")
      (is (vector? (io/read-kv-pair! in)) "k2 intact")
      (is (= :truncated (io/read-kv-pair! in)) "partial tail entry is :truncated"))))

(deftest midfile-crc-mismatch-is-corrupt-test
  ;; Flip a byte inside the FIRST entry's value data. The length prefix is
  ;; intact, so the whole segment is read but the CRC no longer matches.
  (let [path (str TMP_DIR "/corrupt.wal")]
    (write-wal-file! path entries)
    ;; layout of entry 1: key seg (8 len + 2 data + 8 crc = 18) then value seg;
    ;; offset 26 is the first byte of "v1".
    (flip-byte! path 26)
    (with-open [in (jio/input-stream path)]
      (is (= :corrupt (io/read-kv-pair! in)) "CRC mismatch surfaces as :corrupt"))))

(deftest load-existing-wal-truncation-keeps-prefix-test
  ;; A WAL whose tail entry is torn loads its valid prefix without throwing.
  (let [wal-dir (str TMP_DIR "/wal-trunc")]
    (io/make-dir wal-dir)
    (let [path (str wal-dir "/0.wal")]
      (write-wal-file! path entries)
      (truncate-file! path (- (.length (jio/file path)) 3)))
    (let [pairs (wal/load-existing-wal {:wal-dir wal-dir})]
      (is (= 2 (count pairs)) "only the two intact entries are replayed")
      (is (data/byte-array-equals? (.getBytes "k1") (ffirst pairs)))
      (is (data/byte-array-equals? (.getBytes "k2") (first (second pairs)))))))

(deftest load-existing-wal-midfile-corruption-throws-test
  (let [wal-dir (str TMP_DIR "/wal-corrupt")]
    (io/make-dir wal-dir)
    (let [path (str wal-dir "/0.wal")]
      (write-wal-file! path entries)
      (flip-byte! path 26))
    (is (thrown? clojure.lang.ExceptionInfo
                 (wal/load-existing-wal {:wal-dir wal-dir}))
        "mid-file corruption must raise, not silently truncate the log")))
