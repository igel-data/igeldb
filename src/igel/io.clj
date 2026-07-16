(ns igel.io
  (:require [clojure.java.io :as io]
            [igel.data :as data])
  (:import (java.io BufferedInputStream
                    BufferedOutputStream)
           (java.nio ByteBuffer)
           (java.util.zip CRC32)))

(defn serialize-long
  [value]
  (let [buf (ByteBuffer/allocate Long/BYTES)]
    (.putLong buf value)
    (.array buf)))

(defn deserialize-long
  [bytes]
  (.getLong (ByteBuffer/wrap bytes)))

(defn- calc-crc32 [data]
  (let [crc32 (CRC32.)]
    (.update crc32 data)
    (.getValue crc32)))

(defn- valid-data? [data crc]
  (let [crc32 (doto (CRC32.) (.update data))]
    (= (.getValue crc32) crc)))

(defn make-dir
  [dir]
  (let [dir (io/file dir)]
    (when-not (and (.exists dir) (.isDirectory dir))
      (.mkdirs dir))))

(defn delete-file
  [file-path]
  (let [file (io/file file-path)]
    (when (.exists file)
      (.delete file))))

(defn list-files
  [dir]
  (file-seq (io/file dir)))

; Data format in an SSTable
; | Key0 | Value0 | Key1 | Value1 | ...
; - Each key or value's data format
; | Length (8 bytes) | Data (Length bytes) | CRC (4 byters) |

(def ^:private ^:const LEN_SIZE Long/BYTES)
(def ^:private ^:const CRC_SIZE Long/BYTES)

(defn write-bytes!
  [^BufferedOutputStream out-stream ^bytes b]
  (doto out-stream
    (.write (serialize-long (count b)))
    (.write b)
    (.write (serialize-long (calc-crc32 b)))))

(defn write-tombstone!
  [^BufferedOutputStream out-stream]
  (.write out-stream (serialize-long 0)))

(defn append-wal!
  [^BufferedOutputStream out-stream [^bytes k ^data/Data data]]
  (write-bytes! out-stream k)
  (if (:deleted? data)
    (write-tombstone! out-stream)
    (write-bytes! out-stream (:value data))))

(defn read-data!
  "Return the byte-array of the data segment from the input stream.
  If the data length is zero, it returns nil."
  [^BufferedInputStream in-stream]
  (let [buf (make-array Byte/TYPE LEN_SIZE)
        read-len (.readNBytes in-stream buf 0 LEN_SIZE)
        data-len (deserialize-long buf)]
    (when (and (= read-len LEN_SIZE) (> data-len 0))
      (let [buf (make-array Byte/TYPE data-len)
            read-len (.readNBytes in-stream buf 0 data-len)
            crc-buf (make-array Byte/TYPE CRC_SIZE)
            crc-len (.readNBytes in-stream crc-buf 0 CRC_SIZE)]
        ;; TODO throw an exception when unexpected length or crc error
        (when (and (= read-len data-len)
                   (= crc-len CRC_SIZE)
                   (valid-data? buf (deserialize-long crc-buf)))
          buf)))))

(defn read-kv-pair!
  [^BufferedInputStream in-stream]
  [(read-data! in-stream) (read-data! in-stream)])

(defn read-value
  [file-path target-key]
  (with-open [in-stream (io/input-stream file-path)]
    (loop []
      (let [[k v] (read-kv-pair! in-stream)]
        (if (data/byte-array-equals? k target-key)
          (if (nil? v)
            (data/deleted-data)
            (data/new-data v))
          (if (nil? k) nil (recur)))))))

(defn scan-pairs
  [file-path from-key to-key]
  (with-open [in-stream (io/input-stream file-path)]
    (loop [pairs (transient [])]
      (let [[k v] (read-kv-pair! in-stream)
            data (cond
                   (and (seq k) (seq v)) (data/new-data v)
                   (and (seq k) (nil? v)) (data/deleted-data)
                   :else nil)]
        (if (nil? k)
          (persistent! pairs)
          (if (data/byte-array-smaller-or-equal? to-key k)
            (persistent! pairs)
            (recur
             (if (data/byte-array-smaller-or-equal? from-key k)
               (conj! pairs [k data])
               pairs))))))))
