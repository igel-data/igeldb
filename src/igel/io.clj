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
  "Read one length-prefixed, CRC-checked data segment, classifying the outcome
  so callers can distinguish the four error classes. Returns one of:

    :eof        - a clean end at a record boundary (nothing left to read)
    :truncated  - an incomplete entry: EOF was reached partway through the
                  length prefix, the data, or the CRC. Normal only at a WAL
                  tail (a crash before the write's fsync completed).
    :tombstone  - a deletion marker (segment length 0)
    :corrupt    - the whole segment was read but its CRC did not match, or the
                  length prefix is nonsensical (mid-file corruption)
    {:ok bytes} - a valid data segment"
  [^BufferedInputStream in-stream]
  (let [len-buf (make-array Byte/TYPE LEN_SIZE)
        len-read (.readNBytes in-stream len-buf 0 LEN_SIZE)]
    (cond
      (zero? len-read) :eof
      (< len-read LEN_SIZE) :truncated
      :else
      (let [data-len (deserialize-long len-buf)]
        (cond
          (zero? data-len) :tombstone
          (neg? data-len) :corrupt
          :else
          (let [data-buf (make-array Byte/TYPE data-len)
                data-read (.readNBytes in-stream data-buf 0 data-len)
                crc-buf (make-array Byte/TYPE CRC_SIZE)
                crc-read (.readNBytes in-stream crc-buf 0 CRC_SIZE)]
            (cond
              (or (< data-read data-len) (< crc-read CRC_SIZE)) :truncated
              (valid-data? data-buf (deserialize-long crc-buf)) {:ok data-buf}
              :else :corrupt)))))))

(defn read-bytes!
  "Read a data segment that is required to be present and valid, e.g. a field of
  a fixed-format file (SSTable info). Throws on any EOF/truncation/corruption."
  [^BufferedInputStream in-stream file-path]
  (let [r (read-data! in-stream)]
    (if (map? r)
      (:ok r)
      (throw (ex-info "Corrupted or truncated file segment"
                      {:file (str file-path) :reason r})))))

(defn read-kv-pair!
  "Read one key-value entry (key segment then value segment). Returns:
    :eof        - a clean end at a record boundary
    :truncated  - an incomplete entry at the tail
    :corrupt    - a CRC mismatch (mid-file corruption)
    [k data]    - a complete entry; `data` is a `data/Data` (value or tombstone)"
  [^BufferedInputStream in-stream]
  (let [k (read-data! in-stream)]
    (if (map? k)
      (let [v (read-data! in-stream)]
        (cond
          (map? v) [(:ok k) (data/new-data (:ok v))]
          (= :tombstone v) [(:ok k) (data/deleted-data)]
          (= :corrupt v) :corrupt
          ;; :eof or :truncated after a key = an incomplete entry
          :else :truncated))
      ;; key segment itself terminated the read (or was invalid):
      ;; :eof -> clean end; :truncated -> incomplete; a zero-length key
      ;; (:tombstone) is not valid and is treated as corruption.
      (case k
        :eof :eof
        :truncated :truncated
        :corrupt))))

(defn read-value
  [file-path target-key]
  (with-open [in-stream (io/input-stream file-path)]
    (loop []
      (let [entry (read-kv-pair! in-stream)]
        (case entry
          :eof nil
          ;; SSTables are fsynced, so any corruption or truncation there is real.
          (:truncated :corrupt)
          (throw (ex-info "SSTable is corrupted"
                          {:file (str file-path) :reason entry}))
          (let [[k data] entry]
            (if (data/byte-array-equals? k target-key)
              data
              (recur))))))))

(defn scan-pairs
  [file-path from-key to-key]
  (with-open [in-stream (io/input-stream file-path)]
    (loop [pairs (transient [])]
      (let [entry (read-kv-pair! in-stream)]
        (case entry
          :eof (persistent! pairs)
          (:truncated :corrupt)
          (throw (ex-info "SSTable is corrupted"
                          {:file (str file-path) :reason entry}))
          (let [[k data] entry]
            (if (data/byte-array-smaller-or-equal? to-key k)
              (persistent! pairs)
              (recur
               (if (data/byte-array-smaller-or-equal? from-key k)
                 (conj! pairs [k data])
                 pairs)))))))))
