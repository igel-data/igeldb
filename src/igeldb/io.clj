(ns igeldb.io
  (:require [clojure.java.io :as io]
            [igeldb.data :as data])
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

;; ---- InternalKey encoding (format v2) ------------------------------------
;;
;; An entry's key segment is `seq(8) ++ user_key` (still one length+CRC-framed
;; segment). The value segment is unchanged (bytes, or length 0 = tombstone). The
;; put/delete "type" therefore rides in the value (length-0 tombstone), and seq is
;; carried in the key segment. SSTable files start with a one-byte format version.

(def ^:const SSTABLE_FORMAT_VERSION 2)

(defn encode-ikey
  ^bytes [ikey]
  (let [uk ^bytes (:user-key ikey)
        buf (ByteBuffer/allocate (+ Long/BYTES (count uk)))]
    (.putLong buf (:seq ikey))
    (.put buf uk)
    (.array buf)))

(defn decode-ikey
  [^bytes b]
  (let [buf (ByteBuffer/wrap b)
        s (.getLong buf)
        uk (make-array Byte/TYPE (.remaining buf))]
    (.get buf ^bytes uk)
    (data/->ikey uk s)))

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

(defn write-format-byte!
  "Write the one-byte SSTable format version at the start of a file."
  [^BufferedOutputStream out-stream]
  (.write out-stream (int SSTABLE_FORMAT_VERSION)))

(defn append-entry!
  "Write one entry: key segment (`seq ++ user_key`) then value segment (or a
  length-0 tombstone). Used by both the WAL and SSTable writers."
  [^BufferedOutputStream out-stream [ikey ^data/Data data]]
  (write-bytes! out-stream (encode-ikey ikey))
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
  "Read one entry (key segment then value segment). Returns:
    :eof         - a clean end at a record boundary
    :truncated   - an incomplete entry at the tail
    :corrupt     - a CRC mismatch (mid-file corruption)
    [ikey data]  - a complete entry; `ikey` is an InternalKey, `data` a `Data`"
  [^BufferedInputStream in-stream]
  (let [k (read-data! in-stream)]
    (if (map? k)
      (let [v (read-data! in-stream)]
        (cond
          (map? v) [(decode-ikey (:ok k)) (data/new-data (:ok v))]
          (= :tombstone v) [(decode-ikey (:ok k)) (data/deleted-data)]
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

(defn read-format-byte!
  "Consume and validate the leading SSTable format-version byte."
  [^BufferedInputStream in-stream file-path]
  (let [b (.read in-stream)]
    (when-not (= b SSTABLE_FORMAT_VERSION)
      (throw (ex-info "Unsupported or corrupt SSTable format"
                      {:file (str file-path) :format-byte b})))))

(defn read-value
  "Point read: the newest version of `target-key` (a user_key) with seq <=
  `snapshot-seq`, or nil. Entries are user_key-asc / seq-desc, so the first entry
  with user_key == target and seq <= snapshot is the answer."
  [file-path target-key snapshot-seq]
  (with-open [in-stream (io/input-stream file-path)]
    (read-format-byte! in-stream file-path)
    (loop []
      (let [entry (read-kv-pair! in-stream)]
        (case entry
          :eof nil
          ;; SSTables are fsynced, so any corruption or truncation there is real.
          (:truncated :corrupt)
          (throw (ex-info "SSTable is corrupted"
                          {:file (str file-path) :reason entry}))
          (let [[ikey data] entry
                cmp (.compare (data/byte-array-comparator) (:user-key ikey) target-key)]
            (cond
              (neg? cmp) (recur)                       ;; before the target user_key
              (pos? cmp) nil                           ;; past it -> not present
              (<= (:seq ikey) snapshot-seq) data       ;; newest visible version
              :else (recur))))))))                     ;; version too new; try older

(defn scan-pairs
  "Range read: for each user_key in [from-key, to-key), its newest version with
  seq <= `snapshot-seq`, as [user_key data] (tombstones included; callers filter).
  Entries are user_key-asc / seq-desc."
  [file-path from-key to-key snapshot-seq]
  (with-open [in-stream (io/input-stream file-path)]
    (read-format-byte! in-stream file-path)
    (loop [pairs (transient [])
           emitted nil]
      (let [entry (read-kv-pair! in-stream)]
        (case entry
          :eof (persistent! pairs)
          (:truncated :corrupt)
          (throw (ex-info "SSTable is corrupted"
                          {:file (str file-path) :reason entry}))
          (let [[ikey data] entry
                uk (:user-key ikey)]
            (cond
              (data/byte-array-smaller? uk from-key) (recur pairs emitted)
              (data/byte-array-smaller-or-equal? to-key uk) (persistent! pairs)
              (and emitted (data/byte-array-equals? uk emitted)) (recur pairs emitted)
              (> (:seq ikey) snapshot-seq) (recur pairs emitted)
              :else (recur (conj! pairs [uk data]) uk))))))))
