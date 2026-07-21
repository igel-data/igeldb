(ns igeldb.io
  (:require [clojure.java.io :as io]
            [igeldb.data :as data])
  (:import (java.io BufferedInputStream
                    BufferedOutputStream
                    ByteArrayInputStream
                    ByteArrayOutputStream)
           (java.nio ByteBuffer)
           (java.nio.channels FileChannel)
           (java.nio.file Paths OpenOption StandardOpenOption)
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

;; v3 adds the sparse block index + trailer footer (Phase 4). The entry encoding
;; itself is unchanged from v2; only the file gained a footer.
(def ^:const SSTABLE_FORMAT_VERSION 3)

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

(defn entry-size-on-disk
  "Exact number of bytes `append-entry!` writes for one entry: the key segment
  (len + seq + user_key + CRC) plus the value segment (len + value + CRC, or a
  bare length-0 long for a tombstone). Authoritative -- the SSTable writer uses it
  to cut index blocks and compaction uses it to size output tables."
  ^long [ikey data]
  (+ LEN_SIZE Long/BYTES (count ^bytes (:user-key ikey)) CRC_SIZE
     (if (:deleted? data)
       LEN_SIZE
       (+ LEN_SIZE (count ^bytes (:value data)) CRC_SIZE))))

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
  [^java.io.InputStream in-stream]
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

(def ^:dynamic *entries-read*
  "Test instrumentation: bind to an atom and `read-kv-pair!` counts every entry it
  deserializes, so a test can prove an index-directed read touches only its block
  rather than scanning the file. nil in production -- a nil check, no atom traffic
  on the read path."
  nil)

(defn read-kv-pair!
  "Read one entry (key segment then value segment). Returns:
    :eof         - a clean end at a record boundary
    :truncated   - an incomplete entry at the tail
    :corrupt     - a CRC mismatch (mid-file corruption)
    [ikey data]  - a complete entry; `ikey` is an InternalKey, `data` a `Data`"
  [^java.io.InputStream in-stream]
  (let [k (read-data! in-stream)]
    (if (map? k)
      (let [v (read-data! in-stream)]
        (when *entries-read* (swap! *entries-read* inc))
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

;; ---- channel-backed entry reads (Phase 4.5) ------------------------------
;;
;; Reads go through a cached `FileChannel` using ABSOLUTE positional reads
;; (`read(ByteBuffer, position)`), which are safe for many concurrent readers on a
;; single channel and never disturb a shared file position. That removes the
;; per-read open()/close(): ~19us, roughly 30% of a point read, and a throughput
;; ceiling under concurrency. Verified bb-safe.
;;
;; Entries are parsed from in-memory chunks pulled off the channel, so the
;; framing/CRC/corruption logic is reused untouched.

(def ^:private ^:const READ_CHUNK 16384)

(defn- read-at!
  "Read up to `n` bytes at absolute position `pos`. Shorter than `n` only at EOF."
  ^bytes [^FileChannel ch ^long pos ^long n]
  (let [buf (ByteBuffer/allocate n)]
    (loop [p pos]
      (if (.hasRemaining buf)
        (let [got (.read ch buf p)]
          (if (neg? got)
            (java.util.Arrays/copyOf (.array buf) (.position buf))
            (recur (+ p got))))
        (.array buf)))))

(defn- entry-seq
  "A lazy seq of `[pos [ikey data]]` from `start` up to `limit` (where the entries
  region ends), read from `ch` in chunks. Lazy per entry, so a point read parses
  only what it consumes rather than a whole chunk.

  A chunk boundary landing mid-entry is NOT corruption: the reader refills from the
  start of that incomplete entry, doubling the chunk so an oversized entry always
  eventually fits. A CRC mismatch IS corruption and raises."
  [^FileChannel ch start limit file-path first-chunk]
  (letfn [(corrupt! [reason]
            (throw (ex-info "SSTable is corrupted"
                            {:file (str file-path) :reason reason})))
          (refill [pos want]
            (when (< (long pos) (long limit))
              (let [n (min (long want) (- (long limit) (long pos)))
                    bytes (read-at! ch pos n)]
                (walk pos (ByteArrayInputStream. bytes)
                      (+ pos (alength bytes)) want))))
          (walk [pos in chunk-end want]
            (lazy-seq
             (when (< (long pos) (long limit))
               (let [e (read-kv-pair! in)]
                 (cond
                   (vector? e)
                   (cons [pos e]
                         (walk (+ pos (entry-size-on-disk (first e) (second e)))
                               in chunk-end want))
                   (= :corrupt e) (corrupt! e)
                    ;; chunk ran out mid-entry -> refill from this entry's start
                   (< (long chunk-end) (long limit)) (refill pos (* 2 (long want)))
                   :else (corrupt! e))))))]
    (refill start (max 512 (min (long first-chunk) READ_CHUNK)))))

;; ---- SSTable footer: sparse block index + trailer (format v3) -------------
;;
;; File layout:
;;   format-byte(1) ++ entries... ++ index-region ++ trailer
;;
;; The index is sparse and block-granular: one entry per block, recording the
;; block's FIRST user_key and the block's absolute byte offset. It is keyed by
;; user_key (never InternalKey) -- a point read binary-searches for the block whose
;; first user_key is the largest <= target (a floor lookup), seeks there and scans
;; forward. Because a user_key's versions can in principle straddle a block
;; boundary, the scan must continue past a block's end until the user_key exceeds
;; the target; blocks bound where a read STARTS, not where it stops.
;;
;; Both regions reuse the length+CRC frame, so corruption is caught by the Phase 1
;; classes. The trailer is fixed-size so a reader can find it from the file end.

(def ^:private ^:const TRAILER_PAYLOAD_SIZE (+ Long/BYTES Long/BYTES 1))
(def ^:const TRAILER_SIZE (+ LEN_SIZE TRAILER_PAYLOAD_SIZE CRC_SIZE))

(defn- skip-fully!
  "`InputStream.skip` may skip fewer bytes than asked; loop until `n` are consumed."
  [^java.io.InputStream in ^long n]
  (loop [remaining n]
    (when (pos? remaining)
      (let [skipped (.skip in remaining)]
        (cond
          (pos? skipped) (recur (- remaining skipped))
          ;; skip made no progress: force it with a read so we cannot spin
          (neg? (.read in)) (throw (ex-info "Unexpected EOF while seeking"
                                            {:remaining remaining}))
          :else (recur (dec remaining)))))))

(defn encode-index
  "Serialize the sparse index: block-count, then per block
  keylen ++ first-user_key ++ block-start-offset."
  ^bytes [blocks]
  (let [baos (ByteArrayOutputStream.)]
    (.write baos (serialize-long (count blocks)))
    (doseq [[^bytes k off] blocks]
      (.write baos (serialize-long (count k)))
      (.write baos k)
      (.write baos (serialize-long off)))
    (.toByteArray baos)))

(defn decode-index
  "Parse an index payload into a vector of [first-user_key block-offset], in
  user_key order (ready for binary search)."
  [^bytes payload]
  (let [buf (ByteBuffer/wrap payload)
        n (.getLong buf)]
    (loop [i 0 acc (transient [])]
      (if (= i n)
        (persistent! acc)
        (let [klen (.getLong buf)
              k (make-array Byte/TYPE klen)
              _ (.get buf ^bytes k)
              off (.getLong buf)]
          (recur (inc i) (conj! acc [k off])))))))

(defn write-footer!
  "Append the index region then the fixed-size trailer. `index-start` is the byte
  offset just past the last entry (i.e. where the index region begins)."
  [^BufferedOutputStream out-stream blocks index-start]
  (let [payload (encode-index blocks)]
    (write-bytes! out-stream payload)
    (let [baos (ByteArrayOutputStream.)]
      (.write baos (serialize-long index-start))
      (.write baos (serialize-long (count payload)))
      (.write baos (int SSTABLE_FORMAT_VERSION))
      (write-bytes! out-stream (.toByteArray baos)))))

(defn read-footer!
  "Read the trailer from the file end, then the index region. Returns
  `{:index-start <byte offset where entries end> :index [[user_key offset]...]}`.
  Raises on a corrupt/truncated footer or an unsupported format version."
  [file-path]
  (let [len (.length (io/file file-path))]
    (when (< len TRAILER_SIZE)
      (throw (ex-info "SSTable is too short to hold a trailer"
                      {:file (str file-path) :length len})))
    (let [index-start
          (with-open [in (io/input-stream file-path)]
            (skip-fully! in (- len TRAILER_SIZE))
            (let [r (read-data! in)]
              (when-not (map? r)
                (throw (ex-info "SSTable trailer is corrupted"
                                {:file (str file-path) :reason r})))
              (let [buf (ByteBuffer/wrap ^bytes (:ok r))
                    start (.getLong buf)
                    _ (.getLong buf)          ;; index payload length (unused)
                    fmt (.get buf)]
                (when-not (= (int fmt) SSTABLE_FORMAT_VERSION)
                  (throw (ex-info "Unsupported or corrupt SSTable format"
                                  {:file (str file-path) :format-byte fmt})))
                start)))
          blocks
          (with-open [in (io/input-stream file-path)]
            (skip-fully! in index-start)
            (let [r (read-data! in)]
              (when-not (map? r)
                (throw (ex-info "SSTable index is corrupted"
                                {:file (str file-path) :reason r})))
              (decode-index (:ok r))))]
      {:index-start index-start :index blocks})))

(defn read-all-entries
  "Every [ikey data] entry of an SSTable, in file order (compaction reads whole
  tables). Bounded by the entries region -- reading to EOF would run into the
  index/trailer footer and misparse it as entries. Opens its own channel: a
  compaction reads each input exactly once, so it bypasses the read cache."
  [file-path]
  (let [{:keys [index-start]} (read-footer! file-path)]
    (with-open [ch (FileChannel/open
                    (Paths/get (str file-path) (into-array String []))
                    (into-array OpenOption [StandardOpenOption/READ]))]
      (mapv second (entry-seq ch 1 index-start file-path READ_CHUNK)))))

(defn- floor-block-idx
  "Index position of the block a read for `target-key` must START at, or -1 when
  the index is empty. Lower-bound semantics on equality: if several blocks share
  the target as their first user_key (a user_key split across blocks), the FIRST
  is returned -- entering a later one would skip that key's newest versions and
  silently return a stale answer."
  ^long [index ^bytes target-key]
  (let [cmp (data/byte-array-comparator)
        n (count index)
        lb (loop [lo 0 hi n]
             (if (>= lo hi)
               lo
               (let [mid (quot (+ lo hi) 2)]
                 (if (neg? (.compare cmp (first (nth index mid)) target-key))
                   (recur (inc mid) hi)
                   (recur lo mid)))))]
    (cond
      (zero? n) -1
      (and (< lb n) (zero? (.compare cmp (first (nth index lb)) target-key))) lb
      (pos? lb) (dec lb)
      :else 0)))

(defn floor-block-offset
  "Byte offset of the block a read for `target-key` must start at (see
  `floor-block-idx`). Offset 1 -- just past the format byte -- when there is no
  index."
  ^long [index ^bytes target-key]
  (let [i (floor-block-idx index target-key)]
    (if (neg? i) 1 (second (nth index i)))))

(defn floor-block-span
  "`[start end)` for a read of `target-key`: the floor block's offset and where
  that block ends (the next block's offset, or the end of the entries region).
  Sizing the first channel read to the block avoids pulling in bytes the read will
  never look at."
  [index ^bytes target-key ^long index-start]
  (let [i (floor-block-idx index target-key)]
    (if (neg? i)
      [1 index-start]
      [(second (nth index i))
       (if (< (inc i) (count index))
         (second (nth index (inc i)))
         index-start)])))

(defn read-value
  "Point read: the newest version of `target-key` (a user_key) with seq <=
  `snapshot-seq`, or nil. Entries are user_key-asc / seq-desc, so the first entry
  with user_key == target and seq <= snapshot is the answer.

  `footer` is the table's in-memory `{:index :index-start}`: the read starts at the
  floor block for `target-key` rather than the file start, and stops where the
  entries end. It keeps scanning past block ends until the user_key exceeds the
  target, so a user_key whose versions straddle a block boundary is still found in
  full. Bytes come off the cached channel via absolute positional reads."
  [^FileChannel ch file-path target-key snapshot-seq {:keys [index index-start]}]
  (let [cmp (data/byte-array-comparator)
        [start end] (floor-block-span index target-key index-start)]
    (loop [es (entry-seq ch start index-start file-path (- end start))]
      (when-let [[_ [ikey data]] (first es)]
        (let [c (.compare cmp (:user-key ikey) target-key)]
          (cond
            (neg? c) (recur (next es))                ;; before the target user_key
            (pos? c) nil                              ;; past it -> not present
            (<= (:seq ikey) snapshot-seq) data        ;; newest visible version
            :else (recur (next es))))))))             ;; too new; try an older one

(defn scan-pairs
  "Range read: for each user_key in [from-key, to-key), its newest version with
  seq <= `snapshot-seq`, as [user_key data] (tombstones included; callers filter).
  Entries are user_key-asc / seq-desc. Starts at the block covering `from-key`."
  [^FileChannel ch file-path from-key to-key snapshot-seq {:keys [index index-start]}]
  (loop [es (entry-seq ch (floor-block-offset index from-key) index-start
                       file-path READ_CHUNK)
         pairs (transient [])
         emitted nil]
    (if-let [[_ [ikey data]] (first es)]
      (let [uk (:user-key ikey)]
        (cond
          (data/byte-array-smaller? uk from-key) (recur (next es) pairs emitted)
          (data/byte-array-smaller-or-equal? to-key uk) (persistent! pairs)
          (and emitted (data/byte-array-equals? uk emitted))
          (recur (next es) pairs emitted)
          (> (:seq ikey) snapshot-seq) (recur (next es) pairs emitted)
          :else (recur (next es) (conj! pairs [uk data]) uk)))
      (persistent! pairs))))

(defn read-latest-seq
  "The seq of the *newest* version of `target-key` in an SSTable, or nil if absent.
  Entries are user_key-asc / seq-desc, so the first entry with user_key == target
  is the newest. Used by commit-time write-write conflict detection."
  [^FileChannel ch file-path target-key {:keys [index index-start]}]
  (let [cmp (data/byte-array-comparator)
        [start end] (floor-block-span index target-key index-start)]
    (loop [es (entry-seq ch start index-start file-path (- end start))]
      (when-let [[_ [ikey _]] (first es)]
        (let [c (.compare cmp (:user-key ikey) target-key)]
          (cond
            (neg? c) (recur (next es))
            (pos? c) nil
            :else (:seq ikey)))))))

;; ---- WAL record encoding (format v2) -------------------------------------
;;
;; One transaction = one WAL record (Point 6), wrapped in the same length+bytes+CRC
;; frame `write-bytes!`/`read-data!` already use, so atomicity == frame
;; completeness: a torn tail is `:truncated` (an uncommitted tx, discarded) and a
;; mid-frame flip is `:corrupt` (raises). The framed payload is:
;;   format-byte(1) ++ seq(8) ++ entry-count(8)
;;   ++ for each entry: keylen(8) ++ user_key ++ vallen(8) ++ value
;; A value length of 0 is a tombstone (same convention as the segment encoding).
;; Every entry of a tx shares the record's single seq, so no per-entry seq.

(def ^:const WAL_FORMAT_VERSION 2)

(defn encode-wal-record
  "Serialize one tx (its `seq` and folded `[ikey data]` entries) into the record
  payload bytes. The caller frames it with `write-bytes!` (length + CRC)."
  ^bytes [seq entries]
  (let [baos (ByteArrayOutputStream.)]
    (.write baos (int WAL_FORMAT_VERSION))
    (.write baos (serialize-long seq))
    (.write baos (serialize-long (count entries)))
    (doseq [[ikey data] entries]
      (let [uk ^bytes (:user-key ikey)]
        (.write baos (serialize-long (count uk)))
        (.write baos uk)
        (if (:deleted? data)
          (.write baos (serialize-long 0))
          (let [v ^bytes (:value data)]
            (.write baos (serialize-long (count v)))
            (.write baos v)))))
    (.toByteArray baos)))

(defn write-wal-record!
  "Append one framed WAL record (one tx) to a WAL output stream."
  [^BufferedOutputStream out-stream seq entries]
  (write-bytes! out-stream (encode-wal-record seq entries)))

(defn decode-wal-record
  "Parse a WAL record payload into `[seq entries]`, where each entry is
  `[ikey data]` with `ikey`'s seq set to the record's seq."
  [^bytes payload]
  (let [buf (ByteBuffer/wrap payload)
        fmt (.get buf)]
    (when-not (= (int fmt) WAL_FORMAT_VERSION)
      (throw (ex-info "Unsupported or corrupt WAL record format" {:format-byte fmt})))
    (let [seq (.getLong buf)
          n (.getLong buf)]
      (loop [i 0 acc (transient [])]
        (if (= i n)
          [seq (persistent! acc)]
          (let [klen (.getLong buf)
                k (make-array Byte/TYPE klen)
                _ (.get buf ^bytes k)
                vlen (.getLong buf)
                data (if (zero? vlen)
                       (data/deleted-data)
                       (let [v (make-array Byte/TYPE vlen)]
                         (.get buf ^bytes v)
                         (data/new-data v)))]
            (recur (inc i) (conj! acc [(data/->ikey k seq) data]))))))))

(defn read-wal-record!
  "Read one framed WAL record. Returns `:eof` / `:truncated` / `:corrupt` (as
  `read-data!` classifies the frame) or `[seq entries]` for a complete tx."
  [^BufferedInputStream in-stream]
  (let [r (read-data! in-stream)]
    (cond
      (map? r) (decode-wal-record (:ok r))
      ;; a 0-length frame (:tombstone) is impossible for a real record (the
      ;; payload always has the header) -> treat as corruption
      (= r :tombstone) :corrupt
      :else r)))
