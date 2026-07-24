(ns igeldb.manifest
  "Versioned, generational manifest for the durable SSTable set.

  `CURRENT` points to one `MANIFEST-<generation>` file. Every manifest is
  independently recoverable: it starts with a fixed versioned header and one
  complete snapshot record, followed by zero or more edit records. Records use
  the same length + CRC frame as the WAL.

  Rotation writes and fsyncs a new snapshot manifest, then atomically replaces
  `CURRENT`. The existing manifest remains authoritative until that replacement,
  so a crash on either side of the switch recovers a complete table set."
  (:require [clojure.java.io :as java-io]
            [clojure.tools.logging :as logging]
            [igeldb.io :as io])
  (:import (java.io BufferedOutputStream ByteArrayInputStream
                    ByteArrayOutputStream DataInputStream DataOutputStream
                    FileOutputStream)
           (java.nio ByteBuffer)
           (java.nio.channels FileChannel)
           (java.nio.file CopyOption Files OpenOption StandardCopyOption
                          StandardOpenOption)))

(def ^:const MANIFEST_FORMAT_VERSION 1)

(def ^:private magic (.getBytes "IGELMANF" "US-ASCII"))
(def ^:private ^:const header-payload-size 20)
(def ^:private ^:const header-size 24)
(def ^:private ^:const snapshot-record 1)
(def ^:private ^:const edit-record 2)
(def ^:private ^:const max-field-bytes (* 64 1024 1024))
(def ^:private ^:const max-table-count 10000000)
(def ^:private manifest-pattern #"MANIFEST-(\d{20})")

(defrecord Manifest [dir config lock state])

(def ^:dynamic *rotation-step-hook*
  "Test hook called with each durable rotation step. Production leaves it nil."
  nil)

(defn- rotation-step!
  [step]
  (when *rotation-step-hook* (*rotation-step-hook* step)))

(defn- corrupt-ex
  ([message path]
   (corrupt-ex message path nil))
  ([message path cause]
   (ex-info message {:igeldb/manifest-corrupt true :path path} cause)))

(defn- encode-header
  [generation]
  (let [payload (doto (ByteBuffer/allocate header-payload-size)
                  (.put ^bytes magic)
                  (.putInt MANIFEST_FORMAT_VERSION)
                  (.putLong generation))
        payload-bytes (.array payload)]
    (-> (doto (ByteBuffer/allocate header-size)
          (.put payload-bytes)
          (.putInt (unchecked-int (io/crc32 payload-bytes))))
        .array)))

(defn- read-header!
  [in path expected-generation]
  (let [bytes (.readNBytes in header-size)]
    (when-not (= header-size (count bytes))
      (throw (corrupt-ex "Manifest header is truncated" path)))
    (let [buf (ByteBuffer/wrap bytes)
          actual-magic (byte-array (count magic))
          _ (.get buf actual-magic)
          version (.getInt buf)
          generation (.getLong buf)
          expected-crc (Integer/toUnsignedLong (.getInt buf))
          payload (byte-array header-payload-size)]
      (System/arraycopy bytes 0 payload 0 header-payload-size)
      (when-not (= (seq magic) (seq actual-magic))
        (throw (corrupt-ex "Manifest magic is invalid" path)))
      (when-not (= MANIFEST_FORMAT_VERSION version)
        (throw (ex-info (str "Unsupported manifest format version: " version)
                        {:igeldb/unsupported-manifest-version true
                         :version version
                         :path path})))
      (when-not (= expected-crc (io/crc32 payload))
        (throw (corrupt-ex "Manifest header CRC mismatch" path)))
      (when-not (= expected-generation generation)
        (throw (corrupt-ex "Manifest generation does not match its filename" path)))
      generation)))

(defn- write-bytes-field!
  [^DataOutputStream out ^bytes b]
  (.writeLong out (count b))
  (.write out b))

(defn- checked-count
  [n label path]
  (when-not (<= 0 n max-table-count)
    (throw (corrupt-ex (str "Invalid manifest " label ": " n) path)))
  (int n))

(defn- read-bytes-field!
  [^DataInputStream in label path]
  (let [n (.readLong in)]
    (when-not (<= 0 n max-field-bytes)
      (throw (corrupt-ex (str "Invalid manifest " label " length: " n) path)))
    (let [buf (byte-array (int n))]
      (.readFully in buf)
      buf)))

(defn- write-table!
  [^DataOutputStream out {:keys [id level size head-key tail-key bloom-filter]}]
  (.writeLong out id)
  (.writeLong out level)
  (.writeLong out size)
  (write-bytes-field! out head-key)
  (write-bytes-field! out tail-key)
  (write-bytes-field! out bloom-filter))

(defn- read-table!
  [^DataInputStream in path]
  {:id (.readLong in)
   :level (.readLong in)
   :size (.readLong in)
   :head-key (read-bytes-field! in "head-key" path)
   :tail-key (read-bytes-field! in "tail-key" path)
   :bloom-filter (read-bytes-field! in "bloom-filter" path)})

(defn- payload-bytes
  [write!]
  (let [baos (ByteArrayOutputStream.)
        out (DataOutputStream. baos)]
    (write! out)
    (.flush out)
    (.toByteArray baos)))

(defn- encode-snapshot
  [{:keys [tables next-table-id max-seq]}]
  (payload-bytes
   (fn [^DataOutputStream out]
     (.writeByte out snapshot-record)
     (.writeLong out next-table-id)
     (.writeLong out max-seq)
     (.writeLong out (count tables))
     (doseq [table tables] (write-table! out table)))))

(defn- encode-edit
  [{:keys [added deleted max-seq]}]
  (payload-bytes
   (fn [^DataOutputStream out]
     (.writeByte out edit-record)
     (.writeLong out (count added))
     (doseq [table added] (write-table! out table))
     (.writeLong out (count deleted))
     (doseq [id deleted] (.writeLong out id))
     (.writeLong out (or max-seq 0)))))

(defn- decode-record
  [^bytes bytes path]
  (try
    (let [in (DataInputStream. (ByteArrayInputStream. bytes))
          type (.readUnsignedByte in)
          record
          (cond
            (= type snapshot-record)
            (let [next-table-id (.readLong in)
                  max-seq (.readLong in)
                  n (checked-count (.readLong in) "snapshot table count" path)]
              {:type :snapshot
               :next-table-id next-table-id
               :max-seq max-seq
               :tables (mapv (fn [_] (read-table! in path)) (range n))})

            (= type edit-record)
            (let [added-count (checked-count (.readLong in) "added table count" path)
                  added (mapv (fn [_] (read-table! in path)) (range added-count))
                  deleted-count (checked-count (.readLong in)
                                               "deleted table count" path)
                  deleted (mapv (fn [_] (.readLong in)) (range deleted-count))]
              {:type :edit
               :edit {:added added
                      :deleted deleted
                      :max-seq (.readLong in)}})

            :else
            (throw (corrupt-ex (str "Unknown manifest record type: " type) path)))]
      (when-not (zero? (.available in))
        (throw (corrupt-ex "Manifest record has trailing bytes" path)))
      record)
    (catch clojure.lang.ExceptionInfo e
      (throw e))
    (catch Throwable e
      (throw (corrupt-ex "Manifest record is malformed" path e)))))

(defn- manifest-name
  [generation]
  (format "MANIFEST-%020d" (long generation)))

(defn- generation-from-name
  [name]
  (when-let [[_ digits] (re-matches manifest-pattern name)]
    (Long/parseLong digits)))

(defn- current-path
  [dir]
  (str dir "/CURRENT"))

(defn- current-temp-path
  [dir]
  (str dir "/CURRENT.tmp"))

(defn- manifest-file-path
  [dir generation]
  (str dir "/" (manifest-name generation)))

(defn- sync-directory!
  [dir]
  (with-open [channel (FileChannel/open
                       (.toPath (java-io/file dir))
                       (into-array OpenOption [StandardOpenOption/READ]))]
    (.force channel true)))

(defn- close-writer!
  [{:keys [out-stream file-stream]}]
  (when out-stream
    (try (.close ^BufferedOutputStream out-stream) (catch Throwable _)))
  (when file-stream
    (try (.close ^FileOutputStream file-stream) (catch Throwable _))))

(defn- next-generation
  [dir]
  (let [generations (keep #(generation-from-name (.getName %))
                          (or (.listFiles (java-io/file dir)) []))]
    (inc (reduce max 0 generations))))

(defn- write-current!
  [dir name activated?]
  (let [tmp (current-temp-path dir)
        current (current-path dir)]
    (try
      (with-open [file-stream (FileOutputStream. tmp false)
                  out-stream (BufferedOutputStream. file-stream)]
        (io/write-bytes! out-stream (.getBytes ^String name "UTF-8"))
        (.flush out-stream)
        (-> file-stream .getChannel (.force true)))
      (rotation-step! :current-temp-fsynced)
      (Files/move (.toPath (java-io/file tmp))
                  (.toPath (java-io/file current))
                  (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE
                                          StandardCopyOption/REPLACE_EXISTING]))
      (reset! activated? true)
      (rotation-step! :current-replaced)
      (sync-directory! dir)
      (rotation-step! :current-directory-fsynced)
      (catch Throwable e
        (when (.exists (java-io/file tmp))
          (.delete (java-io/file tmp)))
        (throw e)))))

(defn- read-current-name
  [dir]
  (let [path (current-path dir)]
    (with-open [in (java-io/input-stream path)]
      (let [record (io/read-data! in)
            tail (io/read-data! in)]
        (when-not (and (map? record) (= :eof tail))
          (throw (ex-info "CURRENT is truncated or corrupted"
                          {:igeldb/current-corrupt true :path path})))
        (let [name (String. ^bytes (:ok record) "UTF-8")]
          (when-not (generation-from-name name)
            (throw (ex-info "CURRENT contains an invalid manifest name"
                            {:igeldb/current-corrupt true
                             :path path
                             :manifest name})))
          name)))))

(defn- read-manifest-file
  [path generation]
  (with-open [in (java-io/input-stream path)]
    (read-header! in path generation)
    (let [first-record (io/read-data! in)]
      (when-not (map? first-record)
        (throw (corrupt-ex "Manifest has no complete snapshot record" path)))
      (let [snapshot (decode-record (:ok first-record) path)]
        (when-not (= :snapshot (:type snapshot))
          (throw (corrupt-ex "Manifest first record is not a snapshot" path)))
        (loop [edits []
               valid-length (+ header-size io/FRAME_OVERHEAD
                               (count ^bytes (:ok first-record)))
               next-table-id (:next-table-id snapshot)
               max-seq (:max-seq snapshot)]
          (let [record (io/read-data! in)]
            (cond
              (= :eof record)
              {:generation generation
               :tables (:tables snapshot)
               :edits edits
               :next-table-id next-table-id
               :max-seq max-seq
               :valid-length valid-length
               :delta-bytes (- valid-length
                               header-size io/FRAME_OVERHEAD
                               (count ^bytes (:ok first-record)))
               :edit-count (count edits)}

              (= :truncated record)
              (do
                (logging/warn "Discarding an incomplete tail edit in" path)
                {:generation generation
                 :tables (:tables snapshot)
                 :edits edits
                 :next-table-id next-table-id
                 :max-seq max-seq
                 :valid-length valid-length
                 :delta-bytes (- valid-length
                                 header-size io/FRAME_OVERHEAD
                                 (count ^bytes (:ok first-record)))
                 :edit-count (count edits)})

              (map? record)
              (let [decoded (decode-record (:ok record) path)]
                (when-not (= :edit (:type decoded))
                  (throw (corrupt-ex "Unexpected snapshot after manifest start" path)))
                (let [edit (:edit decoded)
                      next-id (reduce (fn [n table]
                                        (max n (inc (:id table))))
                                      next-table-id
                                      (:added edit))]
                  (recur (conj edits edit)
                         (+ valid-length io/FRAME_OVERHEAD
                            (count ^bytes (:ok record)))
                         next-id
                         (max max-seq (or (:max-seq edit) 0)))))

              :else
              (throw (corrupt-ex "Manifest record CRC mismatch" path)))))))))

(defn- open-append-state
  [path recovery]
  (with-open [channel (FileChannel/open
                       (.toPath (java-io/file path))
                       (into-array OpenOption [StandardOpenOption/WRITE]))]
    (.truncate channel (:valid-length recovery)))
  (let [file-stream (FileOutputStream. path true)]
    {:generation (:generation recovery)
     :path path
     :file-stream file-stream
     :out-stream (BufferedOutputStream. file-stream)
     :delta-bytes (:delta-bytes recovery)
     :edit-count (:edit-count recovery)
     :next-table-id (:next-table-id recovery)
     :max-seq (:max-seq recovery)}))

(defn- write-new-manifest!
  [dir generation snapshot]
  (let [path (manifest-file-path dir generation)
        file-stream (FileOutputStream. path false)
        out-stream (BufferedOutputStream. file-stream)]
    (try
      (.write out-stream (encode-header generation))
      (io/write-bytes! out-stream (encode-snapshot snapshot))
      (.flush out-stream)
      (-> file-stream .getChannel (.force true))
      {:generation generation
       :path path
       :file-stream file-stream
       :out-stream out-stream
       :delta-bytes 0
       :edit-count 0
       :next-table-id (:next-table-id snapshot)
       :max-seq (:max-seq snapshot)}
      (catch Throwable e
        (close-writer! {:out-stream out-stream :file-stream file-stream})
        (throw e)))))

(defn- create-initial!
  [dir]
  (let [generation (next-generation dir)
        state (write-new-manifest! dir generation
                                   {:tables [] :next-table-id 0 :max-seq 0})
        activated? (atom false)]
    (try
      (sync-directory! dir)
      (write-current! dir (manifest-name generation) activated?)
      [state {:generation generation
              :tables []
              :edits []
              :next-table-id 0
              :max-seq 0
              :valid-length (.length (java-io/file (:path state)))
              :delta-bytes 0
              :edit-count 0}]
      (catch Throwable e
        (close-writer! state)
        (throw e)))))

(defn open-manifest
  "Open the current manifest and return `[manifest recovery]`.

  `recovery` contains the base snapshot, following edits, and the persisted
  sequence/table-id high-water marks. A torn final edit is removed before the
  append stream is opened."
  [config]
  (let [dir (:sstable-dir config)
        _ (io/make-dir dir)
        current (java-io/file (current-path dir))
        legacy (java-io/file (str dir "/MANIFEST"))
        [state recovery]
        (cond
          (.exists current)
          (let [name (read-current-name dir)
                generation (generation-from-name name)
                path (str dir "/" name)
                recovery (read-manifest-file path generation)]
            [(open-append-state path recovery) recovery])

          (.exists legacy)
          (throw (ex-info "The unversioned manifest format is unsupported"
                          {:igeldb/unsupported-manifest-version true
                           :version 0
                           :path (.getPath legacy)}))

          :else
          (create-initial! dir))]
    [(->Manifest dir config (Object.) (atom state)) recovery]))

(defn current-manifest-path
  "Resolve the manifest named by CURRENT."
  [config]
  (str (:sstable-dir config) "/" (read-current-name (:sstable-dir config))))

(defn read-current-manifest
  "Read the CURRENT manifest without opening it for append. Intended for
  diagnostics and tests."
  [config]
  (let [name (read-current-name (:sstable-dir config))
        generation (generation-from-name name)]
    (read-manifest-file (str (:sstable-dir config) "/" name) generation)))

(defn append-edit!
  "Append and fsync one edit. The caller must hold `(:lock manifest)`."
  [manifest edit]
  (let [payload (encode-edit edit)
        {:keys [^BufferedOutputStream out-stream
                ^FileOutputStream file-stream]} @(:state manifest)]
    (io/write-bytes! out-stream payload)
    (.flush out-stream)
    (-> file-stream .getChannel (.force true))
    (swap! (:state manifest)
           (fn [state]
             (-> state
                 (update :delta-bytes + io/FRAME_OVERHEAD (count payload))
                 (update :edit-count inc)
                 (update :next-table-id
                         (fn [next-id]
                           (reduce (fn [n table] (max n (inc (:id table))))
                                   next-id
                                   (:added edit))))
                 (update :max-seq max (or (:max-seq edit) 0)))))))

(defn- cleanup-old-manifests!
  [dir current-generation]
  (let [deleted? (atom false)]
    (doseq [file (or (.listFiles (java-io/file dir)) [])
            :let [generation (generation-from-name (.getName file))]
            :when (and generation (not= generation current-generation))]
      (if (.delete file)
        (reset! deleted? true)
        (logging/warn "Could not delete obsolete manifest" (.getPath file))))
    (when @deleted?
      (try
        (sync-directory! dir)
        (catch Throwable e
          (logging/warn e "Could not fsync manifest cleanup"))))))

(defn maybe-rotate!
  "Rotate to a complete snapshot when the configured delta threshold is reached.

  Failures before CURRENT replacement are non-fatal: the old manifest already
  contains the committed edit and remains active. A failure after replacement is
  fatal because continuing could append to a manifest other than CURRENT."
  [manifest tables]
  (let [{:keys [manifest-rotation-bytes manifest-rotation-edits]} (:config manifest)
        old-state @(:state manifest)]
    (when (or (>= (:delta-bytes old-state) manifest-rotation-bytes)
              (>= (:edit-count old-state) manifest-rotation-edits))
      (let [generation (next-generation (:dir manifest))
            snapshot {:tables tables
                      :next-table-id (:next-table-id old-state)
                      :max-seq (:max-seq old-state)}
            new-state* (atom nil)
            activated? (atom false)]
        (try
          (let [new-state (write-new-manifest! (:dir manifest) generation snapshot)]
            (reset! new-state* new-state)
            (sync-directory! (:dir manifest))
            (rotation-step! :new-manifest-fsynced)
            (write-current! (:dir manifest) (manifest-name generation) activated?)
            (reset! (:state manifest) new-state)
            (rotation-step! :writer-switched)
            (close-writer! old-state)
            (rotation-step! :old-writer-closed)
            (cleanup-old-manifests! (:dir manifest) generation)
            (rotation-step! :old-manifests-deleted)
            true)
          (catch Throwable e
            (if @activated?
              (do
                ;; CURRENT names the new generation. Make the in-memory owner match
                ;; before propagating so shutdown closes the authoritative writer.
                (when-let [new-state @new-state*]
                  (reset! (:state manifest) new-state))
                (close-writer! old-state)
                (throw (ex-info "Manifest rotation failed after activation"
                                {:igeldb/manifest-rotation-fatal true
                                 :generation generation}
                                e)))
              (do
                (when-let [new-state @new-state*] (close-writer! new-state))
                (logging/warn e "Manifest rotation deferred; old manifest remains active")
                false))))))))

(defn close!
  "Close the current manifest append stream."
  [manifest]
  (close-writer! @(:state manifest)))
