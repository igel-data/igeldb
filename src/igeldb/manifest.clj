(ns igeldb.manifest
  "Append-only manifest: the durable, crash-safe record of the current SSTable
  set. Each committed change (a flush or a compaction) is one *version edit*
  `{:added [{:id :level :head-key :tail-key :bloom-filter :size} ...] :deleted [id ...]}`.

  An edit is committed the moment it is appended and fsynced -- that fsync is the
  atomicity boundary of a table-set change. On startup the whole manifest is
  replayed to rebuild the table set; SSTable files not referenced by the replayed
  state are ignored (a delete lost to a crash is harmless).

  The manifest tracks only SSTable state. The WAL ID is recovered separately from
  the WAL directory (see `igeldb.wal/load-existing-wal`).

  Encoding: each edit is encoded to bytes with the project's own explicit binary
  format (see `edit->bytes`), then framed with the Phase 1 length + CRC envelope
  (`io/write-bytes!` / `io/read-data!`), so the Phase 1 corruption classes apply
  directly. There is no per-field CRC here -- the outer frame CRCs the whole
  edit, so `bytes->edit` only ever runs on already-verified bytes.

  The self-made format keeps IgelDB pure-Clojure/Babashka-compatible: it uses
  only `DataOutputStream`/`DataInputStream` (bb-safe), unlike Fressian.

  In an edit passed to/from this namespace, `:bloom-filter` is the *serialized
  bytes* of the filter (byte[]), `:head-key`/`:tail-key` are byte[] keys, and
  `:id`/`:level`/`:size` are longs.

  TODO (manifest rotation, deferred): the manifest grows unbounded. Rotation will
  snapshot the full current table set into a fresh manifest and atomically swap it
  in (LevelDB's CURRENT/MANIFEST swap), then discard the old manifest."
  (:require [clojure.java.io :as java-io]
            [clojure.tools.logging :as logging]
            [igeldb.io :as io])
  (:import (java.io FileOutputStream BufferedOutputStream
                    ByteArrayOutputStream ByteArrayInputStream
                    DataOutputStream DataInputStream)))

(defn manifest-path
  "Path of the single append-only manifest file (lives beside the SSTables)."
  [config]
  (str (:sstable-dir config) "/MANIFEST"))

;; Binary layout of one edit (all longs are 8-byte big-endian, matching
;; `io/serialize-long`; each byte[] is a long length prefix then the raw bytes):
;;   long   addedCount
;;   repeat addedCount: long id, long level, long size,
;;                      long headLen, bytes head, long tailLen, bytes tail,
;;                      long bloomLen, bytes bloom
;;   long   deletedCount
;;   repeat deletedCount: long id
;;   long   maxSeq        ;; highest InternalKey seq committed by this edit (Point 1
;;                        ;; recovery: next-seq = max(manifest maxSeq, WAL maxSeq)+1)

(defn- write-bytes-field!
  [^DataOutputStream out ^bytes b]
  (.writeLong out (count b))
  (.write out b))

(defn- read-bytes-field!
  [^DataInputStream in]
  (let [buf (byte-array (.readLong in))]
    (.readFully in buf)
    buf))

(defn- edit->bytes
  [{:keys [added deleted max-seq]}]
  (let [baos (ByteArrayOutputStream.)
        out (DataOutputStream. baos)]
    (.writeLong out (count added))
    (doseq [{:keys [id level size head-key tail-key bloom-filter]} added]
      (.writeLong out id)
      (.writeLong out level)
      (.writeLong out size)
      (write-bytes-field! out head-key)
      (write-bytes-field! out tail-key)
      (write-bytes-field! out bloom-filter))
    (.writeLong out (count deleted))
    (doseq [id deleted]
      (.writeLong out id))
    (.writeLong out (or max-seq 0))
    (.flush out)
    (.toByteArray baos)))

(defn- bytes->edit
  [^bytes b]
  (let [in (DataInputStream. (ByteArrayInputStream. b))
        added (mapv (fn [_]
                      ;; explicit let so the field reads happen in order
                      (let [id (.readLong in)
                            level (.readLong in)
                            size (.readLong in)
                            head-key (read-bytes-field! in)
                            tail-key (read-bytes-field! in)
                            bloom-filter (read-bytes-field! in)]
                        {:id id :level level :size size
                         :head-key head-key :tail-key tail-key
                         :bloom-filter bloom-filter}))
                    (range (.readLong in)))
        deleted (mapv (fn [_] (.readLong in))
                      (range (.readLong in)))
        max-seq (.readLong in)]
    {:added added :deleted deleted :max-seq max-seq}))

(defn read-edits
  "Replay every committed edit from the manifest, in append order. Reuses the
  Phase 1 corruption classes: `:eof` ends cleanly; a `:truncated` tail record is
  tolerated (the last edit may not have been fully fsynced before a crash) and
  ends replay; a mid-file `:corrupt` (CRC mismatch) raises. Returns [] when no
  manifest exists yet."
  [path]
  (let [f (java-io/file path)]
    (if-not (.exists f)
      []
      (with-open [in-stream (java-io/input-stream f)]
        (loop [edits (transient [])]
          (let [r (io/read-data! in-stream)]
            (cond
              (= :eof r) (persistent! edits)
              (= :truncated r)
              (do (logging/warn "Discarding an incomplete tail edit in MANIFEST")
                  (persistent! edits))
              (map? r) (recur (conj! edits (bytes->edit (:ok r))))
              ;; :corrupt (or an unexpected :tombstone) -- real corruption
              :else (throw (ex-info "MANIFEST is corrupted (CRC mismatch)"
                                    {:path path :reason r})))))))))

(defrecord Manifest [path file-stream out-stream lock])

(defn open-manifest
  "Open the manifest for appending (creating it if absent). Call after
  `read-edits` has replayed any existing edits."
  [config]
  (let [path (manifest-path config)
        file-stream (FileOutputStream. path true)
        out-stream (BufferedOutputStream. file-stream)]
    (->Manifest path file-stream out-stream (Object.))))

(defn append-edit!
  "Append one edit and fsync -- the commit point. The caller must hold
  `(:lock manifest)` to serialize commits. Throws on any IO failure; the caller
  applies fail-stop (poison)."
  [manifest edit]
  (io/write-bytes! (:out-stream manifest) (edit->bytes edit))
  (.flush ^BufferedOutputStream (:out-stream manifest))
  (-> ^FileOutputStream (:file-stream manifest) .getChannel (.force true)))

(defn close!
  "Close the manifest's append stream."
  [manifest]
  (.close ^BufferedOutputStream (:out-stream manifest))
  (.close ^FileOutputStream (:file-stream manifest)))
