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

  Encoding: each edit is Fressian-serialized to bytes, then framed with the Phase
  1 length + CRC envelope (`io/write-bytes!` / `io/read-data!`), so the Phase 1
  corruption classes apply directly.

  In an edit passed to/from this namespace, `:bloom-filter` is the *serialized
  bytes* of the filter (Fressian cannot serialize the filter object); callers
  deserialize on replay.

  TODO (manifest rotation, deferred): the manifest grows unbounded. Rotation will
  snapshot the full current table set into a fresh manifest and atomically swap it
  in (LevelDB's CURRENT/MANIFEST swap), then discard the old manifest."
  (:require [clojure.data.fressian :as fress]
            [clojure.java.io :as java-io]
            [clojure.tools.logging :as logging]
            [igeldb.io :as io])
  (:import (java.io FileOutputStream BufferedOutputStream)))

(defn manifest-path
  "Path of the single append-only manifest file (lives beside the SSTables)."
  [config]
  (str (:sstable-dir config) "/MANIFEST"))

(defn- edit->bytes
  [edit]
  (let [bb (fress/write edit)
        arr (byte-array (.remaining bb))]
    (.get bb arr)
    arr))

(defn- bytes->edit
  [^bytes b]
  (fress/read b))

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
  (-> ^FileOutputStream (:file-stream manifest) .getFD .sync))

(defn close!
  "Close the manifest's append stream."
  [manifest]
  (.close ^BufferedOutputStream (:out-stream manifest))
  (.close ^FileOutputStream (:file-stream manifest)))
