(ns igeldb.config
  (:require [clojure.java.io :refer [reader]]
            [clj-yaml.core :as yaml]))

(def ^:private ^:const DEFAULT_MEMTABLE_SIZE (* 16 1024 1024))
(def ^:private ^:const DEFAULT_SYNC_WINDOW_TIME 200)
(def ^:private ^:const DEFAULT_GROUP_COMMIT_LIMIT 64)
(def ^:private ^:const DEFAULT_WRITE_RETRIES 10)
(def ^:private ^:const DEFAULT_BLOOM_FILTER {:size 10240})
;; Target bytes per SSTable index block. A point read binary-searches the sparse
;; in-memory index to a block, then linear-scans only within it, so this trades
;; index size against per-read scan work.
(def ^:private ^:const DEFAULT_SSTABLE_BLOCK_SIZE 4096)
(def ^:private ^:const DEFAULT_L0_COMPACTION_TRIGGER 4)
(def ^:private ^:const DEFAULT_L0_STALL_THRESHOLD 16)
(def ^:private ^:const DEFAULT_LEVEL_SIZE_MULTIPLIER 10)
(def ^:private ^:const DEFAULT_MANIFEST_ROTATION_BYTES (* 16 1024 1024))
(def ^:private ^:const DEFAULT_MANIFEST_ROTATION_EDITS 10000)

(defn- read-config
  [config-path]
  (with-open [stream (reader config-path)]
    (yaml/parse-stream stream)))

(defn load-config
  "Load the KVS config from config.yaml"
  [config-path]
  (let [config (read-config config-path)
        default {:memtable-size DEFAULT_MEMTABLE_SIZE
                 :sync-window-time DEFAULT_SYNC_WINDOW_TIME
                 :group-commit-limit DEFAULT_GROUP_COMMIT_LIMIT
                 :write-retries DEFAULT_WRITE_RETRIES
                 :bloom-filter DEFAULT_BLOOM_FILTER
                 :sstable-block-size DEFAULT_SSTABLE_BLOCK_SIZE
                 :l0-compaction-trigger DEFAULT_L0_COMPACTION_TRIGGER
                 :l0-stall-threshold DEFAULT_L0_STALL_THRESHOLD
                 :level-size-multiplier DEFAULT_LEVEL_SIZE_MULTIPLIER
                 :manifest-rotation-bytes DEFAULT_MANIFEST_ROTATION_BYTES
                 :manifest-rotation-edits DEFAULT_MANIFEST_ROTATION_EDITS}
        merged (merge default config)
        ;; Derived defaults: computed from the *merged* values so a user's
        ;; `memtable-size` / `l0-compaction-trigger` overrides flow through,
        ;; but an explicit `sstable-target-size` / `l1-base-size` still wins
        ;; (those keys, when present in `merged`, override the derived ones).
        result (merge {:sstable-target-size (:memtable-size merged)
                       :l1-base-size (* (:memtable-size merged)
                                        (:l0-compaction-trigger merged))}
                      merged)]
    (when (nil? (:sstable-dir result))
      (throw (ex-info "Need to set `sstable-dir` in the config" config)))
    (when (nil? (:wal-dir result))
      (throw (ex-info "Need to set `wal-dir` in the config" config)))
    (doseq [k [:memtable-size :sync-window-time :group-commit-limit
               :l0-compaction-trigger :l0-stall-threshold
               :level-size-multiplier :l1-base-size :sstable-target-size
               :sstable-block-size :manifest-rotation-bytes
               :manifest-rotation-edits]]
      (when-not (pos? (k result))
        (throw (ex-info (str "`" (name k) "` should be positive in the config")
                        result))))
    (when-not (> (:l0-stall-threshold result) (:l0-compaction-trigger result))
      (throw (ex-info
              "`l0-stall-threshold` must be greater than `l0-compaction-trigger`"
              result)))
    result))
