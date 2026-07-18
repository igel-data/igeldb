(ns igeldb.config
  (:require [clojure.java.io :refer [reader]]
            [clj-yaml.core :as yaml]))

(def ^:private ^:const DEFAULT_MEMTABLE_SIZE 1024)
(def ^:private ^:const DEFAULT_SYNC_WINDOW_TIME 200)
(def ^:private ^:const DEFAULT_GROUP_COMMIT_LIMIT 64)
(def ^:private ^:const DEFAULT_WRITE_RETRIES 10)
(def ^:private ^:const DEFAULT_BLOOM_FILTER {:size 10240})

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
                 :bloom-filter DEFAULT_BLOOM_FILTER}
        result (merge default config)]
    (when (nil? (:sstable-dir result))
      (throw (ex-info "Need to set `sstable-dir` in the config" config)))
    (when (nil? (:wal-dir result))
      (throw (ex-info "Need to set `wal-dir` in the config" config)))
    (when (not (pos? (:memtable-size result)))
      (throw (ex-info
              "`memtable-size` should be positive in the config"
              config)))
    (when (not (pos? (:sync-window-time result)))
      (throw (ex-info
              "`sync-window-time` should be positive in the config"
              result)))
    (when-not (pos? (:group-commit-limit result))
      (throw (ex-info
              "`group-commit-limit` should be positive in the config"
              result)))
    result))
