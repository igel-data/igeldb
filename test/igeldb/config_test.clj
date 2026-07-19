(ns igeldb.config-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [clj-yaml.core :as yaml]
            [igeldb.config :as config]))

(def ^:private ^:const CONFIG_FILE_PATH "test-data/test-config.yaml")

(def valid-config
  {:sstable-dir "test-data/sstable"
   :wal-dir "test-data/wal"})

(def invalid-config
  {:wal-dir "test-data/wal"
   :memtable-size 1024
   :sync-window-time 200})

(defn- make-config-file
  [config]
  (io/make-parents CONFIG_FILE_PATH)
  (with-open [writer (io/writer CONFIG_FILE_PATH)]
    (.write writer (yaml/generate-string config))))

(deftest load-config-test
  (make-config-file valid-config)
  (let [loaded (config/load-config CONFIG_FILE_PATH)]
    (is (number? (:memtable-size loaded)))
    (is (number? (:sync-window-time loaded)))
    (is (number? (:write-retries loaded)))
    (is (seq (:bloom-filter loaded)))
    ;; Phase 2 level-config defaults
    (is (number? (:l0-compaction-trigger loaded)))
    (is (number? (:l0-stall-threshold loaded)))
    (is (number? (:level-size-multiplier loaded)))
    ;; derived from memtable-size (* l0-compaction-trigger for l1 base)
    (is (= (:memtable-size loaded) (:sstable-target-size loaded)))
    (is (= (* (:memtable-size loaded) (:l0-compaction-trigger loaded))
           (:l1-base-size loaded))))

  (make-config-file invalid-config)
  (is (thrown? clojure.lang.ExceptionInfo
               (config/load-config CONFIG_FILE_PATH)))

  ;; explicit overrides win over the derived defaults
  (make-config-file (assoc valid-config
                           :sstable-target-size 4096
                           :l1-base-size 8192))
  (let [loaded (config/load-config CONFIG_FILE_PATH)]
    (is (= 4096 (:sstable-target-size loaded)))
    (is (= 8192 (:l1-base-size loaded))))

  ;; l0-stall-threshold must exceed l0-compaction-trigger
  (make-config-file (assoc valid-config
                           :l0-compaction-trigger 8
                           :l0-stall-threshold 4))
  (is (thrown? clojure.lang.ExceptionInfo
               (config/load-config CONFIG_FILE_PATH))))
