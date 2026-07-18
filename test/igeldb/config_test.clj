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
    (is (seq (:bloom-filter loaded))))

  (make-config-file invalid-config)
  (is (thrown? clojure.lang.ExceptionInfo
               (config/load-config CONFIG_FILE_PATH))))
