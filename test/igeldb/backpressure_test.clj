(ns igeldb.backpressure-test
  (:require [clojure.java.io :as jio]
            [clojure.test :refer [deftest is]]
            [clj-yaml.core :as yaml]
            [igeldb.core :as igel]
            [igeldb.data :as data]))

(defn- ->bytes [^String s] (.getBytes s))
(defn- b= [a b] (data/byte-array-equals? a b))

(defn- delete-recursively!
  [file]
  (when (.isDirectory file)
    (doseq [c (.listFiles file)] (delete-recursively! c)))
  (.delete file))

(defn- rm-rf [dir] (delete-recursively! (jio/file dir)))

;; `await-l0-capacity!` is private; call it through its var for these unit tests.
(def ^:private await-l0-capacity! #'igeldb.core/await-l0-capacity!)

(defn- saturated-version
  "A fake version whose L0 holds `n` (contentless) tables."
  [n]
  (atom [(vec (repeat n [0 {}]))]))

(defn- install-poison-watch!
  "Replicate the watch `spawn-bg-workers` installs: waking stalled writers when
  the store is poisoned."
  [poison monitor]
  (add-watch poison ::wake
             (fn [_ _ old new]
               (when (and (nil? old) new)
                 (locking monitor (.notifyAll monitor))))))

;; ---- Step 4-2: a poisoned store wakes a stalled writer -------------------

(deftest stall-wakes-on-poison-test
  (let [monitor (Object.)
        current-version (saturated-version 16)
        poison (atom nil)]
    (install-poison-watch! poison monitor)
    (let [started (promise)
          result (future
                   (deliver started true)
                   (await-l0-capacity! monitor current-version poison 16)
                   :returned)]
      @started
      (Thread/sleep 150)
      (is (not (realized? result)) "the writer stalls while L0 is saturated")
      (reset! poison (ex-info "boom" {}))
      (is (= :returned (deref result 2000 :timed-out))
          "poisoning the store wakes the stalled writer (no deadlock)"))))

;; ---- Step 4-1: draining L0 releases a stalled writer ---------------------

(deftest stall-releases-when-l0-drains-test
  (let [monitor (Object.)
        current-version (saturated-version 16)
        poison (atom nil)
        result (future
                 (await-l0-capacity! monitor current-version poison 16)
                 :returned)]
    (Thread/sleep 150)
    (is (not (realized? result)) "stalled while L0 >= threshold")
    ;; a compaction drops L0 below the threshold and notifies
    (reset! current-version [[]])
    (locking monitor (.notifyAll monitor))
    (is (= :returned (deref result 2000 :timed-out))
        "a compaction that drains L0 releases the writer")))

(deftest no-stall-when-l0-under-threshold-test
  (let [monitor (Object.)
        current-version (saturated-version 2)
        poison (atom nil)]
    (is (= :returned
           (deref (future (await-l0-capacity! monitor current-version poison 16)
                          :returned)
                  1000 :timed-out))
        "an under-threshold L0 never blocks the writer")))

;; ---- Step 4: concurrent writers make progress under back-pressure --------

(deftest concurrent-writers-under-backpressure-test
  (let [data-dir "./test-data/backpressure"
        config {:sstable-dir (str data-dir "/sstable")
                :wal-dir (str data-dir "/wal")
                :memtable-size 256
                :sync-window-time 10
                ;; low, tight thresholds so bursts actually hit the stall gate
                :l0-compaction-trigger 2
                :l0-stall-threshold 3}
        _ (do (rm-rf data-dir) (.mkdirs (jio/file data-dir)))
        config-path (str data-dir "/config.yaml")
        _ (spit config-path (yaml/generate-string config))
        kvs (igel/gen-kvs config-path)
        n-threads 6
        per-thread 120]
    ;; Sustained concurrent load; if the stall gate deadlocked, these never join.
    (let [writers (doall
                   (for [t (range n-threads)]
                     (future
                       (dotimes [i per-thread]
                         (igel/write! kvs
                                      (->bytes (format "t%d-k%04d" t i))
                                      (->bytes (str i)))))))]
      (doseq [w writers] @w))
    (doseq [t (range n-threads)
            i (range per-thread)]
      (is (b= (->bytes (str i))
              (igel/select kvs (->bytes (format "t%d-k%04d" t i))))
          (str "lost write t" t "-k" i)))
    (.finalize kvs)
    (rm-rf data-dir)))
