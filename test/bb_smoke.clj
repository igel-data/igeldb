#!/usr/bin/env bb
;; Babashka smoke test: proves IgelDB's namespaces load under SCI and the durable
;; path works on bb -- write -> flush -> select -> scan -> compaction ->
;; reopen(recovery) -> verify. It does NOT re-cover logic the JVM clojure.test
;; suite owns. Run from the repo root:
;;
;;   bb test/bb_smoke.clj
;;
;; (bb.edn supplies :paths ["src"] and the runtime deps.) Exits non-zero on any
;; failed check.

(require '[igeldb.core :as igel]
         '[igeldb.data :as data]
         '[clj-yaml.core :as yaml]
         '[clojure.java.io :as jio])

(def data-dir "./bb-smoke-data")
(def n 400)

(defn- ->bytes [^String s] (.getBytes s))
(defn- v= [expected actual] (data/byte-array-equals? (->bytes expected) actual))

(defn- delete-recursively! [f]
  (when (.isDirectory f)
    (doseq [c (.listFiles f)] (delete-recursively! c)))
  (.delete f))

(defn- rm-rf [dir] (delete-recursively! (jio/file dir)))

(defn- write-config! []
  (rm-rf data-dir)
  (.mkdirs (jio/file data-dir))
  (let [config {:sstable-dir (str data-dir "/sstable")
                :wal-dir (str data-dir "/wal")
                ;; small memtable so a few hundred writes flush + compact
                :memtable-size 256
                :sync-window-time 10}
        path (str data-dir "/config.yaml")]
    (spit path (yaml/generate-string config))
    path))

(def failures (atom 0))

(defn check [label ok?]
  (if ok?
    (println "   ok  -" label)
    (do (println "  FAIL -" label) (swap! failures inc))))

(defn- l1+-populated? [kvs]
  (some seq (rest @(:current-version (:tree kvs)))))

(defn- wait-l1 [kvs]
  (loop [tries 200]
    (when (and (pos? tries) (not (l1+-populated? kvs)))
      (Thread/sleep 50)
      (recur (dec tries)))))

(defn- all-readable? [kvs]
  (every? (fn [i] (v= (str "v" i) (igel/select kvs (->bytes (format "k%05d" i)))))
          (range n)))

(defn run []
  (let [config-path (write-config!)]
    (println (str "1. create store; write " n " keys (triggers flush + compaction)"))
    (let [kvs (igel/gen-kvs config-path)]
      (dotimes [i n]
        (igel/write! kvs (->bytes (format "k%05d" i)) (->bytes (str "v" i))))
      (println "2. select every key back (bloom + SSTable read)")
      (check "all keys readable" (all-readable? kvs))
      (println "3. scan a range")
      (check "scan [k00010,k00020) returns 10 pairs"
             (= 10 (count (igel/scan kvs (->bytes "k00010") (->bytes "k00020")))))
      (println "4. a compaction ran")
      (wait-l1 kvs)
      (check "L1+ populated by compaction" (boolean (l1+-populated? kvs)))
      (igel/close! kvs))
    (println "5. reopen from the same dir (manifest replay + WAL recovery)")
    (let [kvs (igel/gen-kvs config-path)]
      (check "all keys intact after reopen" (all-readable? kvs))
      (igel/close! kvs))))

(try
  (run)
  (rm-rf data-dir)
  (if (zero? @failures)
    (do (println "\nbb smoke test PASSED") (System/exit 0))
    (do (println (str "\nbb smoke test FAILED (" @failures " checks)"))
        (System/exit 1)))
  (catch Throwable e
    (rm-rf data-dir)
    (println "\nbb smoke test ERROR:" (str e))
    (System/exit 1)))
