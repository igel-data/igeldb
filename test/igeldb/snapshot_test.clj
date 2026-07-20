(ns igeldb.snapshot-test
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

(defn- config-path!
  [data-dir]
  (rm-rf data-dir)
  (.mkdirs (jio/file data-dir))
  (let [config {:sstable-dir (str data-dir "/sstable")
                :wal-dir (str data-dir "/wal")
                ;; tiny memtable so a few hundred writes trigger many flushes
                :memtable-size 256
                :sync-window-time 10}
        path (str data-dir "/config.yaml")]
    (with-open [w (jio/writer path)]
      (.write w (yaml/generate-string config)))
    path))

;; ---- Step 2: no flush-visibility gap (immutable-memtable consult) ---------

(deftest acknowledged-write-never-vanishes-across-flush-test
  ;; The flush-visibility gap: when `switch-memtable!` installs a fresh empty
  ;; memtable, the switched-out data is momentarily in neither `@memtable` nor a
  ;; committed version. Reads must consult the *immutable memtable* (the one being
  ;; flushed) to bridge that window. Invariant under test: once `write!` returns,
  ;; the key is durably in the memtable, so a concurrent reader must ALWAYS see it
  ;; (with its exact value) -- never nil, never stale -- even while its memtable is
  ;; mid-flush. Without the immutable-memtable consult, a just-flushed key that is
  ;; not yet in any SSTable reads as nil during the switch window.
  (let [data-dir "./test-data/flush-gap"
        kvs (igel/gen-kvs (config-path! data-dir))
        n 4000
        max-written (atom -1) ;; highest index whose write! has returned (acked)
        stop (atom false)
        errors (atom [])]
    (let [readers (doall
                   (for [_ (range 4)]
                     (future
                       (try
                         (while (not @stop)
                           (let [hi @max-written]
                             (when (>= hi 0)
                               ;; bias to the most recent acked keys -- those are
                               ;; the ones still in the (soon-to-be-flushed)
                               ;; memtable, where the gap window lives
                               (let [lo (max 0 (- hi 300))
                                     i (+ lo (rand-int (inc (- hi lo))))
                                     v (igel/select kvs (->bytes (str "k" i)))]
                                 (when-not (and v (b= v (->bytes (str "v" i))))
                                   (throw (ex-info "acked key vanished/stale mid-flush"
                                                   {:key (str "k" i)
                                                    :got (some-> v (String.))})))))))
                         (catch Throwable e (swap! errors conj e))))))]
      (dotimes [i n]
        (igel/write! kvs (->bytes (str "k" i)) (->bytes (str "v" i)))
        (reset! max-written i))
      (reset! stop true)
      (doseq [r readers] @r))
    (is (empty? @errors)
        (str "flush-visibility gap: " (some-> @errors first ex-message)
             " " (some-> @errors first ex-data)))
    (igel/close! kvs)
    (rm-rf data-dir)))
