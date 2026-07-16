(ns igel.wal
  (:require [clojure.core.async :as async]
            [clojure.java.io :as java-io]
            [igel.io :as io]
            [igel.store :as store])
  (:import (java.io FileOutputStream BufferedOutputStream)))

(def ^:const ^:private DEFAULT_WINDOW_TIME 200)
(def ^:const ^:private DEFAULT_GROUP_COMMIT_LIMIT 64)

;; Instrumentation: total number of fsyncs performed by the WAL worker across
;; all generations. A single fsync commits a whole batch, so this lets tests
;; verify that group commit actually batches multiple entries per fsync.
(def fsync-count (atom 0))

(defn wal-file-path
  [^long id config]
  (str (:wal-dir config) \/ id ".wal"))

(defn load-existing-wal
  [config]
  (let [wal-file (->> (:wal-dir config)
                      io/list-files
                      (filter #(and (.isFile %)
                                    (.endsWith (.getName %) ".wal")))
                      first)]
    (when-not (nil? wal-file)
      (with-open [in-stream (java-io/input-stream wal-file)]
        (loop [result (transient [])]
          (let [pair (io/read-kv-pair! in-stream)]
            (if (= [nil nil] pair)
              (persistent! result)
              (recur (conj! result pair)))))))))

(defn- entry-size
  "Byte size an entry contributes to the memtable: the key plus, for a live
  value, the value bytes. A tombstone contributes only the key."
  [^bytes k data]
  (+ (count k) (if (:deleted? data) 0 (count (:value data)))))

(defn- commit-batch!
  "fsync the WAL, then apply the batch to the memtable and release each waiting
  writer. Order matters and is intentional:

  1. fsync BEFORE applying, so a failed fsync never leaves a value in the
     memtable (this is what makes rollback unnecessary).
  2. apply in batch order -- the batch is a vector built in the exact order the
     entries arrived on the channel, which is the WAL-append order. A single
     worker thread does the append and the apply, so WAL order == memtable
     order structurally.
  3. deliver :done only AFTER applying, so read-your-writes holds: once a
     writer's `write!` returns, its value is already visible to readers.

  `memstore` is locked only to guard these writes against concurrent reader
  `select`/`scan`; no lock is held across the fsync."
  [^BufferedOutputStream out-stream ^FileOutputStream file-stream batch memstore size]
  (.flush out-stream)
  (-> file-stream .getFD .sync)
  (swap! fsync-count inc)
  (locking memstore
    (doseq [[k data _] batch]
      (store/write-data! memstore k data)
      (swap! size + (entry-size k data))))
  (doseq [[_ _ comp-chan] batch]
    (async/>!! comp-chan :done)))

(defn spawn-wal-writer
  [flush-req-chan flush-wal-chan config]
  (io/make-dir (:wal-dir config))
  (let [sync-window (or (:sync-window-time config) DEFAULT_WINDOW_TIME)
        batch-limit (or (:group-commit-limit config) DEFAULT_GROUP_COMMIT_LIMIT)]
    ;; wait for the first memtable generation
    (let [[wal-index memtable] (async/<!! flush-wal-chan)]
      ;; Start the WAL writer loop
      (async/go-loop [wal-id wal-index
                      memtable memtable]
        (let [data-chan (:wal-chan memtable)
              memstore (:mem memtable)
              size (:size memtable)
              file-stream (FileOutputStream. (wal-file-path wal-id config))
              out-stream (BufferedOutputStream. file-stream 4096)]
          ;; WAL loop until a flush is completed.
          ;;
          ;; `batch` is an ordered vector of [k data comp-chan]; the ordering is
          ;; the source of truth for concurrent writes to the same key.
          ;;
          ;; `window-chan` is an `async/timeout` channel and must be recreated
          ;; every time it fires: a timed-out channel stays closed and taking
          ;; from a closed channel returns nil immediately, so reusing one
          ;; instance would make `alt!` match the window on every iteration --
          ;; group commit would never batch and an empty batch would busy-loop.
          ;;
          ;; Group commit triggers on an OR condition: the time window elapsing
          ;; or the batch reaching `batch-limit` entries.
          (loop [batch []
                 window-chan (async/timeout sync-window)]
            ;; `next` is [next-batch next-window-chan] to keep going, or nil
            ;; once the data channel is closed (a flush was requested).
            (let [next (async/alt!
                         data-chan ([[k d comp-chan]]
                                    (when-not (nil? k)
                                      (io/append-wal! out-stream [k d])
                                      (let [batch (conj batch [k d comp-chan])]
                                        (if (>= (count batch) batch-limit)
                                          (do
                                            (commit-batch! out-stream file-stream
                                                           batch memstore size)
                                            (async/>! flush-req-chan :try-flush)
                                            [[] (async/timeout sync-window)])
                                          [batch window-chan]))))
                         window-chan ([]
                                      (if (seq batch)
                                        (do
                                          (commit-batch! out-stream file-stream
                                                         batch memstore size)
                                          (async/>! flush-req-chan :try-flush)
                                          [[] (async/timeout sync-window)])
                                        [batch (async/timeout sync-window)])))]
              (if (nil? next)
                (do
                  ;; Flush requested: commit whatever is still buffered so that
                  ;; every entry of this WAL generation is applied to the
                  ;; memtable BEFORE the switch, then hand off to the flush
                  ;; writer.
                  (when (seq batch)
                    (commit-batch! out-stream file-stream batch memstore size))
                  (.close out-stream)
                  (.close file-stream)
                  (async/>! flush-req-chan :flush))
                (recur (first next) (second next)))))
          ;; The current WAL loop finished.
          ;; Wait for the new memtable generation from the flush writer.
          (let [[wal-id memtable] (async/<! flush-wal-chan)]
            (recur wal-id memtable)))))))
