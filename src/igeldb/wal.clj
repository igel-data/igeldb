(ns igeldb.wal
  (:require [clojure.core.async :as async]
            [clojure.java.io :as java-io]
            [clojure.tools.logging :as logging]
            [igeldb.io :as io]
            [igeldb.store :as store])
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

(defn- wal-file-id
  "The numeric ID encoded in a `<id>.wal` filename."
  [file]
  (Long/parseLong (re-find #"\d+" (.getName file))))

(defn- replay-wal
  "Replay one WAL file into a vector of [k data] entries."
  [wal-file]
  (with-open [in-stream (java-io/input-stream wal-file)]
    (loop [result (transient [])]
      (let [entry (io/read-kv-pair! in-stream)]
        (case entry
          ;; clean end of the log
          :eof (persistent! result)
          ;; An incomplete entry at the tail is expected: the process died
          ;; before this write's fsync completed. Keep everything before it.
          :truncated
          (do
            (logging/warn "Discarding an incomplete tail entry in WAL"
                          (.getName wal-file))
            (persistent! result))
          ;; A CRC mismatch mid-file is real corruption, not a torn tail.
          :corrupt
          (throw (ex-info "WAL is corrupted (CRC mismatch)"
                          {:file (.getName wal-file)}))
          ;; entry is [k data]
          (recur (conj! result entry)))))))

(defn load-existing-wal
  "Return `[wal-id entries]` for crash recovery.

  Crash-recovery invariant: every WAL other than the highest-ID one has already
  been committed to the manifest (a flush appends+fsyncs its manifest edit --
  the commit point -- before deleting the WAL). So multiple WALs remaining is
  the *normal* result of crashing after a flush's manifest commit but before the
  WAL delete -- it must not be treated as an error. We replay only the
  highest-ID WAL and discard the stale ones.

  When no WAL exists, the WAL ID starts at 0 with no entries."
  [config]
  (let [wal-files (->> (:wal-dir config)
                       io/list-files
                       (filter #(and (.isFile %)
                                     (.endsWith (.getName %) ".wal")))
                       (sort-by wal-file-id))]
    (if (empty? wal-files)
      [0 []]
      (let [newest (last wal-files)
            stale (butlast wal-files)]
        (when (seq stale)
          (logging/info (str "Restoring from WAL " (wal-file-id newest)
                             ", discarding stale WALs "
                             (mapv wal-file-id stale)))
          (doseq [f stale] (io/delete-file f)))
        [(wal-file-id newest) (replay-wal newest)]))))

(defn entry-size
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

  The batch is applied as one atomic snapshot publish (`write-batch!`), so a
  lock-free reader sees the pre-batch or post-batch memtable, never a torn
  mid-batch view. This worker is the sole applier, preserving WAL-append order ==
  memtable-apply order."
  [^BufferedOutputStream out-stream ^FileOutputStream file-stream batch memstore size]
  (try
    (.flush out-stream)
    (-> file-stream .getChannel (.force true))
    (swap! fsync-count inc)
    (catch Throwable e
      ;; fsync/flush failed. Nothing was applied to the memtable yet, so there
      ;; is nothing to roll back -- but the waiting writers must not be left to
      ;; block forever. Release them with an error and propagate so the worker
      ;; can poison the store (fail-stop: never swallow an fsync failure).
      (doseq [[_ _ comp-chan] batch]
        (async/>!! comp-chan :error))
      (throw e)))
  (store/write-batch! memstore (mapv (fn [[k data _]] [k data]) batch))
  (swap! size + (transduce (map (fn [[k data _]] (entry-size k data))) + 0 batch))
  (doseq [[_ _ comp-chan] batch]
    (async/>!! comp-chan :done)))

(defn spawn-wal-writer
  [poison flush-req-chan flush-wal-chan config]
  (io/make-dir (:wal-dir config))
  (let [sync-window (or (:sync-window-time config) DEFAULT_WINDOW_TIME)
        batch-limit (or (:group-commit-limit config) DEFAULT_GROUP_COMMIT_LIMIT)
        ;; wait for the first memtable generation
        [wal-index memtable] (async/<!! flush-wal-chan)]
    ;; Start the WAL writer loop
    (async/go-loop [wal-id wal-index
                    memtable memtable]
      (let [data-chan (:wal-chan memtable)
            memstore (:mem memtable)
            size (:size memtable)
            file-stream (FileOutputStream. (wal-file-path wal-id config))
            out-stream (BufferedOutputStream. file-stream 4096)
              ;; `healthy?` is true when this generation ended with a normal
              ;; flush hand-off, false when an IO error poisoned the store.
            healthy?
            (try
                ;; WAL loop until a flush is completed.
                ;;
                ;; `batch` is an ordered vector of [k data comp-chan]; the
                ;; ordering is the source of truth for concurrent writes to the
                ;; same key.
                ;;
                ;; `window-chan` is an `async/timeout` channel and must be
                ;; recreated every time it fires: a timed-out channel stays
                ;; closed and taking from a closed channel returns nil
                ;; immediately, so reusing one instance would make `alt!` match
                ;; the window on every iteration -- group commit would never
                ;; batch and an empty batch would busy-loop.
                ;;
                ;; Group commit triggers on an OR condition: the time window
                ;; elapsing or the batch reaching `batch-limit` entries.
              (loop [batch []
                     window-chan (async/timeout sync-window)]
                  ;; `next` is [next-batch next-window-chan] to keep going, or
                  ;; nil once the data channel is closed (a flush was requested).
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
                        ;; Flush requested: commit whatever is still buffered so
                        ;; that every entry of this WAL generation is applied to
                        ;; the memtable BEFORE the switch, then hand off to the
                        ;; flush writer.
                      (when (seq batch)
                        (commit-batch! out-stream file-stream batch memstore size))
                      (.close out-stream)
                      (.close file-stream)
                      (async/>! flush-req-chan :flush)
                      true)
                    (recur (first next) (second next)))))
              (catch Throwable e
                  ;; Fail-stop: an fsync (or other IO) failure poisons the whole
                  ;; store. Writers already waiting were released with :error by
                  ;; commit-batch!; closing data-chan makes any parked/future
                  ;; `>!!` return false so no writer deadlocks, and `core`
                  ;; rejects every subsequent write.
                (reset! poison e)
                (logging/error e "WAL writer failed; the store is poisoned")
                (async/close! data-chan)
                (try (.close out-stream) (catch Throwable _))
                (try (.close file-stream) (catch Throwable _))
                false))]
        (when healthy?
            ;; Wait for the new memtable generation from the flush writer.
          (let [[wal-id memtable] (async/<! flush-wal-chan)]
            (recur wal-id memtable)))))))
