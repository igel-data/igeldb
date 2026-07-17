(ns igel.flush
  (:require  [blossom.core :as blossom]
             [clojure.core.async :as async]
             [clojure.tools.logging :as logging]
             [igel.io :as io]
             [igel.memtable :as memtable]
             [igel.sstable :as sstable]
             [igel.wal :as wal])
  (:import (java.io FileOutputStream BufferedOutputStream)))

(defn- switch-memtable!
  [memtable wal-chan]
  (let [[old _] (reset-vals! memtable (memtable/create-memtable wal-chan))]
    old))

(defn- flush-memtable!
  [memtable new-id {:keys [sstable-dir bloom-filter]}]
  (let [sstable-path (sstable/get-sstable-path new-id sstable-dir)
        info-path (sstable/get-info-path new-id sstable-dir)
        bf (blossom/make-filter bloom-filter)
        entry-set (memtable/entry-set memtable)
        head-key (-> entry-set first first)
        tail-key (-> entry-set last first)]
    (logging/info "Starting flush to SSTable" sstable-path)
    (with-open [file-stream (FileOutputStream. sstable-path)]
      (with-open [out-stream (BufferedOutputStream. file-stream 16384)]
        (doseq [entry entry-set]
          (let [k (first entry)
                data (second entry)
                value (:value data)]
            ;; write the key
            (io/write-bytes! out-stream k)
            ;; write the value
            ;; if it's deleted, write only the length 0
            (if (:deleted? data)
              (io/write-tombstone! out-stream)
              (io/write-bytes! out-stream value))
            (blossom/add bf k)))
        (.flush out-stream)
        (-> file-stream .getFD .sync)))
    (let [table-info (sstable/->TableInfo bf head-key tail-key)]
      (sstable/write-table-info info-path table-info 0)
      table-info)))

(defn- flush!
  "Commit the current memtable to an SSTable (when it holds data) and rotate the
  WAL. The SSTable ID and the WAL ID are independent monotonic counters.

  Runs on a real thread (see `spawn-flush-writer`), so the blocking `>!!` and
  file IO here are intentional and do not occupy a core.async pool thread."
  [memtable tree sstable-id wal-id flush-wal-chan config]
  (if (zero? (-> @memtable :size deref))
    ;; Nothing to flush: hand the current (unchanged) WAL generation to the
    ;; worker. Only reached for the very first flush of an empty store.
    (async/>!! flush-wal-chan [@wal-id @memtable])
    (let [new-id @sstable-id
          old-wal-id @wal-id
          new-wal-chan (async/chan)
          table-info (-> (switch-memtable! memtable new-wal-chan)
                         (flush-memtable! new-id config))]
      ;; Hand the new WAL generation to the worker. `switch-memtable!` has
      ;; already installed the new (empty) memtable as `@memtable`; the worker
      ;; reads its wal-chan, memstore and size from it. The new WAL ID is the
      ;; WAL counter's next value, independent of the SSTable ID.
      (async/>!! flush-wal-chan [(swap! wal-id inc) @memtable])
      (sstable/add-new-table! tree new-id table-info)
      ;; Crash-recovery invariant: the SSTable is committed BEFORE the old WAL is
      ;; deleted, so every WAL except the newest is already in an SSTable. On
      ;; restart only the highest-ID WAL is replayed (see `wal/load-existing-wal`).
      ;; A future change that deletes the WAL before committing the SSTable would
      ;; break recovery.
      (io/delete-file (wal/wal-file-path old-wal-id config))
      (swap! sstable-id inc))))

(defn spawn-flush-writer
  "Start the flush writer on a dedicated (real) thread: it does blocking file IO
  and blocking channel hand-offs, which belong on a thread rather than a
  core.async go pool. `ready` is delivered once the initial flush has set up the
  first WAL generation (true on success, false if the store was poisoned)."
  [memtable tree sstable-id wal-id poison ready req-chan flush-wal-chan config]
  (letfn [(safe-flush! []
            ;; Fail-stop (3-3): a flush failure poisons the store just like an
            ;; fsync failure. If the loop died silently instead, nothing would
            ;; ever flush again and the memtable would grow unbounded.
            ;; Returns true on success, false once poisoned.
            (try
              (flush! memtable tree sstable-id wal-id flush-wal-chan config)
              true
              (catch Throwable e
                (reset! poison e)
                (logging/error e "Flush writer failed; the store is poisoned")
                false)))]
    (async/thread
      (deliver ready (safe-flush!))
      (let [threshold (:memtable-size config)]
        (loop []
          (case (async/<!! req-chan)
            :flush (when (safe-flush!) (recur))
            :try-flush (do
                         (when (> (deref (:size @memtable)) threshold)
                           ;; close the data channel not to send data to the WAL thread
                           (async/close! (:wal-chan @memtable)))
                         (recur))
            ;; nil: `req-chan` was closed -> shutdown. Close the WAL channel so
            ;; the worker drains and fsyncs any buffered entries; those are
            ;; recovered from the WAL by replay on restart. Then END the loop.
            (async/close! (:wal-chan @memtable))))))))
