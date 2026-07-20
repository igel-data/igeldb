(ns igeldb.flush
  (:require  [blossom.core :as blossom]
             [clojure.core.async :as async]
             [clojure.tools.logging :as logging]
             [igeldb.io :as io]
             [igeldb.memtable :as memtable]
             [igeldb.sstable :as sstable]
             [igeldb.wal :as wal])
  (:import (java.io FileOutputStream BufferedOutputStream File)))

(defn- switch-memtable!
  [memtable wal-chan]
  ;; the new memtable shares the global seq counter with the old one
  (let [seq-counter (:seq-counter @memtable)
        [old _] (reset-vals! memtable (memtable/create-memtable wal-chan seq-counter))]
    old))

(defn- flush-memtable!
  "Write the memtable out as one L0 SSTable (data + fsync) and return the table
  entry `{:id :level 0 :head-key :tail-key :bloom-filter :size}`. No `.info` file
  -- the manifest is now the sole metadata source (see `sstable/commit-edit!`)."
  [memtable new-id {:keys [sstable-dir bloom-filter]}]
  (let [sstable-path (sstable/get-sstable-path new-id sstable-dir)
        bf (blossom/make-filter bloom-filter)
        entry-set (memtable/entry-set memtable)
        ;; entries are [ikey data] in InternalKey order; head/tail are user_keys
        head-key (:user-key (first (first entry-set)))
        tail-key (:user-key (first (last entry-set)))]
    (logging/info "Starting flush to SSTable" sstable-path)
    (with-open [file-stream (FileOutputStream. sstable-path)
                out-stream (BufferedOutputStream. file-stream 16384)]
      (io/write-format-byte! out-stream)
      (doseq [[ikey data] entry-set]
        (sstable/write-entry! out-stream bf ikey data))
      (.flush out-stream)
      (-> file-stream .getChannel (.force true)))
    {:id new-id :level 0 :head-key head-key :tail-key tail-key
     :bloom-filter bf :size (.length (File. sstable-path))}))

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
    (let [new-id (sstable/next-id! sstable-id)
          old-wal-id @wal-id
          new-wal-chan (async/chan)
          entry (-> (switch-memtable! memtable new-wal-chan)
                    (flush-memtable! new-id config))]
      ;; Ordering is crash-safety-critical:
      ;;   write+fsync SSTable (done above)
      ;;   -> append+fsync the manifest edit  <- COMMIT POINT
      ;;   -> hand the new WAL generation to the worker
      ;;   -> delete the old WAL
      ;; The manifest commit MUST precede the WAL hand-off. Otherwise a crash
      ;; after the SSTable write but before the commit would leave the new
      ;; (higher-id) WAL as the newest; recovery replays only the newest and
      ;; discards the old WAL, whose data lives only in the un-committed (orphan,
      ;; therefore ignored) SSTable -> data loss.
      (sstable/commit-edit! tree {:added [entry] :deleted []})
      ;; Hand the new WAL generation to the worker. `switch-memtable!` has
      ;; already installed the new (empty) memtable as `@memtable`; the worker
      ;; reads its wal-chan, memstore and size from it. The new WAL ID is the
      ;; WAL counter's next value, independent of the SSTable ID.
      (async/>!! flush-wal-chan [(swap! wal-id inc) @memtable])
      ;; Crash-recovery invariant: "SSTable committed" now means "manifest edit
      ;; fsynced". Every WAL except the newest is already committed to the
      ;; manifest, so on restart only the highest-ID WAL is replayed (see
      ;; `wal/load-existing-wal`). Deleting the old WAL before the manifest
      ;; commit would break recovery.
      (io/delete-file (wal/wal-file-path old-wal-id config)))))

(defn spawn-flush-writer
  "Start the flush writer on a dedicated (real) thread: it does blocking file IO
  and blocking channel hand-offs, which belong on a thread rather than a
  core.async go pool. `ready` is delivered once the initial flush has set up the
  first WAL generation (true on success, false if the store was poisoned)."
  [memtable tree sstable-id wal-id poison ready req-chan flush-wal-chan
   compaction-req-chan config]
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
                false)))
          (flush-then-signal! []
            ;; A successful flush may push L0 past its compaction trigger; nudge
            ;; the compaction worker (sliding-buffer channel, so this never
            ;; blocks and signals coalesce).
            (let [ok (safe-flush!)]
              (when ok (async/>!! compaction-req-chan :maybe-compact))
              ok))]
    (async/thread
      (deliver ready (flush-then-signal!))
      (let [threshold (:memtable-size config)]
        (loop []
          (case (async/<!! req-chan)
            :flush (when (flush-then-signal!) (recur))
            :try-flush (do
                         (when (> (deref (:size @memtable)) threshold)
                           ;; close the data channel not to send data to the WAL thread
                           (async/close! (:wal-chan @memtable)))
                         (recur))
            ;; nil: `req-chan` was closed -> shutdown. Close the WAL channel so
            ;; the worker drains and fsyncs any buffered entries; those are
            ;; recovered from the WAL by replay on restart. Then END the loop.
            (async/close! (:wal-chan @memtable))))))))
