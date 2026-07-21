(ns igeldb.flush
  (:require  [clojure.core.async :as async]
             [clojure.tools.logging :as logging]
             [igeldb.io :as io]
             [igeldb.memtable :as memtable]
             [igeldb.sstable :as sstable]
             [igeldb.wal :as wal]))

(defn- switch-memtable!
  "Swap in a fresh empty memtable and return the old one (to be flushed). Publish
  the memtable about to be flushed as the *immutable memtable* BEFORE the swap, so
  a concurrent reader never sees a gap where the switched-out data lives in
  neither `@memtable` nor a committed version. Reads consult
  mutable -> immutable -> version; at this instant the immutable memtable IS the
  current mutable one, so nothing is missed. `flush!` clears the reference once
  the flush is committed to a version. Only the flush thread swaps the outer
  `memtable` atom, so reading it then resetting it is race-free here."
  [memtable immutable-memtable wal-chan]
  (let [old @memtable
        ;; the new memtable shares the global tx registry with the old one
        registry (:registry old)]
    (reset! immutable-memtable old)
    (reset! memtable (memtable/create-memtable wal-chan registry))
    old))

(defn- flush-memtable!
  "Write the memtable out as one L0 SSTable (data + fsync) and return the table
  entry `{:id :level 0 :head-key :tail-key :bloom-filter :size}`. No `.info` file
  -- the manifest is now the sole metadata source (see `sstable/commit-edit!`)."
  [memtable new-id config]
  (let [entry-set (memtable/entry-set memtable)
        ;; highest seq flushed -- carried into the manifest edit for next-seq
        ;; recovery (a clean shutdown after a flush leaves an empty WAL, so the
        ;; newest seq can live only in SSTables). Ignored by per-table encoding.
        max-seq (reduce (fn [m [ikey _]] (max m (:seq ikey))) 0 entry-set)
        writer (sstable/open-table! new-id config)]
    (logging/info "Starting flush to SSTable" (:path writer))
    (doseq [[ikey data] entry-set]
      (sstable/write-entry! writer ikey data))
    ;; head/tail keys, bloom, sparse index and size all come from the writer
    (assoc (sstable/close-table! writer 0) :max-seq max-seq)))

(defn- flush!
  "Commit the current memtable to an SSTable (when it holds data) and rotate the
  WAL. The SSTable ID and the WAL ID are independent monotonic counters.

  Runs on a real thread (see `spawn-flush-writer`), so the blocking `>!!` and
  file IO here are intentional and do not occupy a core.async pool thread."
  [memtable immutable-memtable tree sstable-id wal-id flush-wal-chan config]
  (if (zero? (-> @memtable :size deref))
    ;; Nothing to flush: hand the current (unchanged) WAL generation to the
    ;; worker. Only reached for the very first flush of an empty store.
    (async/>!! flush-wal-chan [@wal-id @memtable])
    (let [new-id (sstable/next-id! sstable-id)
          old-wal-id @wal-id
          new-wal-chan (async/chan)
          entry (-> (switch-memtable! memtable immutable-memtable new-wal-chan)
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
      (sstable/commit-edit! tree {:added [entry] :deleted []
                                  :max-seq (:max-seq entry)})
      ;; The flushed data now lives in a committed version; drop the immutable
      ;; memtable reference. Reads fall through mutable -> version with no gap:
      ;; before this reset the data is in both the immutable memtable and the
      ;; version (consistent), after it it is in the version only.
      (reset! immutable-memtable nil)
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
  [memtable immutable-memtable tree sstable-id wal-id poison ready req-chan
   flush-wal-chan compaction-req-chan config]
  (letfn [(safe-flush! []
            ;; Fail-stop (3-3): a flush failure poisons the store just like an
            ;; fsync failure. If the loop died silently instead, nothing would
            ;; ever flush again and the memtable would grow unbounded.
            ;; Returns true on success, false once poisoned.
            (try
              (flush! memtable immutable-memtable tree sstable-id wal-id
                      flush-wal-chan config)
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
            ;; nil: `req-chan` closed -> shutdown; end the loop, then clean up below.
            nil))
        ;; The loop ends on shutdown (req-chan closed) OR on a fault (a flush failed
        ;; and did not recur). Either way stop the WAL worker so it never parks
        ;; forever: close its data channel (it drains + fsyncs any buffered entries,
        ;; recovered by replay on restart) and the generation hand-off channel (so it
        ;; exits instead of awaiting a next generation that never comes). Both closes
        ;; are idempotent. This thread then returns and `close!` joins on it.
        (async/close! (:wal-chan @memtable))
        (async/close! flush-wal-chan)))))
