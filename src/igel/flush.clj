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
  [memtable tree sstable-id flush-wal-chan config]
  (if (zero? (-> @memtable :size deref))
    ;; no flush
    (async/go
      (async/>! flush-wal-chan [@sstable-id @memtable]))
    ;; flush and update the memtable and the sstables
    (let [new-id @sstable-id
          wal-chan (async/chan)
          table-info (-> (switch-memtable! memtable wal-chan)
                         (flush-memtable! new-id config))]
      ;; Send the new WAL ID and the new memtable generation to the WAL writer.
      ;; `switch-memtable!` has already installed it as `@memtable`; the worker
      ;; reads its wal-chan, memstore and size from it.
      (async/>!! flush-wal-chan [(+ @sstable-id 2) @memtable])
      ;; Update the tree
      (sstable/add-new-table! tree new-id table-info)
      ;; The previous WAL can be deleted
      (io/delete-file (wal/wal-file-path @sstable-id config))
      ;; Update SSTable ID for the next
      (swap! sstable-id (partial + 2)))))

(defn spawn-flush-writer
  [memtable tree sstable-id poison req-chan flush-wal-chan config]
  (letfn [(safe-flush! []
            ;; Fail-stop (3-3): a flush failure poisons the store just like an
            ;; fsync failure. If the go-loop died silently instead, nothing
            ;; would ever flush again and the memtable would grow unbounded.
            ;; Returns true on success, false once poisoned.
            (try
              (flush! memtable tree sstable-id flush-wal-chan config)
              true
              (catch Throwable e
                (reset! poison e)
                (logging/error e "Flush writer failed; the store is poisoned")
                false)))]
    ;; first flush
    (async/go (safe-flush!))
    (let [threshold (:memtable-size config)]
      (async/go-loop []
        (case (async/<! req-chan)
          :flush (when (safe-flush!) (recur))
          :try-flush (do
                       (when (> (deref (:size @memtable)) threshold)
                         ;; close the data channel not to send data to the WAL thread
                         (async/close! (:wal-chan @memtable)))
                       (recur))
          ;; nil: `req-chan` was closed -> shutdown. Close the WAL channel so the
          ;; worker drains and fsyncs any buffered entries; those are recovered
          ;; from the WAL by replay on restart. Then END the loop (no `recur`):
          ;; recurring here would re-read the closed `req-chan`, get nil again,
          ;; and busy-loop a CPU core for the lifetime of the JVM.
          (async/close! (:wal-chan @memtable)))))))
