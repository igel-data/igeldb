(ns igeldb.store)

(defprotocol IStoreRead
  "Interface for reading from the data store, at a given `snapshot-seq` (the
  newest version of each user_key with seq <= snapshot-seq). Non-tx reads pass
  `Long/MAX_VALUE` (latest)."
  ;; select could return three value types; the valid data, the deleted data,
  ;; and nil.
  ;; The deleted data is a tombstone, and nil means that the key doesn't exist.
  (select [this ^bytes k snapshot-seq])
  (scan [this ^bytes from-key ^bytes to-key snapshot-seq])
  ;; The seq of the newest committed version of `k` (latest, ignoring snapshots),
  ;; or nil if absent. Used for commit-time write-write conflict detection.
  (latest-seq [this ^bytes k]))

(defprotocol IStoreMutate
  "Interface for mutations to the data store"
  (write! [this ^bytes k ^bytes v])
  ;; Apply an already-built `data/Data` record (a value or a tombstone).
  ;; Used by the group-commit worker to apply a batch to the memtable after
  ;; the WAL fsync completes, preserving the exact data enqueued by the writer.
  (write-data! [this ^bytes k data])
  ;; Apply a whole batch of [k data] pairs (in order, last wins) as one atomic
  ;; snapshot publish, so a lock-free reader sees the pre-batch or post-batch
  ;; state, never a torn mid-batch view. The group-commit worker uses this.
  (write-batch! [this entries])
  (delete! [this ^bytes k]))
