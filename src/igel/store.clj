(ns igel.store)

(defprotocol IStoreRead
  "Interface for reading from the data store"
  ;; select could return three value types; the valid data, the deleted data,
  ;; and nil.
  ;; The deleted data is a tombstone, and nil means that the key doesn't exist.
  (select [this ^bytes k])
  (scan [this ^bytes from-key ^bytes to-key]))

(defprotocol IStoreMutate
  "Interface for mutations to the data store"
  (write! [this ^bytes k ^bytes v])
  ;; Apply an already-built `data/Data` record (a value or a tombstone).
  ;; Used by the group-commit worker to apply a batch to the memtable after
  ;; the WAL fsync completes, preserving the exact data enqueued by the writer.
  (write-data! [this ^bytes k data])
  (delete! [this ^bytes k]))
