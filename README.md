# IgelDB

An embeddable key-value store for the Clojure ecosystem — pure Clojure, zero native
dependencies, and usable directly from [Babashka](https://babashka.org/) with no pod and no
native binary. Just add it to your classpath.

Built on an LSM tree with leveled compaction, IgelDB persists every write to disk and is
designed to be crash-safe: durability isn't an afterthought but a tested property.

## Why IgelDB

- **Drop-in for Babashka.** Unlike stores that ship a native binary (and reintroduce the
  per-OS/per-arch distribution problem), IgelDB is pure Clojure. It loads on a Babashka
  classpath as-is — no pod process, no compiled artifact to build or match to your platform.
- **Zero native dependencies.** Runs on plain JVM bytecode, so distribution and debugging on
  multi-architecture environments (e.g. mixed-arch Kubernetes) stay simple.
- **Crash-safe by design.** Every write goes through a write-ahead log with `fsync`-backed
  group commit; the memtable is only updated after the WAL is durably persisted. Compaction
  and the manifest are structured so a crash at any point recovers to a consistent state.
  These properties are covered by tests, including crash-recovery paths.
- **Lightweight footprint.** Small defaults tuned for embedding in an application process.
  Everything (memtable size, compaction thresholds, level sizing) is configurable. For
  hundreds-of-GB workloads, reach for RocksDB — IgelDB targets the embedded niche.
- **Concurrent reads.** Readers work against an immutable snapshot with no locking, so reads
  proceed concurrently with writes and compaction.

## Design

- **LSM tree** with a memtable (an immutable sorted map, swapped atomically), on-disk
  SSTables, and **leveled compaction**.
- **Write path:** WAL append → `fsync` (group-committed) → memtable apply → return. A single
  commit worker applies each batch in WAL order, so WAL order and memtable order always match.
- **Manifest:** an append-only log of version edits (added/deleted tables), `fsync`-committed;
  startup rebuilds state by replaying it. A crash between steps recovers cleanly.
- **Bloom filters** on SSTables (via [blossom](https://github.com/yito88/blossom)) skip disk
  reads on misses.

## Usage

```clojure
(require '[igeldb.core :as igel])

;; Generate a KVS with a config file
(def kvs (igel/gen-kvs "config.yaml"))

;; A key and a value as bytes
(def key1 (.getBytes "key1"))
(def val1 (.getBytes "val1"))

;; Write the key-value pair
(igel/write! kvs key1 val1)

;; Read the key-value pair
(igel/select kvs key1)
; -> #whidbey/bin "dmFsMQ"

;; with deserialization
(String. (igel/select kvs key1))
; -> "val1"

;; Write some pairs
(doseq [i (range 2 5)]
  (igel/write! kvs (.getBytes (str "key" i)) (.getBytes (str "val" i))))

;; Scan pairs with from-key(inclusive) and to-key(not-inclusive)
(def from-key (.getBytes "key0"))
(def to-key (.getBytes "key3"))
(igel/scan kvs from-key to-key)
; -> ([#whidbey/bin "a2V5MQ" #whidbey/bin "dmFsMQ"]
;     [#whidbey/bin "a2V5Mg" #whidbey/bin "dmFsMg"])

;; with deserialization
(map (fn [[k v]] [(String. k) (String. v)])
     (igel/scan kvs from-key to-key))
; -> (["key1" "val1"] ["key2" "val2"])

;; Delete a key
(igel/delete! kvs (.getBytes "key2"))

;; The key2 was deleted
(map (fn [[k v]] [(String. k) (String. v)])
     (igel/scan kvs from-key to-key))
; -> (["key1" "val1"])

;; Close the store when done: flushes and releases resources.
;; After close!, reads and writes are rejected.
(igel/close! kvs)
```

Keys and values are `byte[]`. Serialize your own values to bytes before writing.

### Babashka

IgelDB runs under Babashka with no extra setup — add `src` and the runtime deps to your
classpath and `require` it as usual. See [`test/bb_smoke.clj`](test/bb_smoke.clj) for a
full write → flush → compaction → reopen (recovery) cycle running on `bb`.

## Configuration

Configuration is a YAML file (see `gen-kvs`). Key parameters:

| Key | Meaning |
| --- | --- |
| `sstable-dir` / `wal-dir` | Directories for SSTables and WAL files |
| `memtable-size` | Flush threshold; write buffer size before flushing to an SSTable |
| `sync-window-time` | Group-commit window (ms) for batching `fsync` |
| `l0-compaction-trigger` | L0 table count that triggers L0→L1 compaction |
| `l0-stall-threshold` | L0 table count at which writers stall (back-pressure safety valve) |
| `level-size-multiplier` | Per-level size growth factor |

An open IgelDB instance exclusively owns both configured storage directories.
Opening another instance that shares either directory fails until the first
instance's synchronous `close!` returns.

## License

Copyright © 2023 Yuji Ito

Distributed under the Eclipse Public License 2.0 (or the secondary licenses it
allows). See [LICENSE](LICENSE) for details.
