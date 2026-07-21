# IgelDB vs SQLite — small benchmark

A deliberately small, deliberately honest comparison. Read the caveats before
quoting any number: this is a **whole-stack, indicative** comparison, not a
controlled engine benchmark.

## Running

```sh
bb bench/bench.clj    # IgelDB interpreted under Babashka/SCI
clojure -M:bench      # IgelDB on the JVM
```

Run both. The gap between them is Babashka's interpretation overhead, not a
storage-engine difference. Needs `sqlite3` on `PATH`; without it the script
degrades to IgelDB-only. It creates and removes `./bench-data` itself.

## Method

SQLite is driven through the **`sqlite3` CLI** — no pod, no JDBC — so `bb.edn`,
`deps.edn` runtime deps and CI stay untouched. The cost is that SQLite's timings
include SQL text parsing per statement plus one process start.

- **Durability matched**: SQLite `journal_mode=WAL` + `synchronous=FULL` (fsync per
  commit) against IgelDB's fsync per group commit.
- **Schema**: `kv (k BLOB PRIMARY KEY, v BLOB)` — the closest SQL analogue to a KV store.
- **Shape**: 5 000 records bulk-loaded, 1 600 individually-durable writes, 200 random
  point reads; 16-byte keys, 100-byte values.
- **What is timed**: only the engine work. IgelDB's key/value byte arrays and
  SQLite's `.sql` script are both built *before* the timer starts.
- **Reads run against SSTables, not the memtable** — the store is closed and reopened
  after the bulk load, which flushes the replayed WAL to an SSTable.
- Write phases: one timed run against a fresh store. Read phases: one warmup, then
  best-of-3.

### Phases

| # | Phase | IgelDB | SQLite |
|---|-------|--------|--------|
| 1 | Bulk load, one transaction | `with-tx` + `tx-put` ×N | `BEGIN` … `INSERT` ×N … `COMMIT` |
| 2 | Individually-durable writes | 32 concurrent `write!` (group commit) | 1 writer, autocommit |
| 3 | Point reads, 1 thread (**latency**) | `select` | `SELECT … WHERE k = …` |
| 4 | Point reads, 8 threads (**throughput**) | 8 reader threads | 8 concurrent `sqlite3` processes |
| 5 | Range scan | `scan` over all keys | `SELECT sum(length(v)) … WHERE k BETWEEN …` |

Reads are measured **both** ways because they answer different questions: one
thread gives per-op latency (the lens that exposes the linear scan), 8 threads
gives throughput (what a server actually sees). Phase 4 gives the two engines
*different op counts* on purpose — IgelDB 800 reads, SQLite 8 000 — because SQLite
is ~100× faster per read and each of its readers is a separate process paying
~4 ms of startup. Throughput in ops/sec is comparable regardless of op count.

Phase 2 gives IgelDB 32 writers and SQLite 1 deliberately. Group commit coalesces
concurrent writers into shared fsyncs, so IgelDB's durable throughput scales with
concurrency; SQLite's writers serialise on its write lock, so a single writer is
SQLite's *best* case, not a handicap.

## Sample results

macOS, Apple silicon, SSD, SQLite 3.51.0, blossom 2.0.1. Numbers are ops/sec;
`ratio` is IgelDB ÷ SQLite (higher is better for IgelDB).

**JVM 21**

| phase | IgelDB | SQLite | ratio |
|---|---:|---:|---:|
| bulk load (1 txn) | 72,333 | 206,347 | 0.35× |
| durable writes | 3,189 | 11,338 | 0.28× |
| point reads (1 thread) | 444 | 38,128 | 0.01× |
| point reads (8 threads) | 2,249 | 262,532 | 0.01× |
| range scan (all keys) | 400,032 | 982,623 | 0.41× |

**Babashka 1.12 (SCI)**

| phase | IgelDB | SQLite | ratio |
|---|---:|---:|---:|
| bulk load (1 txn) | 11,206 | 298,380 | 0.04× |
| durable writes | 2,703 | 10,873 | 0.25× |
| point reads (1 thread) | 28 | 42,486 | 0.00× |
| point reads (8 threads) | 150 | 258,950 | 0.00× |
| range scan (all keys) | 16,773 | 1,273,250 | 0.01× |

> **The SQLite 1-thread read row is ~85 % process startup.** `sqlite3` startup is
> ~3.9 ms (measured and printed by the script); 200 reads at 38,128/s is 5.2 ms
> total, so almost all of it is spawning the process. Do **not** read that cell as
> SQLite's read performance — the 8-thread row, which amortises startup over 1 000
> reads per process, is the honest estimate (~262 k reads/s).

## What the numbers say

**Point reads are IgelDB's weak spot, by a wide margin (~0.01×).** This is
architectural, not harness noise. Profiling the read path (JVM, 5 000 entries in
one SSTable) attributes essentially all of it to one thing:

| component of a point read | µs |
|---|---:|
| `blossom/hit?` (bloom check) | 1.6 |
| `with-version` (rw read-lock) | 0.1 |
| memtable lookup | 0.2 |
| open the SSTable file | 13.3 |
| **`io/read-value` for a mid-table key** | **2,722.9** |

An SSTable carries **no block index**, so `io/read-value` scans the table file
linearly for every key — reading *and CRC-verifying the 100-byte value of every
entry it skips*, when it only needs the keys to compare. Latency is linear in the
key's position: 0.34 ms at the front of the table, 5.07 ms at the back (≈0.93 µs
per entry skipped). That scan is **~98 % of a mid-table read**. SQLite's B-tree
does a logarithmic descent instead.

Adding a sparse block index (or at minimum, skipping the value segment while
scanning) is the single highest-value optimisation available.

### Reads do scale with concurrency — but so do SQLite's

IgelDB reads parallelise well (short read lock over an immutable version
snapshot, so readers don't exclude each other). On a 12-core machine, 5 000
entries:

| threads | reads/sec | speedup |
|---:|---:|---:|
| 1 | 363 | 1.00× |
| 2 | 738 | 2.03× |
| 4 | 1,195 | 3.29× |
| 8 | 2,087 | 5.75× |
| 16 | 2,256 | 6.21× |

So a single-threaded read number is latency, not throughput — measuring only that
understates IgelDB by ~5–6×. But the **ratio is unchanged (0.01×)**, because
SQLite parallelises just as well (40,937 → 262,532, 6.3×). Concurrency does not
narrow the gap; only the block index will.

### A latent scaling issue: the read lock is *fair*

`sstable/make-rw-lock` uses `(Semaphore. RW_PERMITS true)` — fair mode, chosen so a
waiting deletion isn't starved by a stream of readers. Fairness routes every
acquire through the AQS FIFO queue, so readers serialise on the lock's own queue
even holding non-conflicting permits. Measured in isolation (acquire + release,
empty body):

| threads | acquires/sec | vs 1 thread |
|---:|---:|---:|
| 1 | 27,705,950 | 1.00× |
| 8 | 4,421,831 | 0.16× |
| 16 | 1,499,769 | **0.05×** |

This is **not** binding today — 1.5 M acq/s against ~2 300 actual reads/s is four
orders of magnitude of headroom. But if the block index lands and reads drop to
~15 µs, 16 threads would want ~1.07 M reads/s, which is the same order as that
ceiling. Worth revisiting as a follow-on to the index work, not before.

### A fixed per-read tax that used to be here (fixed in blossom 2.0.1)

Earlier runs of this benchmark also paid ~300 µs on *every* read before touching
the file. `blossom.hash/get-hash-fn` folded the 32-byte SHA-256 digest with
`areduce` over an **untyped** array, so `alength` and 32 × `aget` resolved
reflectively — ~93 µs per hash × 3 hashes. blossom 2.0.1 adds the missing
`^bytes` / `^longs` / `^objects` hints:

| | blossom 2.0.0 | blossom 2.0.1 | |
|---|---:|---:|---|
| `blossom/hit?` | 335 µs | 1.6 µs | 209× |
| `select` of an **absent** key (bloom miss, no I/O) | 330.5 µs | 2.4 µs | 138× |
| `select` of key #0 (first entry) | 352.5 µs | 25.1 µs | 14× |
| `select` of key #2500 (mid table) | 2,688.6 µs | 2,774.8 µs | unchanged |

Note the last row: this benchmark barely moved (363 → 387 reads/s) because it uses
a **single** SSTable and random keys, so the scan dwarfs the bloom check. The fix
matters most where this benchmark is weakest — **negative lookups** (138×) and
**multi-level stores**, where `sstable/select` pays the bloom check once *per
SSTable consulted*. Treat the point-read row as a measure of the linear scan, not
of bloom performance.

**The interpreter tax is very uneven** — this is the main reason to run both:

| phase | JVM ÷ bb |
|---|---:|
| bulk load | 6.5× |
| durable writes | **1.2×** |
| point reads (1 thread) | 15.9× |
| point reads (8 threads) | 15.0× |
| range scan | 23.9× |

Durable writes barely move (1.2×) because that phase is fsync-bound — the
interpreter is not on the critical path. CPU-bound phases degrade 7–30×. So
Babashka is a perfectly reasonable place to run *write-heavy, fsync-bound*
IgelDB workloads, and a poor one for scan- or read-heavy work.

**Writes are within the same order of magnitude** (0.25–0.30×) against a mature
C engine, which is a reasonable place for a pure-Clojure store to be.

## Caveats

1. IgelDB under bb is interpreted; those numbers are not engine performance.
2. SQLite's numbers include SQL parsing per statement (the CLI has no
   prepared-statement reuse). A real client would be faster — most of all on point
   reads, so SQLite's read figure is a *floor*, making the gap in phase 3 if
   anything understated.
3. fsync semantics are platform-dependent. On macOS neither engine issues
   `F_FULLFSYNC` by default, so "durable" here means `fsync()` returned, not that
   data reached stable media. Both are affected equally, but absolute write
   numbers are optimistic.
4. Different data models (KV vs SQL) and structures: IgelDB is an LSM tree
   (write-optimised), SQLite a B-tree (read/update-optimised).
5. Small dataset, warm page cache, one machine, few repetitions. IgelDB's
   `memtable-size` and `sync-window-time` materially move its numbers and are
   printed with every run.
