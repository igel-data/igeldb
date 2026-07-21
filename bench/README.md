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
thread gives per-op latency, 8 threads gives throughput (what a server actually
sees). Phase 4 gives the two engines *different op counts* on purpose — IgelDB 800
reads, SQLite 8 000 — because SQLite is still ~10× faster per read and each of its
readers is a separate process paying ~3.5 ms of startup, which needs more work to
amortise. Throughput in ops/sec is comparable regardless of op count.

Phase 2 gives IgelDB 32 writers and SQLite 1 deliberately. Group commit coalesces
concurrent writers into shared fsyncs, so IgelDB's durable throughput scales with
concurrency; SQLite's writers serialise on its write lock, so a single writer is
SQLite's *best* case, not a handicap.

## Sample results

macOS, Apple silicon, SSD, SQLite 3.51.0, blossom 2.0.1, **with the Phase 4 sparse
block index**. Numbers are ops/sec; `ratio` is IgelDB ÷ SQLite (higher is better
for IgelDB).

**JVM 21**

| phase | IgelDB | SQLite | ratio |
|---|---:|---:|---:|
| bulk load (1 txn) | 79,438 | 258,392 | 0.31× |
| durable writes | 3,058 | 10,774 | 0.28× |
| point reads (1 thread) | 22,772 | 37,487 | 0.61× |
| point reads (8 threads) | 78,170 | 256,196 | 0.31× |
| range scan (all keys) | 477,608 | 978,848 | 0.49× |

**Babashka 1.12 (SCI)**

| phase | IgelDB | SQLite | ratio |
|---|---:|---:|---:|
| bulk load (1 txn) | 11,028 | 301,040 | 0.04× |
| durable writes | 2,642 | 10,432 | 0.25× |
| point reads (1 thread) | 3,790 | 45,172 | 0.08× |
| point reads (8 threads) | 22,563 | 262,501 | 0.09× |
| range scan (all keys) | 17,605 | 1,183,513 | 0.01× |

> **The SQLite 1-thread read row is mostly process startup.** `sqlite3` startup is
> ~3.5 ms (measured and printed by the script); 200 reads at 37,487/s is 5.3 ms
> total, so most of it is spawning the process. Do **not** read that cell as
> SQLite's read performance — the 8-thread row, which amortises startup over 1 000
> reads per process, is the honest estimate (~256 k reads/s). The same caveat
> flatters IgelDB's 0.61× on that row; **0.31× (8 threads) is the fair number.**

## What the numbers say

**Point reads used to be the weak spot (0.01×); the sparse block index fixed it.**
Before Phase 4, `io/read-value` linear-scanned the SSTable from the front for every
key. It now binary-searches an in-memory sparse index to a block, seeks there, and
scans only within that block:

| phase | before index | after index | speedup |
|---|---:|---:|---:|
| point reads, 1 thread (JVM) | 444 | 22,772 | **51×** |
| point reads, 8 threads (JVM) | 2,179 | 78,170 | **36×** |
| point reads, 1 thread (bb) | 28 | 3,790 | **135×** |
| point reads, 8 threads (bb) | 135 | 22,563 | **167×** |

The ratio against SQLite moved from **0.01× to 0.31×** (8-thread, JVM) — from ~100×
slower to ~3× slower. Latency is also no longer linear in key position: it is now
bounded by where a key sits *within its block*, which is why a key at 99 % of the
file can be cheaper to read than one at 75 %.

bb gains far more than the JVM (135–167× vs 36–51×) because the interpreter tax was
concentrated in exactly the per-entry scanning work the index eliminates.

**Writes and scans were untouched by Phase 4**, as expected — bulk load, durable
writes and range scan all moved within noise. IgelDB now sits at roughly **0.3× of
SQLite** across writes and reads on the JVM, which is a reasonable place for a
pure-Clojure store against a mature C engine.

**The remaining soft spot is the range scan under bb** (0.01×): scanning is pure
per-entry interpretation, so it carries the full ~27× SCI tax and the index does not
help (a scan reads every entry by definition).

### Reads do scale with concurrency — but so do SQLite's

IgelDB reads parallelise well (short read lock over an immutable version snapshot,
so readers don't exclude each other): 22,772 → 78,170 on the JVM (3.4× on 8 threads)
and 3,790 → 22,563 under bb (6.0×). SQLite scales similarly (37,487 → 256,196), so
concurrency does not by itself change the ratio — measuring reads single-threaded
reports latency, not throughput, which is why both are reported.

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

This is still **not** binding, but the index moved it a long way closer. Before
Phase 4 the headroom was four orders of magnitude (1.5 M acq/s against ~2 300
reads/s). Now IgelDB does 78 k reads/s on 8 threads against a lock that sustains
~4.4 M acquires/s at that width — roughly **56× of headroom** rather than ~10 000×.

Each read takes exactly one `with-version` acquire, so the lock becomes the ceiling
once read throughput approaches it. It is the next thing to look at if reads get
materially faster again (a file-handle cache or mmap would do that), and switching
to a non-fair lock would need another answer for writer starvation — the reason
fairness was chosen.

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

Note the last row: at the time, the benchmark barely moved (363 → 387 reads/s),
because with a **single** SSTable and random keys the linear scan dwarfed the bloom
check. Phase 4 removed that scan, so the bloom fix now shows: it is ~1.6 µs of a
~44 µs read rather than ~300 µs of a ~2 700 µs one, and it is paid once *per SSTable
consulted*, so it compounds in multi-level stores and on negative lookups (138×).

**The interpreter tax is very uneven** — this is the main reason to run both:

| phase | JVM ÷ bb |
|---|---:|
| bulk load | 7.2× |
| durable writes | **1.2×** |
| point reads (1 thread) | 6.0× |
| point reads (8 threads) | 3.5× |
| range scan | **27.1×** |

Durable writes barely move (1.2×) because that phase is fsync-bound — the
interpreter is not on the critical path. The point-read tax *fell* (15.9× → 6.0×)
because the index removed most of the interpreted per-entry work; what remains is
mostly file I/O and one small block scan. Range scan keeps the full ~27× tax, since
a scan reads every entry by definition and the index cannot help it.

So Babashka is a reasonable place to run write-heavy fsync-bound workloads and now
point-read workloads too; it remains a poor one for scan-heavy work.

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
