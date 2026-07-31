# IgelDB × jepsen-lite

Fault-injection / consistency verification of IgelDB with
[jepsen-lite](https://github.com/igel-data/jepsen-lite), in three shapes:

- **in-process** — jepsen-lite opens and closes IgelDB inside its own JVM.
  Each workload maps to IgelDB via an adapter + handler in `igeldb/jepsen.clj`.
- **a separate process, killed with SIGKILL** — `igeldb/driver.clj` embeds
  IgelDB behind a small HTTP API and runs as a program of its own. jepsen-lite
  starts it, `kill -9`s it mid-run, and starts it again; `igeldb/client.clj` is
  the adapter that talks to it.
- **the same process, powered off** — that separate process again, with its
  data directory on [lazyfs](https://github.com/dsrhaslab/lazyfs). Each fault
  throws away everything IgelDB never fsynced *and then* kills it, so recovery
  starts from a disk that lost exactly what was never made durable.

The workloads, the checkers and the verdicts are identical in all three. What
differs is the adapter (the protocol IgelDB is reached by) and the target-type
(how it is deployed) — which is jepsen-lite's whole design premise, and the
reason the separate process cost a driver and a client rather than a second
test suite, and `power-off` cost four lines of `:lazyfs` on top of that.

## Run

```sh
clojure -M:jepsen                                       # every workload
clojure -M:jepsen --workload bank                       # one of them
clojure -M:jepsen --workload set --fault crash          # in-process: close! + reopen
clojure -M:jepsen --profile process --workload set --fault crash      # kill -9
clojure -M:jepsen --profile process --workload set --fault power-off  # + fsync
clojure -M:jepsen --profile process --workload counter --fault pause  # SIGSTOP
clojure -M:jepsen --workload bank --time-limit 30 --concurrency 8
clojure -M:jepsen --help
```

The command line is jepsen-lite's own: `lite.runner` takes the suite
`igeldb.jepsen/suite` declares — workloads, profiles, and the one
IgelDB-specific option (`--lazyfs`) — and owns the parsing, the repetition over
workloads, the summary and the exit status. Exits non-zero if any verdict is
`:valid? false`, and 2 if jepsen-lite refused the run.

| flag | |
|---|---|
| `--workload NAME` | `bank`, `register`, `set`, `counter`; may be repeated |
| `--profile NAME` | `in-process` (default) or `process` |
| `--fault NAME` | `crash`, `pause`, `power-off`; may be repeated |
| `--time-limit N` | run for N seconds instead of a fixed op count |
| `--concurrency N` | workers issuing ops |
| `--lazyfs PATH` | a built lazyfs checkout, for `--fault power-off` |

Which faults a profile can inject is jepsen-lite's business, not ours: asking
`in-process` for a `power-off` stops the run with what went wrong and why.

| workload | checks | IgelDB feature exercised |
|---|---|---|
| `bank` | total balance conserved | multi-key atomic transactions (snapshot isolation) |
| `register` | linearizability (Knossos) | CAS via a transaction |
| `set` | no lost / phantom writes | write durability |
| `counter` | reads within increment range | read-modify-write in a transaction |

## This is a permanent correctness gate

`bank` (and `counter`) surfaced a real snapshot-isolation bug — concurrent
read-modify-write transactions lost updates (money was created). Two gaps, both
between "commit confirmed" and "visible to the read path":

1. **write side** — conflict detection read committed state from the memtable, which
   the group-commit worker applies *asynchronously*, so an in-flight commit was
   invisible and a conflicting write slipped through. Fixed by the `pending` map
   (`igeldb.tx`): in-flight commits are consulted alongside the read path.
2. **read side** — `begin-tx` pinned its snapshot to `current-seq` (the confirmed
   seq), which runs ahead of the applied memtable, so `tx-get` returned stale
   values for the tx's own snapshot. Fixed by pinning snapshots to `applied` (the
   memtable high-water mark).

The deterministic in-suite regression is
`igeldb.commit-test/no-lost-updates-under-concurrent-rmw-test`; these jepsen
workloads are the end-to-end gate. Keep both green.

## Three kinds of crash, and what each proves

`--fault crash` on **`in-process`** is `close!` + reopen: a *clean* shutdown and
recovery. It exercises WAL replay and manifest recovery and the durability of
acknowledged writes across a restart. Closing cleanly is required there, because
IgelDB's background threads must stop before the store is reopened on the same
directory.

The same fault on **`--profile process`** is a real `SIGKILL`. No `close!`, no flush, no
shutdown hook — the store is simply gone, and recovery has to come from what
actually reached the disk. Its driver uses a 256-byte memtable and rotates the
manifest after every edit; faults are spaced far enough apart for a flush and
rotation before the next kill, so recovery includes rotated manifests rather
than only the WAL. A recent run:

```
process / set / crash:  1958 acknowledged writes, 0 lost, 5 kills
           8 more came back :info (the connection died mid-request) and turned
           out to have been committed — reported as `recovered`, not as errors
```

Neither tests the loss of writes the OS accepted but never flushed: SIGKILL
kills the process, not the page cache. A store that fsyncs what it
acknowledges and one that merely `write()`s it come through both identically.

`power-off` is the fault that asks. jepsen-lite mounts lazyfs — a FUSE
filesystem that holds writes in a cache of its own until an fsync — under the
driver's data directory, and each fault clears that cache (waiting for lazyfs
to confirm, so the clear is strictly before the kill) and then SIGKILLs. What
IgelDB never synced is simply gone, and recovery has to cope. That makes it the
end-to-end test of the WAL fsync in `igeldb.wal/commit-batch!`, which happens
before a commit is acknowledged — and of the manifest and directory syncs the
files it names depend on.

```sh
# Linux, /dev/fuse, and a built lazyfs checkout
JEPSEN_LITE_LAZYFS=/path/to/lazyfs/lazyfs \
  clojure -M:jepsen --profile process --workload set --fault power-off
# or inline
clojure -M:jepsen --profile process --workload set --fault power-off \
  --lazyfs /path/to/lazyfs/lazyfs
```

Anywhere else — macOS, a container without `--device /dev/fuse` — jepsen-lite
stops the run and says what is missing (exit 2). It never falls back to a plain
kill, which would report durability nobody tested. A recent run:

```
bank / register / set / counter power-off:  all :valid? true
set:  1789 acknowledged writes, 0 lost, 5 power-offs
```

Read a *failure* here as a finding only once the fault is known to have teeth:
jepsen-lite's own demo store, deliberately built not to fsync, is the control —
`clojure -M:run-local counter power-off nofsync` in the jepsen-lite checkout
must come back `:valid? false` on the same host. It does on this one.

Ops that land while the store is dead are recorded honestly, and the
distinction matters to the verdict: a refused connection is a `:fail` (the
request provably never arrived), while a connection dropped mid-request is an
`:info` — nobody can know, and a checker told otherwise would be being lied to.

## Layout

| file | what it is |
|---|---|
| `igeldb/jepsen.clj` | the suite: the two config builders, the in-process adapter and handlers |
| `igeldb/driver.clj` | IgelDB behind an HTTP API — the process that gets killed |
| `igeldb/client.clj` | the adapter and handlers that speak to that driver |

None of it is part of IgelDB proper; it is test code, and it uses nothing a
jepsen-lite user couldn't. What is *not* here is as much to the point: no
argument parsing, no summary, no exit-status logic, no HTTP-connection
lifecycle, and no unpacking of workload ops — `lite.runner`, `lite.client` and
`lite.handlers` own those, and a handler is now one function per operation
(`(fn [conn key old new] ...)` for a CAS) rather than a `case` over `:f`.
Getting those op shapes right by hand is easy to get subtly wrong: the counter
checker wants the increment and not the new total, and a CAS mismatch has to be
reported as *exactly* `false`.

Every run gets a directory of its own under `jepsen-data/<label>/<timestamp>-<uuid>/`,
created fresh and never reused — a crash test means nothing if what it recovers
turns out to be the previous run's. Nothing is deleted, so old runs accumulate
until you remove them.

A `power-off` run lays its directory out in three parts, because lazyfs needs
somewhere to stand:

```
jepsen-data/poweroff-<workload>/<run>/
  data/     the lazyfs mount — IgelDB's data directory, and the only place
            lazyfs can see its writes
  root/     what lazyfs actually writes through to
  *.fifo, lazyfs.toml, lazyfs.log, driver.log
            beside the mount, never under it: a FIFO behind the filesystem
            being set up is one nothing can open
```
