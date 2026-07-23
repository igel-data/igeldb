# IgelDB × jepsen-lite

Fault-injection / consistency verification of IgelDB with
[jepsen-lite](https://github.com/yito88/jepsen-lite). IgelDB is an `:in-process`
target; each workload maps to IgelDB via an adapter + handler in
`igeldb/jepsen.clj`.

## Run

```sh
clojure -M:jepsen               # bank + register + set + counter
clojure -M:jepsen bank          # one workload
clojure -M:jepsen set crash     # with the crash nemesis
clojure -M:jepsen bank time=10  # run for 10 seconds
```

Exits non-zero if any workload's verdict is `:valid? false`.

`time=<seconds>` sets jepsen-lite's `:time-limit`. A timed run continues for
that duration instead of ending after the workload's default operation count.

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

The `:crash` nemesis for an `:in-process` target is `close!` + reopen — a clean
shutdown and recovery, exercising WAL replay / manifest recovery and the
durability of acknowledged writes, **not** a hard `kill -9` mid-fsync (that needs a
`:local-process` target, not yet in jepsen-lite).
