(ns igeldb.commit
  (:require [clojure.core.async :as async]
            [igeldb.data :as data]
            [igeldb.store :as store]
            [igeldb.tx :as tx]))

(defn- latest-committed-seq
  "The latest committed seq for user_key `k`: the max of the pending seq (in-flight
  commits confirmed under the commit lock but not yet applied) and the read-path
  seq (the applied state -- memtable/immutable/tree). The read path alone MISSES a
  commit during its fsync+apply window, which is the lost-update gap the `pending`
  map closes -- see `tx/pending-invariant`. 0 if `k` was never written.

  READ ORDER IS LOAD-BEARING: read `pending` FIRST, then the store. The worker moves
  a commit from pending to the read path by applying it to the memtable and THEN
  clearing pending (`wal/commit-batch!`). So either we read pending while it is
  still set (caught), or it was already cleared -- which happens-after the apply, so
  the store read below is guaranteed to see that commit. Reading the store FIRST
  would let a commit that transitions between the two reads (store observes the OLD
  state, then the worker applies+clears, then pending observes the CLEARED state) be
  missed by both -> a lost update. That window is tiny, so it only surfaced under
  load on a slower environment; this ordering makes it impossible, not merely rare."
  [kvs k]
  (let [p (long (or (tx/pending-seq (:registry kvs) k) 0))]
    (max p (long (or (store/latest-seq kvs k) 0)))))

(defn- conflict?
  "Write-write conflict (first-committer-wins): true if ANY write-set key has a
  committed version newer than the tx's snapshot (its latest committed seq >
  snapshot seq). Blind writes are included -- every key is looked up. Runs inside
  the commit lock, so the pending map it reads is consistent with the seq it is
  about to assign."
  [kvs snapshot-seq entries]
  (some (fn [[k _]] (> (latest-committed-seq kvs k) snapshot-seq)) entries))

(defn commit!
  "Serialize commits under the store's commit lock, holding it over
  {conflict-check -> seq assign -> enqueue one WAL record}. fsync + memtable apply
  stay in the group-commit worker. Assigning the seq and enqueuing in the same
  locked region makes enqueue order == seq order, so the worker's FIFO gives WAL
  order == seq order.

  `entries` are folded [user_key data] pairs (one per user_key). `snapshot-seq` is
  the tx's pinned snapshot, or nil for a non-tx auto-commit op. Returns:
    :committed  durably fsynced and applied
    :conflict   (tx only) a write-set key was committed after the snapshot
    :switched   the memtable switched mid-enqueue; the caller may retry
  Throws {:retriable false} if the worker failed the commit."
  [kvs snapshot-seq entries]
  (let [registry (:registry kvs)
        commit-lock (:commit-lock kvs)
        memtable (:memtable kvs)
        comp-chan (async/chan)
        outcome (locking commit-lock
                  (if (and snapshot-seq (conflict? kvs snapshot-seq entries))
                    :conflict
                    (let [s (tx/next-seq! registry)
                          ks (mapv first entries)
                          record (mapv (fn [[k value]]
                                         [(data/->ikey k s) value])
                                       entries)]
                      ;; Record the in-flight commit BEFORE enqueuing, so the worker
                      ;; can never apply+clear it before it is recorded. On a
                      ;; memtable switch (enqueue fails) undo it conditionally.
                      (tx/record-pending! registry ks s)
                      (if (async/>!! (:wal-chan @memtable) [s record comp-chan])
                        :enqueued
                        (do (tx/clear-pending! registry ks s)
                            :switched)))))]
    (case outcome
      :conflict :conflict
      :switched :switched
      :enqueued (case (async/<!! comp-chan)
                  :done :committed
                  ;; :error (fsync fault) or a closed channel: the store is being
                  ;; poisoned; igeldb.write/run-mutation! will re-check and
                  ;; report the cause.
                  (throw (ex-info "Commit failed" {:retriable false}))))))

(defn auto-commit!
  "Non-tx auto-commit of a folded write-set (no snapshot -> no conflict check).
  Adapts `commit!` to `igeldb.write/run-mutation!`'s contract: nil on success, a
  retriable ex-info on a memtable switch."
  [kvs entries]
  (case (commit! kvs nil entries)
    :committed nil
    :switched (throw (ex-info "Write failed due to memtable switching"
                              {:retriable true}))))

(defn tx-commit-attempt!
  "One commit-handler attempt for a tx: :committed / :conflict, or a retriable
  throw on a memtable switch so `igeldb.write/run-mutation!` retries the enqueue."
  [kvs snapshot-seq entries]
  (case (commit! kvs snapshot-seq entries)
    :committed :committed
    :conflict :conflict
    :switched (throw (ex-info "commit hit a memtable switch" {:retriable true}))))
