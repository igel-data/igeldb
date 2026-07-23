(ns igeldb.tx
  (:require [igeldb.data :as data]))

;; Global commit-order / MVCC state shared across the whole store (one per KVS):
;;
;;  - `seq-counter`  the monotonic commit-order counter. 1 tx = 1 seq (Point 1);
;;                   its current value is the highest seq assigned so far. New
;;                   commits take `next-seq!`; a begin-tx pins its snapshot at the
;;                   current value.
;;  - `active`       a multiset of *live-tx snapshot seqs*, held as a sorted-map
;;                   `seq -> count` so the minimum key is O(1). A multiset (not a
;;                   plain set) because several txs can pin the SAME snapshot seq
;;                   (concurrent begins with no commit between them); each must be
;;                   removed independently.
;;  - `pending`      user_key -> seq of *in-flight* commits: confirmed (seq
;;                   assigned under the commit lock, WAL record enqueued) but not
;;                   yet applied to the memtable by the group-commit worker. Closes
;;                   the gap between "commit confirmed" and "visible in the read
;;                   path" -- see `pending-invariant` and
;;                   `igeldb.commit/conflict?`.
;;                   Keyed by user_key, so a sorted-map keyed by the byte-array
;;                   comparator (byte arrays don't hash/= by value).
;;  - `applied`      the max seq the group-commit worker has APPLIED to the
;;                   memtable. `begin-tx` pins its snapshot to this, NOT to
;;                   `seq-counter`: every seq <= `applied` is materializable by the
;;                   read path, so a tx never pins a snapshot the read path can't
;;                   serve (which would return stale values -> lost updates). A
;;                   seq assigned-but-not-yet-applied is, to any external observer,
;;                   not yet committed (commits block until apply), so excluding it
;;                   from a fresh snapshot is correct, not stale.
;;
;; `min-active-snapshot-seq` is the MVCC GC floor (Point 3): the oldest snapshot a
;; live tx can see. Compaction (Step 6) keeps, per user_key, the newest version
;; <= this floor plus every newer version, so it never drops a version a live tx
;; still needs.

(defrecord TxRegistry [seq-counter active pending applied])

(defn create-registry
  "Create the global tx registry. `init-seq` seeds both the commit-order counter
  and the applied high-water mark (on startup the replayed/recovered data is
  already materializable by the read path, so applied == init-seq; 0 for a fresh
  store)."
  ([] (create-registry 0))
  ([init-seq]
   (->TxRegistry (atom init-seq)
                 (atom (sorted-map))
                 (atom (sorted-map-by (data/byte-array-comparator)))
                 (atom init-seq))))

(defn current-seq
  "The highest commit-order seq assigned so far. A begin-tx pins this as its
  snapshot; non-tx reads use `Long/MAX_VALUE` instead (always latest)."
  [registry]
  @(:seq-counter registry))

(defn next-seq!
  "Atomically assign and return the next commit-order seq."
  [registry]
  (swap! (:seq-counter registry) inc))

(defn seed-seq!
  "Reset the commit-order counter AND the applied high-water mark to `n` (recovery
  seeds them to the max replayed seq: later writes get strictly higher seqs, and
  the recovered data is already materializable so applied == n). Init only."
  [registry n]
  (reset! (:seq-counter registry) n)
  (reset! (:applied registry) n))

(defn current-applied
  "The max seq the worker has applied to the memtable. `begin-tx` pins its
  snapshot to this (see the `applied` note above)."
  [registry]
  @(:applied registry))

(defn mark-applied!
  "Advance the applied high-water mark to `seq` (monotonic max). Called by the
  group-commit worker AFTER it has published the memtable apply, so any reader
  that observes the new applied value can materialize every seq <= it."
  [registry seq]
  (swap! (:applied registry) max (long seq)))

(defn register-snapshot!
  "Register a live tx pinned at snapshot seq `s` (increment its multiset count).
  Called on begin-tx."
  [registry s]
  (swap! (:active registry) update s (fnil inc 0)))

(defn deregister-snapshot!
  "Remove one live tx pinned at snapshot seq `s` (decrement, dropping the key at
  zero). Called on commit-tx / rollback-tx."
  [registry s]
  (swap! (:active registry)
         (fn [m]
           (let [c (get m s 0)]
             (if (<= c 1) (dissoc m s) (assoc m s (dec c)))))))

(defn min-active-snapshot-seq
  "The minimum snapshot seq over all live txs, or -- when none is active -- the
  current seq (nothing older than 'now' needs preserving).

  Reads `current-seq` FIRST, then the active set, and returns their `min`. Order
  matters for the no-active-tx race: any tx that begins after this read pins a
  snapshot >= the `current-seq` we observed (the counter only grows), so a floor
  of the observed current-seq never exceeds a future live tx's snapshot -- GC
  stays safe."
  [registry]
  (let [s (current-seq registry)
        m (first (keys @(:active registry)))]
    (if m (min m s) s)))

;; ---- pending: in-flight commits (conflict detection) ----------------------
;;
;; INVARIANT (`pending-invariant`): every commit that has been confirmed (seq
;; assigned under the commit lock, WAL record enqueued) and is not yet visible in
;; the read path (memtable) has each of its write-set keys present in `pending`
;; with seq >= its own. Therefore, for conflict detection,
;;   latest-committed-seq(K) = max(read-path latest seq for K, pending seq for K).
;; An entry leaves `pending` only once/after the write becomes visible in the
;; memtable, so the union is gap-free. This invariant is the whole point of the
;; map; any change to the apply ordering or the record/clear sites can break it,
;; the same class of bug it was added to fix.
;;
;; Access: `record-pending!` (handler, under the commit lock, on a confirmed
;; commit) and `pending-seq` (handler, under the commit lock, in conflict-check);
;; `clear-pending!` (group-commit worker, at the memtable-apply site, NOT under the
;; commit lock). All go through the atom's `swap!`/`deref`, so the worker's
;; conditional removal is safe against concurrent handler access without extending
;; the commit lock across the worker's apply.

(defn record-pending!
  "Mark each of `keys` as having an in-flight commit at `seq` (the larger seq wins
  if one is already pending -- commits are ordered by the commit lock, but `max`
  keeps it order-independent). Called under the commit lock after seq assignment."
  [registry keys seq]
  (swap! (:pending registry)
         (fn [m] (reduce (fn [m k] (assoc m k (max (long (get m k Long/MIN_VALUE))
                                                   (long seq))))
                         m keys))))

(defn clear-pending!
  "Drop each of `keys` whose pending seq is still exactly `seq`. Called by the
  worker after it has applied this tx's batch to the memtable, so the read path
  already sees the write. The equality guard is essential: a newer in-flight
  commit may have raised the entry to a higher seq, and that must survive until
  its own apply -- never evict a newer pending seq."
  [registry keys seq]
  (swap! (:pending registry)
         (fn [m] (reduce (fn [m k] (if (= (get m k) seq) (dissoc m k) m))
                         m keys))))

(defn pending-seq
  "The in-flight committed seq for user_key `k`, or nil. Read under the commit
  lock as part of conflict detection."
  [registry k]
  (get @(:pending registry) k))
