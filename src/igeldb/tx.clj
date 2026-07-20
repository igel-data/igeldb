(ns igeldb.tx)

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
;;
;; `min-active-snapshot-seq` is the MVCC GC floor (Point 3): the oldest snapshot a
;; live tx can see. Compaction (Step 6) keeps, per user_key, the newest version
;; <= this floor plus every newer version, so it never drops a version a live tx
;; still needs.

(defrecord TxRegistry [seq-counter active])

(defn create-registry
  "Create the global tx registry. `init-seq` seeds the commit-order counter (the
  max seq recovered from WAL/manifest on startup; 0 for a fresh store)."
  ([] (create-registry 0))
  ([init-seq]
   (->TxRegistry (atom init-seq) (atom (sorted-map)))))

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
  "Reset the commit-order counter to `n` (recovery seeds it to the max replayed
  seq so later writes get strictly higher seqs). Only called during init."
  [registry n]
  (reset! (:seq-counter registry) n))

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
