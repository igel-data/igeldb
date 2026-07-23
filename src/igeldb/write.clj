(ns igeldb.write)

(defn- await-l0-capacity!
  "Back-pressure: block before enqueueing a write while L0 holds at least
  `l0-stall-threshold` tables, until compaction drains it below the threshold or
  the store is poisoned. The compaction worker notifies `monitor` after each
  compaction; a watch on `poison` notifies it on fail-stop, so a poisoned store
  wakes stalled writers rather than deadlocking. The timed wait is a safety net
  against a missed notification. Returns when unblocked; the caller re-checks
  `poison` and surfaces the error."
  [^Object monitor current-version poison threshold]
  (locking monitor
    (loop []
      (when (and (nil? @poison)
                 (>= (count (first @current-version)) threshold))
        (.wait monitor 200)
        (recur)))))

(defn- unusable-ex
  "Build the rejection exception for an unusable store. `poisoned` is either the
  `:closed` marker (clean shutdown) or the fatal `Throwable` (an IO fault, kept as
  the cause -- but only a Throwable can be an `ex-info` cause, hence the split)."
  [poisoned op]
  (if (= poisoned :closed)
    (ex-info (str (name op) " rejected: the store is closed") {:retriable false})
    (ex-info (str (name op) " rejected: the store is poisoned")
             {:retriable false} poisoned)))

(defn run-mutation!
  "Run a memtable mutation, applying the settled error classes:

  - Unusable store (fail-stop): if the store has been poisoned by an IO error, or
    closed via `close!`, reject the mutation immediately -- and also if it becomes
    unusable mid-flight. `poison` holds the fatal `Throwable` (a fault) or the
    `:closed` marker; a fault is preserved as the exception cause.
  - Class C (retriable): a collision with a memtable switch throws
    {:retriable true}; retry up to `write-retries` times, then give up. The last
    retriable failure is preserved as the cause.

  Non-retriable exceptions propagate unchanged. Returns `mutate!`'s value on
  success (nil for auto-commit writes; the commit outcome for a tx commit). `op` is
  :write / :delete / :commit-tx, used only for the message."
  [coordinator tree config op mutate!]
  (let [poison (:poison coordinator)
        monitor (:stall-monitor coordinator)
        threshold (:l0-stall-threshold config)]
    (loop [retries (:write-retries config)]
      (when-let [p @poison]
        (throw (unusable-ex p op)))
      ;; Stall before enqueueing while L0 is saturated (woken by a compaction, or
      ;; by the store becoming unusable -- poisoned or closed).
      (await-l0-capacity! monitor (:current-version tree) poison threshold)
      (when-let [p @poison]
        (throw (unusable-ex p op)))
      (let [outcome (try
                      {:ok (mutate!)}
                      (catch clojure.lang.ExceptionInfo e
                        (cond
                          ;; a closed wal-chan looks like a retriable memtable
                          ;; switch; if the store is actually unusable, surface that
                          @poison (throw (unusable-ex @poison op))
                          (-> e ex-data :retriable) {:err e}
                          :else (throw e))))]
        (if (contains? outcome :ok)
          (:ok outcome)
          (if (pos? retries)
            (do (Thread/sleep 100)
                (recur (dec retries)))
            (throw (ex-info (str (name op) " failed repeatedly")
                            {:retriable false} (:err outcome)))))))))
