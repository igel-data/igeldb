(ns igeldb.tx-test
  (:require [clojure.test :refer [deftest is testing]]
            [igeldb.tx :as tx]))

(defn- ->bytes [^String s] (.getBytes s))

;; ---- Step 3: active-tx tracking + min-active-snapshot-seq ------------------

(deftest current-and-next-seq-test
  (let [r (tx/create-registry)]
    (is (= 0 (tx/current-seq r)) "fresh store starts at 0")
    (is (= 1 (tx/next-seq! r)) "next-seq assigns and returns the incremented value")
    (is (= 2 (tx/next-seq! r)))
    (is (= 2 (tx/current-seq r)) "current-seq is the highest assigned")
    (tx/seed-seq! r 100)
    (is (= 100 (tx/current-seq r)) "seed-seq resets the counter (recovery)")))

(deftest min-with-no-active-tx-is-current-seq-test
  (let [r (tx/create-registry)]
    (is (= 0 (tx/min-active-snapshot-seq r)) "no tx active -> current seq (0)")
    (dotimes [_ 5] (tx/next-seq! r))
    (is (= 5 (tx/min-active-snapshot-seq r))
        "no tx active -> tracks the advancing current seq")))

(deftest min-tracks-overlapping-begins-and-ends-test
  (let [r (tx/create-registry)]
    (dotimes [_ 20] (tx/next-seq! r)) ;; current seq = 20
    (testing "one active tx -> its snapshot is the floor"
      (tx/register-snapshot! r 5)
      (is (= 5 (tx/min-active-snapshot-seq r))))
    (testing "a second, newer tx does not lower the floor"
      (tx/register-snapshot! r 8)
      (is (= 5 (tx/min-active-snapshot-seq r)) "min stays at the oldest snapshot"))
    (testing "ending the oldest raises the floor to the next-oldest"
      (tx/deregister-snapshot! r 5)
      (is (= 8 (tx/min-active-snapshot-seq r))))
    (testing "ending the last active tx falls back to the current seq"
      (tx/deregister-snapshot! r 8)
      (is (= 20 (tx/min-active-snapshot-seq r))))))

(deftest min-floor-never-exceeds-current-seq-test
  ;; A snapshot seq is always <= current-seq; the floor must respect that even if
  ;; a stale/oldest snapshot lingers while the counter keeps advancing.
  (let [r (tx/create-registry)]
    (dotimes [_ 10] (tx/next-seq! r))
    (tx/register-snapshot! r 3)
    (dotimes [_ 10] (tx/next-seq! r)) ;; current seq = 20, oldest snapshot still 3
    (is (= 3 (tx/min-active-snapshot-seq r))
        "the long-lived snapshot pins the floor below the advancing counter")))

(deftest multiset-counts-duplicate-snapshots-test
  ;; Concurrent begins can pin the SAME snapshot seq; each must be removed
  ;; independently (a plain set would drop the seq on the first end).
  (let [r (tx/create-registry)]
    (dotimes [_ 30] (tx/next-seq! r)) ;; current seq = 30
    (tx/register-snapshot! r 7)
    (tx/register-snapshot! r 7)
    (is (= 7 (tx/min-active-snapshot-seq r)))
    (tx/deregister-snapshot! r 7)
    (is (= 7 (tx/min-active-snapshot-seq r))
        "one of two txs at seq 7 ended; the other still pins the floor")
    (tx/deregister-snapshot! r 7)
    (is (= 30 (tx/min-active-snapshot-seq r))
        "both ended -> back to the current seq")))

(deftest concurrent-register-deregister-is-consistent-test
  ;; The multiset must stay balanced under concurrent begin/end churn: after every
  ;; registered snapshot is matched by a deregister, the active set is empty and
  ;; the floor is the current seq again.
  (let [r (tx/create-registry)]
    (dotimes [_ 100] (tx/next-seq! r)) ;; current seq = 100
    (let [workers (doall
                   (for [_ (range 8)]
                     (future
                       (dotimes [_ 500]
                         (let [s (inc (rand-int 100))]
                           (tx/register-snapshot! r s)
                           (tx/deregister-snapshot! r s))))))]
      (doseq [w workers] @w))
    (is (empty? @(:active r))
        "all registrations were matched -- the active multiset is empty")
    (is (= 100 (tx/min-active-snapshot-seq r))
        "no lingering active tx -> floor is the current seq")))

;; ---- Bug 1: the pending (in-flight commit) map ----------------------------

(deftest pending-record-and-clear-test
  (let [r (tx/create-registry)
        a (->bytes "a")
        b (->bytes "b")]
    (is (nil? (tx/pending-seq r a)) "absent -> nil")
    (tx/record-pending! r [a b] 5)
    (is (= 5 (tx/pending-seq r a)))
    (is (= 5 (tx/pending-seq r b)))
    (testing "record keeps the larger seq (order-independent max)"
      (tx/record-pending! r [a] 3)
      (is (= 5 (tx/pending-seq r a)) "a smaller seq does not lower the entry")
      (tx/record-pending! r [a] 9)
      (is (= 9 (tx/pending-seq r a))))
    (testing "clear only removes an entry still at exactly that seq"
      (tx/clear-pending! r [a] 5)
      (is (= 9 (tx/pending-seq r a)) "a newer in-flight seq (9) survives clear(5)")
      (tx/clear-pending! r [a] 9)
      (is (nil? (tx/pending-seq r a)) "clearing the current seq removes it")
      (tx/clear-pending! r [b] 5)
      (is (nil? (tx/pending-seq r b))))))

(deftest pending-keyed-by-value-not-identity-test
  ;; byte arrays don't hash/= by value; the pending map must still match a
  ;; distinct byte[] with the same contents.
  (let [r (tx/create-registry)]
    (tx/record-pending! r [(->bytes "k")] 7)
    (is (= 7 (tx/pending-seq r (->bytes "k")))
        "a different byte[] with the same content resolves")))

;; ---- Bug 2: the applied high-water mark -----------------------------------

(deftest applied-monotonic-test
  (let [r (tx/create-registry)]
    (is (= 0 (tx/current-applied r)))
    (tx/mark-applied! r 5)
    (is (= 5 (tx/current-applied r)))
    (testing "monotonic: a smaller seq never lowers it (out-of-order apply-marks)"
      (tx/mark-applied! r 3)
      (is (= 5 (tx/current-applied r)))
      (tx/mark-applied! r 8)
      (is (= 8 (tx/current-applied r))))
    (testing "seed-seq! seeds both counter and applied (recovery)"
      (tx/seed-seq! r 100)
      (is (= 100 (tx/current-seq r)))
      (is (= 100 (tx/current-applied r))))))
