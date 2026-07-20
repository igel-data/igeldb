(ns igeldb.tx-test
  (:require [clojure.test :refer [deftest is testing]]
            [igeldb.tx :as tx]))

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
