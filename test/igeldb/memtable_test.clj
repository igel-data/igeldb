(ns igeldb.memtable-test
  (:require [clojure.test :refer [deftest is testing]]
            [igeldb.data :as data]
            [igeldb.memtable :as memtable]
            [igeldb.store :as store]))

(defn- ->bytes [^String s] (.getBytes s))

(defn- fresh-store []
  (memtable/->MemStore (atom (sorted-map-by (data/byte-array-comparator)))))

(defn- scan-keys [ms from to]
  (mapv (comp #(String. ^bytes %) first)
        (store/scan ms (->bytes from) (->bytes to))))

;; ---- new capability: lock-free concurrent reads while writing -------------

(deftest concurrent-lock-free-reads-test
  ;; The memtable store is now an immutable sorted map behind an atom, so readers
  ;; never lock and never see a torn map -- no ConcurrentModificationException
  ;; even while a writer keeps swapping in new snapshots.
  (let [ms (fresh-store)
        stop (atom false)
        errors (atom [])]
    (store/write-batch! ms (for [i (range 100)]
                             [(->bytes (format "k%04d" i))
                              (data/new-data (->bytes (str i)))]))
    (let [readers (doall
                   (for [_ (range 4)]
                     (future
                       (try
                         (while (not @stop)
                           (doall (store/scan ms (->bytes "k0000") (->bytes "k9999")))
                           (dotimes [i 100]
                             (store/select ms (->bytes (format "k%04d" i)))))
                         (catch Throwable e (swap! errors conj e))))))]
      (dotimes [n 3000]
        (store/write-data! ms
                           (->bytes (format "k%04d" (mod n 100)))
                           (data/new-data (->bytes (str n)))))
      (reset! stop true)
      (doseq [r readers] @r))
    (is (empty? @errors)
        (str "lock-free reads threw during concurrent writes: " (first @errors)))))

;; ---- scan boundary exactness (from-inclusive, to-exclusive) ---------------

(deftest scan-boundary-exactness-test
  (let [ms (fresh-store)]
    (doseq [k ["a" "b" "c" "d" "e"]]
      (store/write-data! ms (->bytes k) (data/new-data (->bytes k))))
    (testing "boundaries present in the map: from inclusive, to exclusive"
      (is (= ["b" "c"] (scan-keys ms "b" "d")))
      (is (= ["c" "d" "e"] (scan-keys ms "c" "z")))
      (is (= ["a" "b" "c" "d" "e"] (scan-keys ms "a" "z"))))
    (testing "empty ranges"
      (is (= [] (scan-keys ms "b" "b")) "from == to is empty")
      (is (= [] (scan-keys ms "x" "z")) "range past all keys"))
    (testing "a single-key range excludes the upper bound"
      (is (= ["a"] (scan-keys ms "a" "b"))))))
