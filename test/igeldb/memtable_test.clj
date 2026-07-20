(ns igeldb.memtable-test
  (:require [clojure.test :refer [deftest is testing]]
            [igeldb.data :as data]
            [igeldb.memtable :as memtable]
            [igeldb.store :as store]))

(def ^:private latest Long/MAX_VALUE)

(defn- ->bytes [^String s] (.getBytes s))

(defn- fresh-store []
  (memtable/->MemStore (atom (sorted-map-by (data/internal-key-comparator)))))

(defn- put! [ms k seq v]
  (store/write-batch! ms [[(data/->ikey (->bytes k) seq) (data/new-data (->bytes v))]]))

(defn- scan-keys
  ([ms from to] (scan-keys ms from to latest))
  ([ms from to snapshot]
   (mapv (comp #(String. ^bytes %) first)
         (store/scan ms (->bytes from) (->bytes to) snapshot))))

;; ---- lock-free concurrent reads while writing -----------------------------

(deftest concurrent-lock-free-reads-test
  ;; The memtable store is an immutable sorted map behind an atom, so readers
  ;; never lock and never see a torn map -- no ConcurrentModificationException
  ;; even while a writer keeps swapping in new versions.
  (let [ms (fresh-store)
        seq (atom 0)
        stop (atom false)
        errors (atom [])]
    (store/write-batch! ms (for [i (range 100)]
                             [(data/->ikey (->bytes (format "k%04d" i)) (swap! seq inc))
                              (data/new-data (->bytes (str i)))]))
    (let [readers (doall
                   (for [_ (range 4)]
                     (future
                       (try
                         (while (not @stop)
                           (doall (store/scan ms (->bytes "k0000") (->bytes "k9999") latest))
                           (dotimes [i 100]
                             (store/select ms (->bytes (format "k%04d" i)) latest)))
                         (catch Throwable e (swap! errors conj e))))))]
      (dotimes [n 3000]
        (put! ms (format "k%04d" (mod n 100)) (swap! seq inc) (str n)))
      (reset! stop true)
      (doseq [r readers] @r))
    (is (empty? @errors)
        (str "lock-free reads threw during concurrent writes: " (first @errors)))))

;; ---- multi-version reads at a snapshot seq --------------------------------

(deftest snapshot-visibility-test
  (let [ms (fresh-store)]
    ;; three versions of "k": seq 5 = v5, seq 8 = v8, seq 11 = tombstone
    (put! ms "k" 5 "v5")
    (put! ms "k" 8 "v8")
    (store/write-batch! ms [[(data/->ikey (->bytes "k") 11) (data/deleted-data)]])
    (testing "each snapshot sees the newest version <= its seq"
      (is (nil? (store/select ms (->bytes "k") 4)) "before any version")
      (is (= "v5" (String. (:value (store/select ms (->bytes "k") 6)))))
      (is (= "v8" (String. (:value (store/select ms (->bytes "k") 8))))
          "boundary seq is inclusive")
      (is (= "v8" (String. (:value (store/select ms (->bytes "k") 10)))))
      (is (:deleted? (store/select ms (->bytes "k") 11)) "tombstone visible at its seq")
      (is (:deleted? (store/select ms (->bytes "k") latest))))))

;; ---- scan boundary exactness (from-inclusive, to-exclusive) ---------------

(deftest scan-boundary-exactness-test
  (let [ms (fresh-store)]
    (doseq [[i k] (map-indexed vector ["a" "b" "c" "d" "e"])]
      (put! ms k (inc i) k))
    (testing "boundaries present in the map: from inclusive, to exclusive"
      (is (= ["b" "c"] (scan-keys ms "b" "d")))
      (is (= ["c" "d" "e"] (scan-keys ms "c" "z")))
      (is (= ["a" "b" "c" "d" "e"] (scan-keys ms "a" "z"))))
    (testing "empty ranges"
      (is (= [] (scan-keys ms "b" "b")) "from == to is empty")
      (is (= [] (scan-keys ms "x" "z")) "range past all keys"))
    (testing "a single-key range excludes the upper bound"
      (is (= ["a"] (scan-keys ms "a" "b"))))))
