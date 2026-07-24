(ns igeldb.transaction-test
  (:require [clojure.java.io :as jio]
            [clojure.test :refer [deftest is testing]]
            [clj-yaml.core :as yaml]
            [igeldb.core :as igel]
            [igeldb.store :as store]
            [igeldb.tx :as tx]))

(defn- ->bytes [^String s] (.getBytes s))
(defn- s= [expected ^bytes actual]
  (and actual (= expected (String. actual))))

(defn- delete-recursively!
  [file]
  (when (.isDirectory file)
    (doseq [c (.listFiles file)] (delete-recursively! c)))
  (.delete file))

(defn- rm-rf [dir] (delete-recursively! (jio/file dir)))

(defn- config-path!
  [data-dir]
  (rm-rf data-dir)
  (.mkdirs (jio/file data-dir))
  (let [config {:sstable-dir (str data-dir "/sstable")
                :wal-dir (str data-dir "/wal")
                :memtable-size (* 8 1024 1024) ;; large: keep it all in one memtable
                :sync-window-time 10}
        path (str data-dir "/config.yaml")]
    (with-open [w (jio/writer path)]
      (.write w (yaml/generate-string config)))
    path))

(defmacro with-store [[sym dir] & body]
  `(let [~sym (igel/gen-kvs (config-path! ~dir))]
     (try ~@body
          (finally (igel/close! ~sym) (rm-rf ~dir)))))

;; ---- with-tx commit / rollback -------------------------------------------

(deftest with-tx-commits-on-success-test
  (with-store [kvs "./test-data/tx-commit"]
    (let [ret (igel/with-tx [tx kvs]
                (igel/tx-put tx (->bytes "a") (->bytes "1"))
                (igel/tx-put tx (->bytes "b") (->bytes "2"))
                :ok)]
      (is (= :ok ret) "with-tx returns the body value on success")
      (is (s= "1" (igel/select kvs (->bytes "a"))))
      (is (s= "2" (igel/select kvs (->bytes "b")))))))

(deftest with-tx-rolls-back-on-exception-test
  (with-store [kvs "./test-data/tx-rollback"]
    (igel/write! kvs (->bytes "a") (->bytes "orig"))
    (is (thrown? RuntimeException
                 (igel/with-tx [tx kvs]
                   (igel/tx-put tx (->bytes "a") (->bytes "changed"))
                   (igel/tx-put tx (->bytes "new") (->bytes "x"))
                   (throw (RuntimeException. "boom")))))
    (testing "nothing the tx buffered was committed"
      (is (s= "orig" (igel/select kvs (->bytes "a"))))
      (is (nil? (igel/select kvs (->bytes "new")))))))

(deftest read-only-tx-is-a-commit-no-op-test
  (with-store [kvs "./test-data/tx-readonly"]
    (igel/write! kvs (->bytes "k") (->bytes "v"))
    (let [before (tx/current-seq (:registry kvs))
          seen (igel/with-tx [tx kvs] (igel/tx-get tx (->bytes "k")))]
      (is (s= "v" seen) "a read-only tx reads the committed value")
      (is (= before (tx/current-seq (:registry kvs)))
          "a read-only tx assigns no seq (no WAL record, free commit)"))))

;; ---- finished transaction handles ----------------------------------------

(defn- tx-closed-ex
  [f]
  (try
    (f)
    nil
    (catch clojure.lang.ExceptionInfo e
      (when (:igeldb/tx-closed (ex-data e)) e))))

(deftest tx-operations-reject-finished-handles-test
  (with-store [kvs "./test-data/tx-finished"]
    (doseq [[end-name finish!] [[:commit igel/commit-tx]
                                [:rollback igel/rollback-tx]]]
      (let [tx (igel/begin-tx kvs)
            k (->bytes (name end-name))
            v (->bytes "v")]
        (finish! tx)
        (doseq [[op-name op] [[:tx-get #(igel/tx-get tx k)]
                              [:tx-put #(igel/tx-put tx k v)]
                              [:tx-delete #(igel/tx-delete tx k)]
                              [:commit-tx #(igel/commit-tx tx)]
                              [:rollback-tx #(igel/rollback-tx tx)]]]
          (testing (str (name op-name) " after " (name end-name))
            (let [ex (tx-closed-ex op)]
              (is (some? ex))
              (is (= op-name (:op (ex-data ex))))
              (is (false? (:retriable (ex-data ex)))))))))))

;; ---- atomicity + one-seq-per-tx ------------------------------------------

(deftest multi-key-tx-is-atomic-and-shares-one-seq-test
  (with-store [kvs "./test-data/tx-atomic"]
    (igel/with-tx [tx kvs]
      (igel/tx-put tx (->bytes "k1") (->bytes "a"))
      (igel/tx-put tx (->bytes "k2") (->bytes "b"))
      (igel/tx-put tx (->bytes "k3") (->bytes "c")))
    (testing "every key of the committed tx is visible"
      (is (s= "a" (igel/select kvs (->bytes "k1"))))
      (is (s= "b" (igel/select kvs (->bytes "k2"))))
      (is (s= "c" (igel/select kvs (->bytes "k3")))))
    (testing "1 tx = 1 seq: all keys carry the same commit seq"
      (let [seqs (mapv #(store/latest-seq kvs (->bytes %)) ["k1" "k2" "k3"])]
        (is (apply = seqs) (str "expected one shared seq, got " seqs))))))

;; ---- read-your-own-writes -------------------------------------------------

(deftest read-your-own-writes-within-tx-test
  (with-store [kvs "./test-data/tx-ryow"]
    (igel/write! kvs (->bytes "k") (->bytes "committed"))
    (igel/with-tx [tx kvs]
      (is (s= "committed" (igel/tx-get tx (->bytes "k"))) "reads committed value first")
      (igel/tx-put tx (->bytes "k") (->bytes "buffered"))
      (is (s= "buffered" (igel/tx-get tx (->bytes "k"))) "then its own buffered write")
      (igel/tx-delete tx (->bytes "k"))
      (is (nil? (igel/tx-get tx (->bytes "k"))) "then its own tombstone"))))

;; ---- snapshot isolation ---------------------------------------------------

(deftest tx-reads-a-stable-snapshot-test
  (with-store [kvs "./test-data/tx-snapshot"]
    (igel/write! kvs (->bytes "k") (->bytes "v0"))
    (let [tx (igel/begin-tx kvs)]
      (is (s= "v0" (igel/tx-get tx (->bytes "k"))))
      ;; a concurrent commit advances k after the tx's snapshot
      (igel/write! kvs (->bytes "k") (->bytes "v1"))
      (is (s= "v0" (igel/tx-get tx (->bytes "k")))
          "the tx keeps seeing its snapshot, not the newer commit")
      (igel/rollback-tx tx))
    (testing "a fresh tx sees the latest committed value"
      (is (s= "v1" (igel/with-tx [tx kvs] (igel/tx-get tx (->bytes "k"))))))))

;; ---- write-write conflict (first-committer-wins) --------------------------

(deftest conflicting-txs-first-committer-wins-test
  (with-store [kvs "./test-data/tx-conflict"]
    (igel/write! kvs (->bytes "k") (->bytes "v0"))
    (let [t1 (igel/begin-tx kvs)
          t2 (igel/begin-tx kvs)]
      ;; both read k at the same snapshot, both intend to write it
      (igel/tx-get t1 (->bytes "k"))
      (igel/tx-get t2 (->bytes "k"))
      (igel/tx-put t1 (->bytes "k") (->bytes "t1"))
      (igel/tx-put t2 (->bytes "k") (->bytes "t2"))
      (is (= :committed (igel/commit-tx t1)) "first committer wins")
      (let [ex (try (igel/commit-tx t2) nil
                    (catch clojure.lang.ExceptionInfo e e))]
        (is (some? ex) "second committer conflicts")
        (is (:igeldb/conflict (ex-data ex)) "conflict is tagged for the caller"))
      (is (s= "t1" (igel/select kvs (->bytes "k"))) "t1's write stands, t2's is dropped"))))

(deftest with-tx-conflict-propagates-and-retry-succeeds-test
  (with-store [kvs "./test-data/tx-conflict-retry"]
    (igel/write! kvs (->bytes "k") (->bytes "0"))
    ;; Open a stale tx, then let another commit advance k so the stale tx conflicts.
    (let [stale (igel/begin-tx kvs)]
      (igel/tx-get stale (->bytes "k"))
      (igel/write! kvs (->bytes "k") (->bytes "1")) ;; k advances past stale's snapshot
      (igel/tx-put stale (->bytes "k") (->bytes "2"))
      (is (thrown? clojure.lang.ExceptionInfo (igel/commit-tx stale))
          "the stale tx's commit conflicts and throws"))
    ;; a fresh tx (new snapshot) succeeds -- demonstrating a caller-driven retry
    (let [ret (try
                (igel/with-tx [tx kvs]
                  (igel/tx-put tx (->bytes "k") (->bytes "3"))
                  :done)
                (catch clojure.lang.ExceptionInfo e
                  (when-not (:igeldb/conflict (ex-data e)) (throw e))
                  :conflicted))]
      (is (= :done ret))
      (is (s= "3" (igel/select kvs (->bytes "k")))))))

(deftest with-tx-preserves-commit-conflict-test
  (with-store [kvs "./test-data/with-tx-conflict"]
    (igel/write! kvs (->bytes "k") (->bytes "0"))
    (let [ex (try
               (igel/with-tx [tx kvs]
                 (igel/tx-get tx (->bytes "k"))
                 (igel/write! kvs (->bytes "k") (->bytes "1"))
                 (igel/tx-put tx (->bytes "k") (->bytes "2")))
               nil
               (catch clojure.lang.ExceptionInfo e e))]
      (is (:igeldb/conflict (ex-data ex))
          "with-tx propagates the original conflict")
      (is (not (:igeldb/tx-closed (ex-data ex)))
          "cleanup does not replace the conflict with a finished-tx error"))))

;; ---- the snapshot leaves the active set on end ----------------------------

(deftest committing-or-rolling-back-frees-the-gc-floor-test
  (with-store [kvs "./test-data/tx-floor"]
    (dotimes [i 5] (igel/write! kvs (->bytes (str "k" i)) (->bytes "v")))
    (let [registry (:registry kvs)
          top (tx/current-seq registry)
          t1 (igel/begin-tx kvs)]
      (is (= (:snapshot-seq t1) (tx/min-active-snapshot-seq registry))
          "an open tx pins the floor at its snapshot")
      (igel/commit-tx t1) ;; empty write-set -> read-only commit
      (is (= top (tx/min-active-snapshot-seq registry))
          "committing frees the floor back to the current seq")
      (let [t2 (igel/begin-tx kvs)]
        (igel/rollback-tx t2)
        (is (= top (tx/min-active-snapshot-seq registry))
            "rolling back also frees the floor")))))
