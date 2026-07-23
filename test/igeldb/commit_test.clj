(ns igeldb.commit-test
  (:require [clojure.java.io :as jio]
            [clojure.test :refer [deftest is testing]]
            [clj-yaml.core :as yaml]
            [igeldb.commit :as commit]
            [igeldb.core :as igel]
            [igeldb.data :as data]
            [igeldb.io :as io]
            [igeldb.store :as store]
            [igeldb.tx :as tx]
            [igeldb.wal :as wal])
  (:import (java.io FileOutputStream BufferedOutputStream)))

(defn- ->bytes [^String s] (.getBytes s))
(defn- b= [a b] (data/byte-array-equals? a b))

(defn- delete-recursively!
  [file]
  (when (.isDirectory file)
    (doseq [c (.listFiles file)] (delete-recursively! c)))
  (.delete file))

(defn- rm-rf [dir] (delete-recursively! (jio/file dir)))

(defn- config-path!
  [data-dir memtable-size]
  (rm-rf data-dir)
  (.mkdirs (jio/file data-dir))
  (let [config {:sstable-dir (str data-dir "/sstable")
                :wal-dir (str data-dir "/wal")
                :memtable-size memtable-size
                :sync-window-time 10}
        path (str data-dir "/config.yaml")]
    (with-open [w (jio/writer path)]
      (.write w (yaml/generate-string config)))
    path))

(defn- wait-until
  [pred]
  (let [deadline (+ (System/currentTimeMillis) 15000)]
    (loop []
      (cond
        (pred) true
        (> (System/currentTimeMillis) deadline) false
        :else (do (Thread/sleep 20) (recur))))))

(def ^:private BIG (* 8 1024 1024)) ;; large enough that no flush happens

;; ---- Step 4: write-write conflict detection (first-committer-wins) ---------

(deftest write-write-conflict-first-committer-wins-test
  (let [dir "./test-data/commit-conflict"
        kvs (igel/gen-kvs (config-path! dir BIG))
        registry (:registry kvs)]
    ;; baseline commit of k; a tx then pins its snapshot right after it
    (igel/write! kvs (->bytes "k") (->bytes "v0"))
    (let [snap (tx/current-seq registry)]
      ;; a concurrent committer advances k past the snapshot
      (igel/write! kvs (->bytes "k") (->bytes "v1"))
      (testing "a tx whose write-set key was committed after its snapshot conflicts"
        (is (= :conflict
               (commit/commit! kvs snap
                               [[(->bytes "k") (data/new-data (->bytes "v2"))]]))))
      (testing "the aborted tx changed nothing"
        (is (b= (->bytes "v1") (igel/select kvs (->bytes "k")))))
      (testing "a tx at the same snapshot writing an untouched key commits"
        (is (= :committed
               (commit/commit! kvs snap
                               [[(->bytes "other") (data/new-data (->bytes "x"))]])))
        (is (b= (->bytes "x") (igel/select kvs (->bytes "other")))))
      (testing "a tx pinned at the current seq (nothing newer) commits"
        (let [snap2 (tx/current-seq registry)]
          (is (= :committed
                 (commit/commit! kvs snap2
                                 [[(->bytes "k") (data/new-data (->bytes "v3"))]])))
          (is (b= (->bytes "v3") (igel/select kvs (->bytes "k")))))))
    (igel/close! kvs)
    (rm-rf dir)))

(deftest blind-write-conflict-detected-test
  ;; A blind write (the tx never read the key) is still conflict-checked: at commit
  ;; the key is looked up via the read path. Here the newer version is a plain
  ;; in-memtable commit; the tx that never saw it must still abort.
  (let [dir "./test-data/commit-blind"
        kvs (igel/gen-kvs (config-path! dir BIG))
        registry (:registry kvs)]
    (igel/write! kvs (->bytes "blind") (->bytes "v0"))
    (let [snap (tx/current-seq registry)]
      (igel/write! kvs (->bytes "blind") (->bytes "v1")) ;; someone else won
      (is (= :conflict
             (commit/commit! kvs snap
                             [[(->bytes "blind") (data/new-data (->bytes "v2"))]]))
          "first-committer-wins covers blind writes (no read-set needed)"))
    (igel/close! kvs)
    (rm-rf dir)))

(deftest conflict-lookup-reaches-sstable-test
  ;; The blind-write lookup must find a key's latest seq even once it has been
  ;; flushed to an SSTable (io/read-latest-seq + TreeStore/latest-seq path).
  (let [dir "./test-data/commit-sstable-lookup"
        kvs (igel/gen-kvs (config-path! dir 256)) ;; tiny memtable -> flushes
        registry (:registry kvs)]
    (igel/write! kvs (->bytes "hot") (->bytes "v"))
    (let [hot-seq (tx/current-seq registry)]
      ;; push "hot" out to an SSTable with a flood of unrelated writes
      (dotimes [i 400]
        (igel/write! kvs (->bytes (format "f%05d" i)) (->bytes "x")))
      (is (wait-until #(nil? (store/latest-seq @(:memtable kvs) (->bytes "hot"))))
          "hot left the mutable memtable")
      (is (= hot-seq (store/latest-seq (:tree kvs) (->bytes "hot")))
          "hot's latest seq is discoverable from the SSTable")
      (is (= :conflict
             (commit/commit! kvs (dec hot-seq)
                             [[(->bytes "hot") (data/new-data (->bytes "v2"))]]))
          "a tx older than the flushed version conflicts via the tree lookup"))
    (igel/close! kvs)
    (rm-rf dir)))

;; ---- Step 4: WAL order == seq order under concurrent commits --------------

(defn- read-all-wal-seqs
  "Read every WAL record's seq from a WAL file, in file order."
  [path]
  (with-open [in (jio/input-stream path)]
    (loop [acc []]
      (let [r (io/read-wal-record! in)]
        (if (vector? r) (recur (conj acc (first r))) acc)))))

(deftest wal-order-equals-seq-order-test
  ;; The commit lock assigns seq and enqueues in one region, so enqueue order ==
  ;; seq order and the worker's FIFO gives WAL order == seq order -- even under
  ;; heavy concurrency. A large memtable keeps everything in one WAL file (0.wal).
  (let [dir "./test-data/commit-wal-order"
        kvs (igel/gen-kvs (config-path! dir BIG))
        n-threads 16
        per 50
        total (* n-threads per)]
    (let [fs (doall
              (for [t (range n-threads)]
                (future
                  (dotimes [i per]
                    (igel/write! kvs (->bytes (str "k-" t "-" i)) (->bytes "v"))))))]
      (doseq [f fs] @f))
    (let [seqs (read-all-wal-seqs (str dir "/wal/0.wal"))]
      (is (= total (count seqs)) "every commit wrote exactly one WAL record")
      (is (apply < seqs) "WAL record order is strictly increasing seq order"))
    (igel/close! kvs)
    (rm-rf dir)))

;; ---- Step 4: a tx is all-or-nothing across a truncated tail record --------

(defn- ikey-entry [k seq v]
  [(data/->ikey (->bytes k) seq)
   (if v (data/new-data (->bytes v)) (data/deleted-data))])

(deftest tx-all-or-nothing-across-truncated-tail-test
  ;; WAL v2: atomicity == record completeness. A torn tail record (a tx that died
  ;; before its fsync) is discarded WHOLE on replay -- none of its entries appear
  ;; -- while every fully-written earlier tx survives.
  (let [dir "./test-data/commit-atomic-tail"
        wal-dir (str dir "/wal")]
    (rm-rf dir)
    (io/make-dir wal-dir)
    (let [path (str wal-dir "/0.wal")]
      (with-open [fs (FileOutputStream. path)
                  os (BufferedOutputStream. fs)]
        ;; tx1: two entries (a full, committed record)
        (io/write-wal-record! os 1 [(ikey-entry "a1" 1 "x") (ikey-entry "a2" 1 "y")])
        ;; tx2: three entries (about to be torn)
        (io/write-wal-record! os 2 [(ikey-entry "b1" 2 "p")
                                    (ikey-entry "b2" 2 "q")
                                    (ikey-entry "b3" 2 "r")])
        (.flush os)
        (-> fs .getChannel (.force true)))
      ;; cut a few bytes off tx2's tail -> its record frame is incomplete
      (let [len (.length (jio/file path))]
        (with-open [raf (java.io.RandomAccessFile. path "rw")]
          (.setLength raf (- len 5)))))
    (let [[wal-id pairs] (wal/load-existing-wal {:wal-dir wal-dir})]
      (is (= 0 wal-id))
      (is (= 2 (count pairs)) "only tx1's two entries survive; tx2 is dropped whole")
      (is (= ["a1" "a2"] (mapv (comp #(String. ^bytes %) :user-key first) pairs))
          "no partial entry from the torn tx2 leaks in"))
    (rm-rf dir)))

;; ---- Bug 1 + Bug 2: no lost updates under concurrent read-modify-write ----
;;
;; The scenario jepsen-lite's bank/counter workloads surfaced. Both fixes are
;; needed and this exercises both: `pending` (a committing tx sees an in-flight
;; first-committer) and applied-seq snapshots (a tx never pins a snapshot ahead of
;; what the read path can serve, so it never computes on stale data).

(deftest no-lost-updates-under-concurrent-rmw-test
  (let [dir "./test-data/commit-rmw"
        kvs (igel/gen-kvs (config-path! dir BIG))
        k (->bytes "counter")
        ;; heavy contention on ONE key so a committing tx and the worker's
        ;; apply+clear race constantly -- this is what surfaces both the pending
        ;; gap and the conflict-check read-order bug (the latter was Java-11 CI only
        ;; at a lower op count, so keep this wide).
        threads 16
        per 100
        committed (atom 0)]
    (igel/write! kvs k (->bytes "0"))
    (let [->l (fn [^bytes b] (Long/parseLong (String. b)))
          fs (doall
              (for [_ (range threads)]
                (future
                  (dotimes [_ per]
                    (try
                      (igel/with-tx [tx kvs]
                        (let [cur (->l (igel/tx-get tx k))]
                          (igel/tx-put tx k (->bytes (str (inc cur))))))
                      (swap! committed inc)
                      (catch clojure.lang.ExceptionInfo e
                        ;; a conflict is the correct outcome for a racing RMW
                        (when-not (:igeldb/conflict (ex-data e)) (throw e))))))))]
      (doseq [f fs] @f))
    (let [final (Long/parseLong (String. ^bytes (igel/select kvs k)))]
      ;; every committed increment must be reflected: final == commit count. A lost
      ;; update (a committed tx whose increment vanished) makes final < committed.
      (is (= @committed final)
          (str "lost updates: committed=" @committed " final=" final))
      (is (pos? @committed) "some txs must have committed"))
    (igel/close! kvs)
    (rm-rf dir)))

;; ---- Bug 2: the applied high-water mark --------------------------------------

(deftest applied-seq-never-exceeds-current-and-catches-up-test
  (let [dir "./test-data/commit-applied"
        kvs (igel/gen-kvs (config-path! dir BIG))
        registry (:registry kvs)
        violations (atom 0)]
    ;; hammer writes while sampling the invariant applied <= current
    (let [stop (atom false)
          sampler (future (while (not @stop)
                            (when (> (tx/current-applied registry)
                                     (tx/current-seq registry))
                              (swap! violations inc))))]
      (dotimes [i 500] (igel/write! kvs (->bytes (str "k" i)) (->bytes "v")))
      (reset! stop true)
      @sampler)
    (is (zero? @violations) "applied-seq must never exceed current-seq")
    (testing "once writes quiesce, applied catches up to current"
      (is (wait-until #(= (tx/current-applied registry) (tx/current-seq registry)))
          (str "applied=" (tx/current-applied registry)
               " never reached current=" (tx/current-seq registry))))
    (igel/close! kvs)
    (rm-rf dir)))

(deftest tx-snapshot-pins-to-applied-not-current-test
  ;; A fresh tx must pin its snapshot to the applied high-water mark: it may lag
  ;; current-seq, but tx-get must return exactly the value that snapshot can serve.
  (let [dir "./test-data/commit-pin"
        kvs (igel/gen-kvs (config-path! dir BIG))
        registry (:registry kvs)]
    (igel/write! kvs (->bytes "k") (->bytes "v0"))
    ;; after a committed write, applied has caught up, so a tx sees v0
    (is (wait-until #(= (tx/current-applied registry) (tx/current-seq registry))))
    (igel/with-tx [tx kvs]
      (is (<= (:snapshot-seq tx) (tx/current-seq registry))
          "snapshot never exceeds current-seq")
      (is (= (tx/current-applied registry) (:snapshot-seq tx))
          "snapshot is pinned to the applied high-water mark")
      (is (b= (->bytes "v0") (igel/tx-get tx (->bytes "k")))
          "tx-get returns the value its snapshot can materialize"))
    (igel/close! kvs)
    (rm-rf dir)))
