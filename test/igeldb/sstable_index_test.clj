(ns igeldb.sstable-index-test
  "Phase 4: the SSTable sparse block index. Behaviour must be IDENTICAL to the
  pre-index linear scan -- only the cost changes -- so these tests focus on the
  boundary cases the index introduces: block cuts, straddling keys, scans across
  block boundaries, and footer corruption."
  (:require [clojure.java.io :as jio]
            [clojure.test :refer [deftest is testing]]
            [igeldb.data :as data]
            [igeldb.io :as io]
            [igeldb.sstable :as sstable])
  (:import (java.io RandomAccessFile)))

(defn- ->bytes [^String s] (.getBytes s))
(defn- b= [a b] (data/byte-array-equals? a b))
(defn- s-of [^bytes b] (when b (String. b)))

(defn- delete-recursively!
  [file]
  (when (.isDirectory file)
    (doseq [c (.listFiles file)] (delete-recursively! c)))
  (.delete file))

(defn- rm-rf [dir] (delete-recursively! (jio/file dir)))

(defn- config [dir block-size]
  {:sstable-dir dir :bloom-filter {:size 10240} :sstable-block-size block-size})

(defn- write-table!
  "Write [k-str seq v-str-or-nil] entries (nil = tombstone) as an SSTable via the
  production writer. Entries must be user_key-asc / seq-desc. Returns the table
  entry map (including :index / :index-start) plus :path."
  [dir id block-size entries]
  (let [w (sstable/open-table! id (config dir block-size))]
    (doseq [[k seq v] entries]
      (sstable/write-entry! w (data/->ikey (->bytes k) seq)
                            (if v (data/new-data (->bytes v)) (data/deleted-data))))
    (assoc (sstable/close-table! w 0)
           :path (sstable/get-sstable-path id dir))))

(defn- kv-entries
  "n single-version records key00000..; ~100-byte values."
  [n]
  (mapv (fn [i] [(format "key%05d" i) (inc i) (str "v" i (apply str (repeat 90 \x)))])
        (range n)))

;; ---- floor lookup --------------------------------------------------------

(deftest floor-block-offset-picks-the-largest-block-at-or-below-test
  ;; The index is keyed by user_key; a lookup must land on the block whose first
  ;; user_key is the largest one <= target, then scan forward from there.
  (let [index [[(->bytes "c") 100] [(->bytes "f") 200] [(->bytes "m") 300]]]
    (testing "target before every block -> start at the first entry (offset 1)"
      (is (= 1 (io/floor-block-offset index (->bytes "a")))))
    (testing "exact match on a block's first key -> that block"
      (is (= 100 (io/floor-block-offset index (->bytes "c"))))
      (is (= 300 (io/floor-block-offset index (->bytes "m")))))
    (testing "between blocks -> the preceding block (floor, not ceiling)"
      (is (= 100 (io/floor-block-offset index (->bytes "d"))))
      (is (= 200 (io/floor-block-offset index (->bytes "g")))))
    (testing "past every block -> the last block"
      (is (= 300 (io/floor-block-offset index (->bytes "z")))))
    (testing "empty index -> the first entry"
      (is (= 1 (io/floor-block-offset [] (->bytes "a")))))))

;; ---- many blocks: every key still readable -------------------------------

(deftest many-block-table-reads-every-key-test
  (let [dir "./test-data/sst-index-many"]
    (rm-rf dir)
    (.mkdirs (jio/file dir))
    ;; tiny blocks so the table has many of them
    (let [n 300
          table (write-table! dir 0 256 (kv-entries n))]
      (is (> (count (:index table)) 20)
          (str "expected many blocks, got " (count (:index table))))
      (testing "every key reads back correctly through the index"
        (doseq [i (range n)]
          (let [d (io/read-value (:path table) (->bytes (format "key%05d" i))
                                 Long/MAX_VALUE table)]
            (is (some? d) (str "missing key" i))
            (is (= (str "v" i (apply str (repeat 90 \x))) (s-of (:value d)))))))
      (testing "absent keys return nil (before, between and after the key range)"
        (doseq [k ["aaa" "key00000x" "zzz"]]
          (is (nil? (io/read-value (:path table) (->bytes k) Long/MAX_VALUE table))))))
    (rm-rf dir)))

(deftest single-block-table-reads-correctly-test
  (let [dir "./test-data/sst-index-single"]
    (rm-rf dir)
    (.mkdirs (jio/file dir))
    ;; block size far larger than the data -> exactly one block
    (let [table (write-table! dir 0 (* 1024 1024) (kv-entries 10))]
      (is (= 1 (count (:index table))) "one block")
      (doseq [i (range 10)]
        (is (some? (io/read-value (:path table) (->bytes (format "key%05d" i))
                                  Long/MAX_VALUE table)))))
    (rm-rf dir)))

;; ---- the straddle rule ---------------------------------------------------

(defn- entry-offset
  "Absolute byte offset of entry `n` (0-based); entries start at offset 1, just
  past the format byte."
  [entries n]
  (reduce + 1 (map (fn [[k seq v]]
                     (io/entry-size-on-disk (data/->ikey (->bytes k) seq)
                                            (if v
                                              (data/new-data (->bytes v))
                                              (data/deleted-data))))
                   (take n entries))))

(deftest versions-straddling-a-block-boundary-are-found-test
  ;; The writer only cuts a block when the user_key CHANGES, so it never splits a
  ;; key itself -- but the format permits it and the reader must cope. Here the
  ;; index is built by hand to deliberately split "hot"'s versions across two
  ;; blocks, both of which therefore start with the same user_key. A read must
  ;; enter at the FIRST such block (otherwise it skips the newest versions and
  ;; returns a stale answer) and then scan past the block end.
  (let [dir "./test-data/sst-index-straddle-split"]
    (rm-rf dir)
    (.mkdirs (jio/file dir))
    (let [versions 200
          entries (concat [["aaa" 1000 "before"]]
                          (map (fn [s] ["hot" s (str "v" s)]) (range versions 0 -1))
                          [["zzz" 1000 "after"]])
          table (write-table! dir 0 128 entries)
          ;; split "hot" in the middle: a second block starting at its 100th version
          split (assoc table
                       :index [[(->bytes "aaa") (entry-offset entries 0)]
                               [(->bytes "hot") (entry-offset entries 1)]
                               [(->bytes "hot") (entry-offset entries 100)]
                               [(->bytes "zzz") (entry-offset entries 201)]])]
      (testing "a key split across blocks resolves to its NEWEST version"
        (is (= "v200" (s-of (:value (io/read-value (:path split) (->bytes "hot")
                                                   Long/MAX_VALUE split))))
            "entered a later block and skipped the newest versions"))
      (testing "snapshots in the later block half still resolve correctly"
        (doseq [snap [1 50 101 150 200]]
          (is (= (str "v" snap)
                 (s-of (:value (io/read-value (:path split) (->bytes "hot")
                                              snap split)))))))
      (testing "neighbours of the split key are unaffected"
        (is (= "before" (s-of (:value (io/read-value (:path split) (->bytes "aaa")
                                                     Long/MAX_VALUE split)))))
        (is (= "after" (s-of (:value (io/read-value (:path split) (->bytes "zzz")
                                                    Long/MAX_VALUE split))))))
      (testing "latest-seq also enters at the first block of the split run"
        (is (= 200 (io/read-latest-seq (:path split) (->bytes "hot") split)))))
    (rm-rf dir)))

(deftest many-versions-of-one-key-are-read-correctly-test
  ;; The writer's own layout: one key with many versions stays in a single block
  ;; (blocks are cut only on a user_key change), and every snapshot still resolves.
  (let [dir "./test-data/sst-index-straddle"]
    (rm-rf dir)
    (.mkdirs (jio/file dir))
    (let [versions 200
          ;; "hot" carries seqs 200..1 (seq-desc), surrounded by other keys
          entries (concat [["aaa" 1000 "before"]]
                          (map (fn [s] ["hot" s (str "v" s)]) (range versions 0 -1))
                          [["zzz" 1000 "after"]])
          table (write-table! dir 0 128 entries)]
      (testing "the writer keeps one user_key's versions together"
        (is (= ["aaa" "zzz"] (mapv (comp s-of first) (:index table)))))
      (testing "the newest version is found"
        (is (= "v200" (s-of (:value (io/read-value (:path table) (->bytes "hot")
                                                   Long/MAX_VALUE table))))))
      (testing "every snapshot resolves to its newest visible version"
        (doseq [snap [1 2 37 99 150 200]]
          (is (= (str "v" snap)
                 (s-of (:value (io/read-value (:path table) (->bytes "hot")
                                              snap table))))
              (str "snapshot " snap))))
      (testing "a snapshot older than every version sees nothing"
        (is (nil? (io/read-value (:path table) (->bytes "hot") 0 table))))
      (testing "keys on either side of the straddling key are unaffected"
        (is (= "before" (s-of (:value (io/read-value (:path table) (->bytes "aaa")
                                                     Long/MAX_VALUE table)))))
        (is (= "after" (s-of (:value (io/read-value (:path table) (->bytes "zzz")
                                                    Long/MAX_VALUE table))))))
      (testing "latest-seq walks the same straddling run"
        (is (= 200 (io/read-latest-seq (:path table) (->bytes "hot") table)))))
    (rm-rf dir)))

;; ---- reads touch only their block ---------------------------------------

(deftest index-directed-read-touches-only-its-block-test
  ;; The acceptance criterion: a point read must not scan the whole file. Counted
  ;; deterministically rather than by timing.
  (let [dir "./test-data/sst-index-touch"]
    (rm-rf dir)
    (.mkdirs (jio/file dir))
    (let [n 400
          table (write-table! dir 0 256 (kv-entries n))
          entries-read (fn [key]
                         (let [c (atom 0)]
                           (binding [io/*entries-read* c]
                             (io/read-value (:path table) (->bytes key)
                                            Long/MAX_VALUE table))
                           @c))
          first-key (entries-read (format "key%05d" 0))
          last-key (entries-read (format "key%05d" (dec n)))]
      (testing "reading the LAST key does not deserialize the whole table"
        (is (< last-key 20)
            (str "read of the last key deserialized " last-key " entries of " n)))
      (testing "cost does not grow with key position (it is block-bounded)"
        ;; without the index this would be ~1 vs ~400
        (is (< (Math/abs (- last-key first-key)) 20)
            (str "first-key read=" first-key " last-key read=" last-key))))
    (rm-rf dir)))

;; ---- scans across block boundaries --------------------------------------

(deftest scan-across-block-boundaries-test
  (let [dir "./test-data/sst-index-scan"]
    (rm-rf dir)
    (.mkdirs (jio/file dir))
    (let [n 300
          table (write-table! dir 0 256 (kv-entries n))
          scan (fn [from to]
                 (mapv (comp s-of first)
                       (io/scan-pairs (:path table) (->bytes from) (->bytes to)
                                      Long/MAX_VALUE table)))
          k #(format "key%05d" %)]
      (testing "a full scan returns every key in order"
        (is (= (mapv k (range n)) (scan (k 0) "key99999"))))
      (testing "a mid-range scan spanning several blocks"
        (is (= (mapv k (range 100 150)) (scan (k 100) (k 150)))))
      (testing "from/to exactly on block-boundary keys"
        ;; use the index's own block boundaries as the range bounds
        (let [boundary-keys (mapv (comp s-of first) (:index table))
              a (nth boundary-keys 1)
              b (nth boundary-keys 3)
              expected (->> (range n) (map k) (filter #(and (>= (compare % a) 0)
                                                            (neg? (compare % b)))))]
          (is (seq boundary-keys))
          (is (= (vec expected) (scan a b)))))
      (testing "empty ranges"
        (is (= [] (scan (k 10) (k 10))) "from == to")
        (is (= [] (scan "zzz" "zzzz")) "past every key")
        (is (= [] (scan "aaa" "aab")) "before every key")))
    (rm-rf dir)))

;; ---- footer integrity ----------------------------------------------------

(defn- flip-byte! [path offset]
  (with-open [raf (RandomAccessFile. (str path) "rw")]
    (.seek raf offset)
    (let [b (.read raf)]
      (.seek raf offset)
      (.write raf (bit-xor b 0xff)))))

(deftest corrupt-footer-raises-test
  (let [dir "./test-data/sst-index-corrupt"]
    (rm-rf dir)
    (.mkdirs (jio/file dir))
    (testing "a corrupt trailer raises rather than silently mis-reading"
      (let [table (write-table! dir 0 256 (kv-entries 50))
            len (.length (jio/file (:path table)))]
        ;; flip a byte inside the trailer's payload (the last TRAILER_SIZE bytes)
        (flip-byte! (:path table) (- len 12))
        (is (thrown? clojure.lang.ExceptionInfo (io/read-footer! (:path table))))))
    (testing "a corrupt index region raises"
      (let [table (write-table! dir 1 256 (kv-entries 50))]
        ;; flip a byte inside the index region (just past where the entries end)
        (flip-byte! (:path table) (+ (:index-start table) 12))
        (is (thrown? clojure.lang.ExceptionInfo (io/read-footer! (:path table))))))
    (testing "a truncated file (no room for a trailer) raises"
      (let [table (write-table! dir 2 256 (kv-entries 50))]
        (with-open [raf (RandomAccessFile. (str (:path table)) "rw")]
          (.setLength raf 8))
        (is (thrown? clojure.lang.ExceptionInfo (io/read-footer! (:path table))))))
    (rm-rf dir)))

;; ---- footer round-trip ---------------------------------------------------

(deftest footer-round-trips-through-the-file-test
  (let [dir "./test-data/sst-index-footer"]
    (rm-rf dir)
    (.mkdirs (jio/file dir))
    (let [table (write-table! dir 0 256 (kv-entries 100))
          {:keys [index index-start]} (io/read-footer! (:path table))]
      (is (= (:index-start table) index-start) "index-start survives the round trip")
      (is (= (count (:index table)) (count index)) "block count survives")
      (testing "block keys and offsets match what the writer recorded"
        (doseq [[[wk wo] [rk ro]] (map vector (:index table) index)]
          (is (b= wk rk))
          (is (= wo ro))))
      (testing "blocks are ordered by user_key and start after the format byte"
        (is (= (mapv (comp s-of first) index)
               (sort (mapv (comp s-of first) index))))
        (is (every? #(>= (second %) 1) index))
        (is (every? #(< (second %) index-start) index))))
    (rm-rf dir)))
