(ns igeldb.manifest-test
  (:require [clojure.java.io :as jio]
            [clojure.test :refer [deftest is testing]]
            [clj-yaml.core :as yaml]
            [igeldb.core :as igel]
            [igeldb.data :as data]
            [igeldb.io :as io]
            [igeldb.manifest :as manifest]
            [igeldb.sstable :as sstable]
            [igeldb.tx :as tx])
  (:import (java.io FileOutputStream BufferedOutputStream RandomAccessFile)))

(defn- ->bytes [^String s] (.getBytes s))
(defn- b= [a b] (data/byte-array-equals? a b))

(defn- delete-recursively!
  [file]
  (when (.isDirectory file)
    (doseq [c (.listFiles file)] (delete-recursively! c)))
  (.delete file))

(defn- rm-rf [dir] (delete-recursively! (jio/file dir)))

(defn- truncate-file!
  [path new-len]
  (with-open [raf (RandomAccessFile. (str path) "rw")]
    (.setLength raf new-len)))

(defn- flip-byte!
  [path offset]
  (with-open [raf (RandomAccessFile. (str path) "rw")]
    (.seek raf offset)
    (let [b (.read raf)]
      (.seek raf offset)
      (.write raf (bit-xor b 0xff)))))

(defn- write-int!
  [path offset n]
  (with-open [raf (RandomAccessFile. (str path) "rw")]
    (.seek raf offset)
    (.writeInt raf n)))

(defn- edit
  "A one-table add edit with bloom filter as raw bytes (manifest form)."
  [id level head tail deleted]
  {:added [{:id id :level level
            :head-key (->bytes head) :tail-key (->bytes tail)
            :bloom-filter (byte-array [id]) :size (* 10 (inc id))}]
   :deleted deleted})

;; ---- Step 1-1 / 1-3: replay reconstructs the table set -------------------

(deftest replay-reconstructs-table-set-test
  (let [dir "./test-data/manifest-replay"
        config {:sstable-dir (str dir "/sstable")}]
    (io/make-dir (:sstable-dir config))
    (let [[m _] (manifest/open-manifest config)]
      (manifest/append-edit! m (edit 0 0 "a" "m" []))
      (manifest/append-edit! m (edit 1 0 "n" "z" []))
      ;; compaction: L1 table 2 supersedes L0 tables 0 and 1
      (manifest/append-edit! m (edit 2 1 "a" "z" [0 1]))
      (manifest/close! m))
    (testing "every committed edit is read back in order"
      (let [edits (:edits (manifest/read-current-manifest config))]
        (is (= 3 (count edits)))
        (is (= 2 (-> edits (nth 2) :added first :id)))
        (is (= [0 1] (-> edits (nth 2) :deleted)))
        (is (b= (->bytes "n") (-> edits (nth 1) :added first :head-key)))))
    (testing "folding the edits reconstructs the exact per-level table set"
      (let [{:keys [tables edits]} (manifest/read-current-manifest config)
            edits (into [{:added tables :deleted []}] edits)
            sstables (reduce sstable/apply-edit [[]] edits)]
        (is (= 2 (count sstables)) "levels L0 and L1")
        (is (empty? (first sstables)) "L0 empty: 0 and 1 were deleted")
        (is (= [2] (mapv first (second sstables))) "L1 holds table 2")))
    (rm-rf dir)))

;; ---- Step 1-1: corruption classes ----------------------------------------

(deftest manifest-header-validation-test
  (testing "unsupported versions are rejected explicitly"
    (let [dir "./test-data/manifest-version"
          config {:sstable-dir (str dir "/sstable")}]
      (io/make-dir (:sstable-dir config))
      (let [[m _] (manifest/open-manifest config)]
        (manifest/close! m))
      ;; magic occupies bytes 0..7; the format version is the following int.
      (write-int! (manifest/current-manifest-path config) 8 99)
      (let [error (try
                    (manifest/read-current-manifest config)
                    nil
                    (catch clojure.lang.ExceptionInfo e e))]
        (is (:igeldb/unsupported-manifest-version (ex-data error)))
        (is (= 99 (:version (ex-data error)))))
      (rm-rf dir)))
  (testing "bad magic and an incomplete mandatory snapshot are corruption"
    (doseq [[suffix corrupt!]
            [["magic" #(flip-byte! % 0)]
             ["snapshot" #(truncate-file! % 30)]]]
      (let [dir (str "./test-data/manifest-" suffix)
            config {:sstable-dir (str dir "/sstable")}]
        (io/make-dir (:sstable-dir config))
        (let [[m _] (manifest/open-manifest config)]
          (manifest/close! m))
        (corrupt! (manifest/current-manifest-path config))
        (let [error (try
                      (manifest/read-current-manifest config)
                      nil
                      (catch clojure.lang.ExceptionInfo e e))]
          (is (:igeldb/manifest-corrupt (ex-data error))))
        (rm-rf dir))))
  (testing "the old unversioned manifest is rejected instead of guessed"
    (let [dir "./test-data/manifest-legacy"
          config {:sstable-dir (str dir "/sstable")}]
      (io/make-dir (:sstable-dir config))
      (spit (str (:sstable-dir config) "/MANIFEST") "legacy")
      (let [error (try
                    (manifest/open-manifest config)
                    nil
                    (catch clojure.lang.ExceptionInfo e e))]
        (is (:igeldb/unsupported-manifest-version (ex-data error)))
        (is (zero? (:version (ex-data error)))))
      (rm-rf dir))))

(deftest manifest-truncation-tolerated-test
  (let [dir "./test-data/manifest-trunc"
        config {:sstable-dir (str dir "/sstable")}]
    (io/make-dir (:sstable-dir config))
    (let [[m _] (manifest/open-manifest config)]
      (manifest/append-edit! m (edit 0 0 "a" "z" []))
      (manifest/append-edit! m (edit 1 0 "a" "z" []))
      (manifest/close! m))
    (let [path (manifest/current-manifest-path config)]
      ;; cut a few bytes off the last edit -> its tail is incomplete
      (truncate-file! path (- (.length (jio/file path)) 3))
      (is (= 1 (count (:edits (manifest/read-current-manifest config))))
          "a partial tail edit is tolerated; the earlier edit survives")
      ;; Reopening truncates the torn frame before appending. A later valid edit
      ;; must therefore remain reachable on the next replay.
      (let [[m recovery] (manifest/open-manifest config)]
        (is (= 1 (count (:edits recovery))))
        (manifest/append-edit! m (edit 2 0 "a" "z" []))
        (manifest/close! m))
      (is (= [0 2] (mapv #(-> % :added first :id)
                         (:edits (manifest/read-current-manifest config))))
          "a valid edit appended after recovery is reachable"))
    (rm-rf dir)))

(deftest manifest-midfile-corruption-raises-test
  (let [dir "./test-data/manifest-corrupt"
        config {:sstable-dir (str dir "/sstable")}]
    (io/make-dir (:sstable-dir config))
    (let [[m _] (manifest/open-manifest config)]
      (manifest/append-edit! m (edit 0 0 "a" "z" []))
      (manifest/append-edit! m (edit 1 0 "a" "z" []))
      (manifest/close! m))
    (let [path (manifest/current-manifest-path config)]
      ;; Header = 24 bytes and the empty snapshot frame = 41 bytes. Offset 80 is
      ;; inside the first edit payload, so its CRC no longer matches.
      (flip-byte! path 80)
      (is (thrown? clojure.lang.ExceptionInfo
                   (manifest/read-current-manifest config))
          "mid-file corruption must raise, not silently truncate"))
    (rm-rf dir)))

(defn- rotation-config
  [dir]
  {:sstable-dir (str dir "/sstable")
   :manifest-rotation-bytes Long/MAX_VALUE
   :manifest-rotation-edits 1})

(deftest manifest-rotation-preserves-snapshot-and-high-water-marks-test
  (let [dir "./test-data/manifest-rotation"
        config (rotation-config dir)
        committed (assoc (edit 7 1 "a" "z" []) :max-seq 42)]
    (io/make-dir (:sstable-dir config))
    (let [[m _] (manifest/open-manifest config)
          old-path (manifest/current-manifest-path config)]
      (manifest/append-edit! m committed)
      (is (true? (manifest/maybe-rotate! m (:added committed))))
      (is (not= old-path (manifest/current-manifest-path config)))
      (manifest/close! m))
    (let [{:keys [generation tables edits next-table-id max-seq]}
          (manifest/read-current-manifest config)
          manifest-files (filter #(re-matches #"MANIFEST-\d{20}" (.getName %))
                                 (.listFiles (jio/file (:sstable-dir config))))]
      (is (= 2 generation))
      (is (= [7] (mapv :id tables)))
      (is (empty? edits))
      (is (= 8 next-table-id))
      (is (= 42 max-seq))
      (is (= 1 (count manifest-files)) "obsolete generations are removed"))
    (rm-rf dir)))

(deftest manifest-rotation-before-current-switch-is-recoverable-test
  (let [dir "./test-data/manifest-rotation-before-current"
        config (rotation-config dir)
        committed (assoc (edit 3 0 "a" "z" []) :max-seq 11)]
    (io/make-dir (:sstable-dir config))
    (let [[m _] (manifest/open-manifest config)
          old-path (manifest/current-manifest-path config)]
      (manifest/append-edit! m committed)
      (binding [manifest/*rotation-step-hook*
                #(when (= :new-manifest-fsynced %)
                   (throw (ex-info "simulated crash" {})))]
        (is (false? (manifest/maybe-rotate! m (:added committed)))))
      (is (= old-path (manifest/current-manifest-path config))
          "CURRENT still names the old, complete generation")
      (manifest/close! m))
    (let [{:keys [tables edits next-table-id max-seq]}
          (manifest/read-current-manifest config)]
      (is (empty? tables))
      (is (= [3] (mapv #(-> % :added first :id) edits)))
      (is (= 4 next-table-id))
      (is (= 11 max-seq)))
    (rm-rf dir)))

(deftest manifest-rotation-after-current-switch-is-fatal-but-recoverable-test
  (let [dir "./test-data/manifest-rotation-after-current"
        config (rotation-config dir)
        committed (assoc (edit 5 0 "a" "z" []) :max-seq 17)]
    (io/make-dir (:sstable-dir config))
    (let [[m _] (manifest/open-manifest config)
          old-path (manifest/current-manifest-path config)]
      (manifest/append-edit! m committed)
      (let [error
            (binding [manifest/*rotation-step-hook*
                      #(when (= :current-replaced %)
                         (throw (ex-info "simulated crash" {})))]
              (try
                (manifest/maybe-rotate! m (:added committed))
                nil
                (catch clojure.lang.ExceptionInfo e e)))]
        (is (:igeldb/manifest-rotation-fatal (ex-data error))))
      (is (not= old-path (manifest/current-manifest-path config))
          "CURRENT already names the new snapshot")
      (manifest/close! m))
    (let [{:keys [tables edits next-table-id max-seq]}
          (manifest/read-current-manifest config)]
      (is (= [5] (mapv :id tables)))
      (is (empty? edits))
      (is (= 6 next-table-id))
      (is (= 17 max-seq)))
    (rm-rf dir)))

;; ---- integration helpers -------------------------------------------------

(defn- make-config-path
  [data-dir]
  (rm-rf data-dir)
  (.mkdirs (jio/file data-dir))
  (let [config {:sstable-dir (str data-dir "/sstable")
                :wal-dir (str data-dir "/wal")
                :memtable-size 1024
                :sync-window-time 20}
        path (str data-dir "/config.yaml")]
    (with-open [w (jio/writer path)]
      (.write w (yaml/generate-string config)))
    path))

(defn- fill!
  "Write n key/value pairs (enough n triggers flushes)."
  [kvs n]
  (dotimes [i n]
    (igel/write! kvs (->bytes (str "key" i)) (->bytes (str "val" i)))))

(defn- wal-id-of [file] (Long/parseLong (re-find #"\d+" (.getName file))))

(defn- wait-until
  [pred]
  (let [deadline (+ (System/currentTimeMillis) 15000)]
    (loop []
      (cond
        (pred) true
        (> (System/currentTimeMillis) deadline) false
        :else (do (Thread/sleep 10) (recur))))))

(defn- tiny-config-path!
  "A config whose memtable flushes on essentially every write (so a single write
  lands in an SSTable and leaves the live WAL generation empty)."
  [data-dir]
  (rm-rf data-dir)
  (.mkdirs (jio/file data-dir))
  (let [config {:sstable-dir (str data-dir "/sstable")
                :wal-dir (str data-dir "/wal")
                :memtable-size 1
                :sync-window-time 10}
        path (str data-dir "/config.yaml")]
    (with-open [w (jio/writer path)]
      (.write w (yaml/generate-string config)))
    path))

;; ---- Step 1-3/1-4: recovery with a stale (undeleted) WAL -----------------

(deftest recovery-with-stale-wal-test
  ;; Simulates a crash between the manifest commit and the WAL delete: an old
  ;; WAL is left on disk after its data was already committed to an SSTable.
  ;; Recovery must tolerate the extra WAL, replay only the highest, discard the
  ;; stale one, and keep the committed data.
  (let [data-dir "./test-data/manifest-stale-wal"
        config-path (make-config-path data-dir)
        wal-dir (str data-dir "/wal")]
    (let [kvs (igel/gen-kvs config-path)]
      (fill! kvs 128)
      (igel/close! kvs))
    ;; at least one flush happened, so lower WAL ids were deleted -> id 0 is free
    (let [live-ids (->> (io/list-files wal-dir)
                        (filter #(.endsWith (.getName %) ".wal"))
                        (map wal-id-of))]
      (is (seq live-ids))
      (is (not (some zero? live-ids)) "a flush advanced the WAL id past 0"))
    ;; drop a stale, lower-id WAL whose key was never written
    (with-open [fs (FileOutputStream. (str wal-dir "/0.wal"))
                os (BufferedOutputStream. fs)]
      (io/append-entry! os [(data/->ikey (->bytes "stale-key") 1)
                            (data/new-data (->bytes "STALE"))])
      (.flush os)
      (-> fs .getChannel (.force true)))
    (let [kvs (igel/gen-kvs config-path)]
      (is (b= (->bytes "val0") (igel/select kvs (->bytes "key0")))
          "committed data survives")
      (is (b= (->bytes "val120") (igel/select kvs (->bytes "key120"))))
      (is (nil? (igel/select kvs (->bytes "stale-key")))
          "the stale WAL was discarded, not replayed")
      (igel/close! kvs))
    (rm-rf data-dir)))

;; ---- Step 1-3: unreferenced SSTable ignored on startup -------------------

(deftest unreferenced-sstable-ignored-test
  (let [data-dir "./test-data/manifest-orphan-sst"
        config-path (make-config-path data-dir)
        sstable-dir (str data-dir "/sstable")]
    (let [kvs (igel/gen-kvs config-path)]
      (fill! kvs 128)
      (igel/close! kvs))
    ;; a stray .sst file not referenced by the manifest (e.g. a delete lost to a
    ;; crash, or a half-written orphan) must be ignored, not scanned
    (spit (str sstable-dir "/999999.sst") "garbage not a real sstable")
    (let [kvs (igel/gen-kvs config-path)]
      (is (b= (->bytes "val0") (igel/select kvs (->bytes "key0")))
          "startup succeeds and committed data is intact")
      (is (nil? (igel/select kvs (->bytes "no-such-key"))))
      (igel/close! kvs))
    (rm-rf data-dir)))

;; ---- Step 7: next-seq recovery via the manifest max-seq ------------------

(deftest next-seq-restored-from-manifest-after-empty-wal-flush-test
  ;; A clean shutdown right after a flush leaves an EMPTY WAL: the newest seq lives
  ;; only in an SSTable. next-seq must be recovered from the manifest's max-seq, not
  ;; the empty WAL -- otherwise a restart would restart seqs at 0 and reuse them,
  ;; corrupting version ordering / MVCC.
  (let [data-dir "./test-data/manifest-max-seq"
        config-path (tiny-config-path! data-dir)]
    (let [kvs (igel/gen-kvs config-path)]
      (igel/write! kvs (->bytes "k") (->bytes "v0"))
      ;; wait until the write is durably flushed: its SSTable is committed to the
      ;; version (so the manifest edit -- with max-seq -- is fsynced) and the live
      ;; memtable is empty.
      (is (wait-until #(and (zero? (deref (:size @(:memtable kvs))))
                            (seq (first @(:current-version (:tree kvs))))))
          "the write was flushed and its manifest edit committed")
      (let [max-before (tx/current-seq (:registry kvs))]
        (is (pos? max-before) "some seq was assigned")
        (igel/close! kvs)
        (let [kvs2 (igel/gen-kvs config-path)]
          (is (= max-before (tx/current-seq (:registry kvs2)))
              "next-seq restored from the manifest max-seq, not the empty WAL")
          (is (b= (->bytes "v0") (igel/select kvs2 (->bytes "k")))
              "the flushed value is recovered from the SSTable")
          ;; a fresh write must get a strictly higher seq -- no collision with the
          ;; already-committed SSTable version of the same key
          (igel/write! kvs2 (->bytes "k") (->bytes "v1"))
          (is (= (inc max-before) (tx/current-seq (:registry kvs2)))
              "the next write advances past the recovered seq")
          (is (b= (->bytes "v1") (igel/select kvs2 (->bytes "k"))))
          (igel/close! kvs2))))
    (rm-rf data-dir)))

;; ---- Step 2: reads stay consistent across version swaps ------------------

(deftest concurrent-reads-during-version-swaps-test
  ;; While flushes keep committing new versions (swapping `current-version`),
  ;; concurrent readers must never throw due to a mid-switch race -- each read
  ;; sees one immutable snapshot.
  (let [data-dir "./test-data/version-swap"
        config-path (make-config-path data-dir)
        kvs (igel/gen-kvs config-path)
        stop (atom false)
        errors (atom [])]
    (fill! kvs 64)
    (let [readers (doall
                   (for [_ (range 4)]
                     (future
                       (try
                         (while (not @stop)
                           (dotimes [i 64]
                             (igel/select kvs (->bytes (str "key" i))))
                           (doall (igel/scan kvs (->bytes "key0") (->bytes "key9"))))
                         (catch Throwable e (swap! errors conj e))))))]
      ;; writers keep triggering flushes == version swaps
      (dotimes [i 384]
        (igel/write! kvs (->bytes (str "key" i)) (->bytes (str "v" i))))
      (reset! stop true)
      (doseq [r readers] @r))
    (is (empty? @errors)
        (str "reads threw during version swaps: " (first @errors)))
    (igel/close! kvs)
    (rm-rf data-dir)))
