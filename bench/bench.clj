;; IgelDB vs SQLite -- a small, deliberately honest benchmark.
;;
;; Run it on BOTH runtimes and compare; the gap between them is the SCI
;; interpretation tax, not a storage-engine difference:
;;
;;   bb bench/bench.clj      ;; IgelDB interpreted under Babashka/SCI
;;   clojure -M:bench        ;; IgelDB on the JVM
;;
;; SQLite is driven through the `sqlite3` CLI (no pod, no JDBC -- keeps bb.edn and
;; CI untouched). That means SQLite's numbers include SQL text parsing per
;; statement plus one process start. See the CAVEATS printed at the end: this is a
;; whole-stack, indicative comparison, NOT a rigorous engine benchmark.
;;
;; Exits non-zero only on error; a missing `sqlite3` degrades to IgelDB-only.

(require '[igeldb.core :as igel]
         '[clojure.java.io :as jio]
         '[clojure.java.shell :as shell]
         '[clj-yaml.core :as yaml])

;; ---- workload shape ------------------------------------------------------

;; Sizes are deliberately small. The binding constraint is IgelDB's point read:
;; an SSTable has no block index, so `io/read-value` scans the table file linearly
;; for every key (~2.8 ms/read on the JVM and ~35 ms/read under SCI at 5k records).
;; A 10k-read phase would run for minutes under bb, so n-reads stays low.
(def n-bulk 5000)      ;; records bulk-loaded in one transaction
(def n-durable 1600)   ;; individually-durable writes (one fsync each side)
(def n-reads 200)      ;; random point reads, single-threaded (latency)

;; Concurrent read phase (throughput). Reads scale with cores, so measuring them
;; single-threaded reports latency dressed up as throughput. The two engines get
;; DIFFERENT op counts on purpose: SQLite is ~100x faster per read, and each
;; reader is a separate `sqlite3` process paying ~4ms of startup, so it needs more
;; work to amortise that -- while the same count would make IgelDB (especially
;; under SCI) run for minutes. Throughput in ops/sec is comparable regardless of
;; op count; the counts are printed with the results.
(def reader-threads 8)
(def reads-per-reader-igel 100)      ;; 800 reads total
(def reads-per-reader-sqlite 1000)   ;; 8000 reads total
;; Concurrent writers for the durable-write phase. This number matters: group
;; commit coalesces concurrent writers into one fsync, so IgelDB's durable
;; throughput scales with concurrency up to the group-commit batch limit (64).
;; With too few writers the sync window (5ms) caps throughput instead, and the
;; phase measures the window rather than the storage engine.
(def writer-threads 32)
(def value-size 100)   ;; bytes per value (keys are 16 bytes)

;; IgelDB tuning used for the run (printed in the report -- these move the numbers)
(def memtable-size (* 4 1024 1024))
(def sync-window 5)

(def base "./bench-data")
(def igel-load-dir (str base "/igel-load"))
(def igel-durable-dir (str base "/igel-durable"))
(def sqlite-load-db (str base "/load.sqlite"))
(def sqlite-durable-db (str base "/durable.sqlite"))
(def sql-dir (str base "/sql"))

;; ---- small helpers -------------------------------------------------------

(defn- rm-rf [f]
  (let [f (jio/file f)]
    (when (.isDirectory f)
      (doseq [c (.listFiles f)] (rm-rf c)))
    (.delete f)))

(defn- ->bytes [^String s] (.getBytes s))
(defn- bench-key [i] (format "key%013d" i)) ;; 16 chars

(def value-str (apply str (repeat value-size \x)))
(def value-bytes (->bytes value-str))
(def key-bytes (mapv (comp ->bytes bench-key) (range n-bulk)))
;; sample uniformly across the WHOLE key space -- reading only the first n-reads
;; keys would sit at the start of the SSTable and flatter IgelDB's linear scan
(def read-order (vec (take n-reads (shuffle (range n-bulk)))))

(defn- runtime-label []
  (if-let [v (System/getProperty "babashka.version")]
    (str "bb " v " (SCI interpreter)")
    (str "JVM " (System/getProperty "java.version"))))

(defn- timed-ns
  "Run `f`, return elapsed nanoseconds (the value of `f` is discarded)."
  [f]
  (let [start (System/nanoTime)]
    (f)
    (- (System/nanoTime) start)))

(defn- best-of [reps f] (reduce min (repeatedly reps #(timed-ns f))))

(defn- ops-per-sec [n nanos]
  (if (pos? nanos) (* n (/ 1e9 (double nanos))) 0.0))

;; ---- IgelDB --------------------------------------------------------------

(defn- igel-config! [data-dir]
  (rm-rf data-dir)
  (.mkdirs (jio/file data-dir))
  (let [path (str data-dir "/config.yaml")]
    (spit path (yaml/generate-string {:sstable-dir (str data-dir "/sstable")
                                      :wal-dir (str data-dir "/wal")
                                      :memtable-size memtable-size
                                      :sync-window-time sync-window}))
    path))

(defn- igel-bulk-load!
  "One transaction containing every record -> one WAL record, one fsync."
  [kvs n]
  (igel/with-tx [tx kvs]
    (dotimes [i n]
      (igel/tx-put tx (key-bytes i) value-bytes))))

(defn- igel-durable-writes!
  "`threads` concurrent auto-commit writers. Group commit is designed for exactly
  this: concurrent writers coalesce into shared fsyncs."
  [kvs n threads]
  (let [per (quot n threads)
        fs (doall (for [t (range threads)]
                    (future
                      (dotimes [j per]
                        (let [i (+ (* t per) j)]
                          (igel/write! kvs (key-bytes i) value-bytes))))))]
    (doseq [f fs] @f)))

(defn- igel-point-reads! [kvs]
  ;; sum lengths so the reads cannot be optimised away
  (reduce (fn [acc i]
            (+ acc (alength ^bytes (igel/select kvs (key-bytes i)))))
          0 read-order))

(defn- spread-key
  "A deterministic spread of key indices across the whole table, so each reader
  touches different keys and the average scan depth stays representative."
  [t j]
  (key-bytes (mod (* (inc t) (inc j) 7919) n-bulk)))

(defn- igel-concurrent-reads!
  "`reader-threads` threads reading in parallel. IgelDB reads take a short read
  lock over an immutable version snapshot, so readers do not exclude each other."
  [kvs threads per]
  (let [fs (doall
            (for [t (range threads)]
              (future
                (loop [j 0 acc 0]
                  (if (= j per)
                    acc
                    (let [^bytes v (igel/select kvs (spread-key t j))]
                      (recur (inc j) (+ acc (alength v)))))))))]
    (reduce + (map deref fs))))

(defn- igel-range-scan! [kvs]
  (reduce (fn [acc [_ v]] (+ acc (alength ^bytes v)))
          0
          (igel/scan kvs (key-bytes 0) (->bytes (bench-key n-bulk)))))

;; ---- SQLite (via the sqlite3 CLI) ---------------------------------------

(def ^:private pragmas
  ;; matched durability: fsync on every commit, like IgelDB's group commit
  "PRAGMA journal_mode=WAL;\nPRAGMA synchronous=FULL;\n")

(def ^:private schema
  "CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB);\n")

(defn- sqlite3! [db & args]
  (let [r (apply shell/sh "sqlite3" db args)]
    (when-not (zero? (:exit r))
      (throw (ex-info "sqlite3 failed" (select-keys r [:exit :err]))))
    r))

(defn- sqlite-available? []
  (try (zero? (:exit (shell/sh "sqlite3" "-version"))) (catch Exception _ false)))

(defn- write-sql!
  "Write a script file (NOT timed -- only the sqlite3 run itself is timed, just as
  IgelDB's key/value byte arrays are built before its timer starts)."
  [path lines]
  (with-open [w (jio/writer path)]
    (.write w pragmas)
    (.write w schema)
    (doseq [l lines] (.write w ^String l))))

(defn- write-sql-raw!
  "Write a script with no PRAGMA/schema preamble. Read-only scripts use this: the
  DB is already configured, and issuing `journal_mode` from several concurrent
  processes would contend on an exclusive lock."
  [path lines]
  (with-open [w (jio/writer path)]
    ;; busy_timeout is per-connection (not persistent): without it, concurrent
    ;; readers fail fast with SQLITE_BUSY on transient contention (e.g. a WAL
    ;; recovery at open) instead of waiting. Any waiting is genuine contention and
    ;; belongs in the throughput number.
    (.write w "PRAGMA busy_timeout=5000;\n")
    (.write w ".output /dev/null\n")
    (doseq [l lines] (.write w ^String l))))

(defn- select-line [i]
  (str "SELECT length(v) FROM kv WHERE k='" (bench-key i) "';\n"))

(defn- insert-line [i]
  (str "INSERT OR REPLACE INTO kv VALUES('" (bench-key i) "','" value-str "');\n"))

(defn- run-sql! [db path] (sqlite3! db (str ".read " path)))

(defn- run-sql-concurrently!
  "Run one `sqlite3` process per script, all at once. WAL mode permits concurrent
  readers, so these do not serialise -- but each pays ~4ms of process startup
  (in parallel, so once in wall-clock, not once per reader)."
  [db paths]
  (let [fs (doall (for [p paths] (future (run-sql! db p))))]
    (doseq [f fs] @f)))

;; ---- report --------------------------------------------------------------

(def results (atom []))
(def harness (atom {}))

(defn- record!
  "Record a phase. The 5-arity uses the same op count for both engines; the
  6-arity allows different counts (throughput in ops/sec stays comparable)."
  ([phase note igel-ns sqlite-ns n]
   (record! phase note igel-ns n sqlite-ns n))
  ([phase note igel-ns igel-n sqlite-ns sqlite-n]
   (swap! results conj {:phase phase :note note
                        :igel (ops-per-sec igel-n igel-ns)
                        :sqlite (when sqlite-ns (ops-per-sec sqlite-n sqlite-ns))})))

(defn- fmt-ops [x] (format "%,d" (long x)))

(defn- print-report! [sqlite?]
  (println)
  (println "================================================================")
  (println "IgelDB vs SQLite --" (runtime-label))
  (println "================================================================")
  (println (format "IgelDB config: memtable-size=%d bytes, sync-window-time=%dms"
                   memtable-size sync-window))
  (println (format "Records: %d bulk / %d durable / %d reads; 16B keys, %dB values"
                   n-bulk n-durable n-reads value-size))
  (when-let [ms (:sqlite-startup-ms @harness)]
    (println (format "sqlite3 process startup: %.2f ms -- paid by EVERY SQLite row." ms))
    (println (format "  It dominates the 1-thread read row (%d reads ~= startup + work);"
                     n-reads))
    (println "  the 8-thread row amortises it over 1000 reads/process and is the"))
  (when (:sqlite-startup-ms @harness)
    (println "  better estimate of SQLite's real read throughput."))
  (println)
  (println (format "%-34s %14s %14s %8s" "phase" "IgelDB ops/s" "SQLite ops/s" "ratio"))
  (println (apply str (repeat 74 \-)))
  (doseq [{:keys [phase igel sqlite]} @results]
    (println (format "%-34s %14s %14s %8s"
                     phase
                     (fmt-ops igel)
                     (if sqlite (fmt-ops sqlite) "n/a")
                     (if (and sqlite (pos? sqlite))
                       (format "%.2fx" (/ igel sqlite))
                       "-"))))
  (println)
  (doseq [{:keys [phase note]} @results]
    (when note (println (str "  * " phase ": " note))))
  (println)
  (println "CAVEATS -- read these before quoting any number:")
  (println "  1. IgelDB under bb is INTERPRETED (SCI). Run both `bb bench/bench.clj`")
  (println "     and `clojure -M:bench`; the difference is interpreter overhead,")
  (println "     not storage-engine performance.")
  (when sqlite?
    (println "  2. SQLite is driven via the sqlite3 CLI, so its timings include SQL text")
    (println "     parsing per statement + one process start. A real client using")
    (println "     prepared statements would be meaningfully faster -- most of all on")
    (println "     the point-read phase, which parses one SELECT per key."))
  (println "  3. Durability is matched: SQLite journal_mode=WAL + synchronous=FULL")
  (println "     (fsync per commit) vs IgelDB's fsync per group commit. Note that")
  (println "     fsync semantics are platform-dependent -- on macOS neither engine")
  (println "     issues F_FULLFSYNC by default, so \"durable\" here means fsync()")
  (println "     returned, not that the data reached stable media. Both are affected")
  (println "     equally, but absolute write numbers are optimistic.")
  (println "  4. Different data models (KV vs SQL) and structures: IgelDB is an LSM")
  (println "     tree (write-optimised), SQLite a B-tree (read/update-optimised).")
  (println "  5. One machine, small dataset, warm page cache, single timed run per")
  (println "     write phase (best-of-3 for reads). Indicative only -- not a")
  (println "     controlled benchmark."))

;; ---- phases --------------------------------------------------------------

(defn run-bench []
  (rm-rf base)
  (.mkdirs (jio/file sql-dir))
  (let [sqlite? (sqlite-available?)]
    (when-not sqlite?
      (println "NOTE: `sqlite3` not found on PATH -- running IgelDB-only."))

    ;; --- pre-generate SQL scripts (untimed) ---
    (let [bulk-sql (str sql-dir "/bulk.sql")
          durable-sql (str sql-dir "/durable.sql")
          reads-sql (str sql-dir "/reads.sql")
          conc-sqls (mapv #(str sql-dir "/reads-" % ".sql") (range reader-threads))
          scan-sql (str sql-dir "/scan.sql")]
      (when sqlite?
        (write-sql! bulk-sql (concat ["BEGIN;\n"] (map insert-line (range n-bulk)) ["COMMIT;\n"]))
        (write-sql! durable-sql (map insert-line (range n-durable)))
        (write-sql-raw! reads-sql (map select-line read-order))
        ;; one script per concurrent reader, each touching a different key spread
        (dotimes [t reader-threads]
          (write-sql-raw! (conc-sqls t)
                          (map (fn [j] (select-line (mod (* (inc t) (inc j) 7919) n-bulk)))
                               (range reads-per-reader-sqlite))))
        (write-sql! scan-sql
                    [(str "SELECT sum(length(v)) FROM kv WHERE k >= '" (bench-key 0)
                          "' AND k < '" (bench-key n-bulk) "';\n")]))

      ;; --- phase 1: bulk load in one transaction ---
      (println "phase 1/5: bulk load (single transaction) ...")
      (let [kvs (igel/gen-kvs (igel-config! igel-load-dir))
            i-ns (timed-ns #(igel-bulk-load! kvs n-bulk))
            _ (igel/close! kvs)
            s-ns (when sqlite? (timed-ns #(run-sql! sqlite-load-db bulk-sql)))]
        (record! "bulk load (1 txn)" nil i-ns s-ns n-bulk))

      ;; --- phase 2: individually-durable writes ---
      (println "phase 2/5: durable writes (one fsync per write) ...")
      (let [kvs (igel/gen-kvs (igel-config! igel-durable-dir))
            i-ns (timed-ns #(igel-durable-writes! kvs n-durable writer-threads))
            _ (igel/close! kvs)
            s-ns (when sqlite? (timed-ns #(run-sql! sqlite-durable-db durable-sql)))]
        (record! "durable writes"
                 (str "IgelDB uses " writer-threads " concurrent writers -- group commit "
                      "coalesces them into shared fsyncs, so its throughput scales with "
                      "concurrency (up to the 64-entry batch limit); with few writers the "
                      "5ms sync window caps it instead. SQLite uses 1 writer because "
                      "concurrent writers serialise on its write lock, so this is SQLite's "
                      "best case, not a handicap.")
                 i-ns s-ns n-durable))

      ;; --- reopen so reads are served from SSTables, not the memtable ---
      (let [kvs (igel/gen-kvs (str igel-load-dir "/config.yaml"))]
        ;; --- phase 3: random point reads ---
        (println "phase 3/5: point reads (1 thread, latency) ...")
        (igel-point-reads! kvs) ;; warmup
        (let [i-ns (best-of 3 #(igel-point-reads! kvs))
              s-ns (when sqlite?
                     (run-sql! sqlite-load-db reads-sql) ;; warmup
                     (best-of 3 #(run-sql! sqlite-load-db reads-sql)))]
          (record! "point reads (1 thread)"
                   (str "IgelDB SSTables carry no block index, so a point read scans the "
                        "table file linearly -- read cost grows with SSTable size. That, "
                        "not the harness, dominates this row."
                        (when sqlite?
                          (str " SQLite also parses one SELECT per key here (the CLI has"
                               " no prepared-statement reuse), so its number is a floor.")))
                   i-ns s-ns n-reads))

        ;; --- phase 4: concurrent point reads (throughput) ---
        (println (str "phase 4/5: point reads, " reader-threads " concurrent readers ..."))
        ;; settle the WAL first (untimed) so the readers don't all race to recover it
        (when sqlite? (sqlite3! sqlite-load-db "PRAGMA wal_checkpoint(TRUNCATE);"))
        (igel-concurrent-reads! kvs reader-threads reads-per-reader-igel) ;; warmup
        (let [igel-n (* reader-threads reads-per-reader-igel)
              sqlite-n (* reader-threads reads-per-reader-sqlite)
              i-ns (best-of 3 #(igel-concurrent-reads! kvs reader-threads
                                                       reads-per-reader-igel))
              s-ns (when sqlite?
                     (run-sql-concurrently! sqlite-load-db conc-sqls) ;; warmup
                     (best-of 3 #(run-sql-concurrently! sqlite-load-db conc-sqls)))]
          (record! (str "point reads (" reader-threads " threads)")
                   (str "Throughput, not latency. Op counts differ by design: IgelDB "
                        igel-n " reads, SQLite " sqlite-n " (SQLite is ~100x faster per "
                        "read and each of its readers is a separate process paying ~4ms "
                        "startup, so it needs more work to amortise it). SQLite readers "
                        "are concurrent processes -- WAL permits parallel readers.")
                   i-ns igel-n s-ns sqlite-n))

        ;; --- phase 5: full range scan ---
        (println "phase 5/5: range scan ...")
        (igel-range-scan! kvs) ;; warmup
        (let [i-ns (best-of 3 #(igel-range-scan! kvs))
              s-ns (when sqlite?
                     (run-sql! sqlite-load-db scan-sql)
                     (best-of 3 #(run-sql! sqlite-load-db scan-sql)))]
          (record! "range scan (all keys)" nil i-ns s-ns n-bulk))
        (igel/close! kvs))

      ;; measure the harness overhead itself so it can be subtracted mentally
      (when sqlite?
        (swap! harness assoc :sqlite-startup-ms
               (/ (best-of 10 #(sqlite3! sqlite-load-db "SELECT 1;")) 1e6))))

    (print-report! sqlite?)))

(try
  (run-bench)
  (rm-rf base)
  (catch Throwable e
    (rm-rf base)
    (println "\nbenchmark ERROR:" (str e))
    (System/exit 1)))
