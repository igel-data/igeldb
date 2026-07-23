(ns igeldb.jepsen
  "Verify IgelDB with jepsen-lite (github.com/yito88/jepsen-lite).

   IgelDB is an :in-process target: jepsen-lite opens/closes the store in its own
   JVM and (for :crash) closes and reopens it. Each workload maps to IgelDB as:

     :bank      multi-key atomic transfers via `with-tx`; the checker proves the
                total is conserved -- the direct test of Phase 3 snapshot
                isolation. THE interesting one.
     :register  a linearizable CAS built from a transaction (IgelDB has no native
                CAS): read the snapshot, compare, write, commit; a value mismatch
                or a commit conflict is a certain :fail. Knossos checks it.
     :set       durability of acknowledged writes; with :crash it exercises WAL
                replay / manifest recovery.
     :counter   read-modify-write in a transaction (lenient checker).

   Run:  clojure -M:jepsen                 ; bank + register + set + counter
         clojure -M:jepsen bank            ; one workload
         clojure -M:jepsen set crash       ; with the crash nemesis
         clojure -M:jepsen bank time=10    ; run for 10 seconds

   NOTE on :crash -- an :in-process 'crash' is `close!` + reopen, i.e. a CLEAN
   shutdown and recovery, not a `kill -9`. It exercises IgelDB's recovery path and
   the durability of already-acknowledged writes across a restart; it does NOT
   test a hard power-loss mid-fsync (that needs a :local-process target, which
   jepsen-lite doesn't run yet). Closing cleanly is required because IgelDB's
   background threads must stop before the store is reopened on the same dir."
  (:require [clojure.java.io :as jio]
            [clojure.string :as str]
            [clj-yaml.core :as yaml]
            [igeldb.core :as igel]
            [lite.client :as client]
            [lite.core :as core]))

;; ---- byte-array (de)serialization ----------------------------------------
;; IgelDB keys and values are byte arrays; the workloads use integers.

(defn- ->bytes ^bytes [^String s] (.getBytes s))
(defn- kbytes ^bytes [k] (->bytes (str k)))
(defn- vbytes ^bytes [n] (->bytes (str n)))
(defn- ->long [^bytes b] (when b (Long/parseLong (String. b))))

(defn- conflict?
  "True if `e` is IgelDB's write-write conflict signal (a rolled-back tx)."
  [e]
  (boolean (:igeldb/conflict (ex-data e))))

;; ---- handlers -------------------------------------------------------------

(def ^:private bank-accounts (vec (range 8)))

(defn- register-handler
  "read/write/cas over independent registers (op carries :key). CAS is a
   transaction: snapshot-read, compare, write, commit. A value mismatch is a
   certain :fail; a commit conflict (someone wrote the key first) is too -- the
   tx rolled back, so the CAS definitely did not take effect."
  [kvs {:keys [f key value]}]
  (case f
    :read  (->long (igel/select kvs (kbytes key)))
    :write (do (igel/write! kvs (kbytes key) (vbytes value)) value)
    :cas   (let [[old new] value]
             (try
               (igel/with-tx [tx kvs]
                 (if (= old (->long (igel/tx-get tx (kbytes key))))
                   (do (igel/tx-put tx (kbytes key) (vbytes new)) new)
                   (client/fail! "cas mismatch")))
               (catch clojure.lang.ExceptionInfo e
                 (if (conflict? e) (client/fail! "cas conflict") (throw e)))))))

(defn- bank-handler
  "Transfers debit one account and credit another atomically in one `with-tx`;
   reads return every account's balance from one snapshot, so a read never
   catches a transfer half-done. Insufficient funds and commit conflicts both
   roll back -> :fail."
  [kvs {:keys [f value]}]
  (case f
    ;; Put the whole starting total in account 0 (the others read as 0), durably
    :init     (igel/write! kvs (kbytes 0) (vbytes 100))
    :read     (igel/with-tx [tx kvs]
                (into {} (map (fn [a]
                                [a (or (->long (igel/tx-get tx (kbytes a))) 0)]))
                      bank-accounts))
    :transfer (let [{:keys [from to amount]} value]
                (try
                  (igel/with-tx [tx kvs]
                    (let [fb (or (->long (igel/tx-get tx (kbytes from))) 0)
                          tb (or (->long (igel/tx-get tx (kbytes to))) 0)]
                      (if (<= amount fb)
                        (do (igel/tx-put tx (kbytes from) (vbytes (- fb amount)))
                            (igel/tx-put tx (kbytes to) (vbytes (+ tb amount)))
                            amount)
                        (client/fail! "insufficient funds"))))
                  (catch clojure.lang.ExceptionInfo e
                    (if (conflict? e) (client/fail! "transfer conflict") (throw e)))))))

;; a set element -> a fixed-width key so lexicographic order = numeric order and a
;; single `scan` over ["00..0", ":") covers every element (':' sorts just past '9')
(defn- set-key ^bytes [element] (->bytes (format "%020d" element)))
(def ^:private set-lo (set-key 0))
(def ^:private set-hi (->bytes ":"))

(defn- set-handler
  [kvs {:keys [f value]}]
  (case f
    :add  (do (igel/write! kvs (set-key value) (vbytes value)) value)
    :read (mapv (fn [[_ v]] (->long v)) (igel/scan kvs set-lo set-hi))))

(def ^:private counter-key (->bytes "counter"))

(defn- counter-handler
  "read-modify-write in a transaction; a concurrent increment that loses the
   commit conflict is a :fail (its increment was not applied)."
  [kvs {:keys [f value]}]
  (case f
    :add  (try
            (igel/with-tx [tx kvs]
              (let [cur (or (->long (igel/tx-get tx counter-key)) 0)]
                (igel/tx-put tx counter-key (vbytes (+ cur value)))
                value))
            (catch clojure.lang.ExceptionInfo e
              (if (conflict? e) (client/fail! "counter conflict") (throw e))))
    :read (or (->long (igel/select kvs counter-key)) 0)))

(def ^:private handlers
  {:register register-handler
   :bank     bank-handler
   :set      set-handler
   :counter  counter-handler})

;; ---- the adapter ----------------------------------------------------------
;;
;; `open` opens (recovers) the store from a fixed dir; `close` closes the handle.
;; The on-disk data is the durable store, so it survives a close/reopen -- which
;; is exactly what the crash nemesis does. jepsen-lite keeps at most one conn
;; live at a time (it closes the old before opening the new), so there are never
;; two KVS handles on one dir. `handler` is filled in by `lite.core` from config.

(defrecord IgelAdapter [config-path handler]
  client/ClientAdapter
  (open [_] (igel/gen-kvs config-path))
  (invoke [_ conn op] (client/complete handler conn op))
  (close [_ conn] (when conn (igel/close! conn))))

;; ---- store setup ----------------------------------------------------------

(defn- rm-rf [f]
  (let [f (jio/file f)]
    (when (.isDirectory f) (doseq [c (.listFiles f)] (rm-rf c)))
    (.delete f)))

(defn- fresh-config!
  "A clean data dir + config for one run; returns the config path. A small
   memtable makes a few hundred ops flush and compact, so the run exercises the
   SSTable and recovery paths, not just the memtable."
  [nm]
  (let [dir (str "./jepsen-data/" nm)]
    (rm-rf dir)
    (.mkdirs (jio/file dir))
    (let [path (str dir "/config.yaml")]
      (spit path (yaml/generate-string {:sstable-dir (str dir "/sstable")
                                        :wal-dir (str dir "/wal")
                                        :memtable-size 16384
                                        :sync-window-time 5}))
      path)))

;; ---- runner ---------------------------------------------------------------

(defn config
  "A jepsen-lite run config for `workload`. `opts`:
     :nemesis     faults, e.g. [:crash]
     :time-limit  how many seconds to run for"
  [workload {:keys [nemesis time-limit]}]
  (let [path (fresh-config! (name workload))]
    (cond-> {:adapter  (map->IgelAdapter {:config-path path})
             :handler  (get handlers workload)
             :workload workload
             :name     (str "igeldb-" (name workload))
             :target   {:type :in-process}}
      time-limit (assoc :time-limit time-limit)
      nemesis (assoc :nemesis nemesis)
      ;; Without a time limit the store answers so quickly that a fault can land
      ;; after the workload; crash often enough to exercise the recovery path.
      (and nemesis (not time-limit))
      (assoc :nemesis-opts {:crashes 8 :crash-interval 1/500}))))

(defn run-workload
  "Run one workload and return jepsen-lite's verdict map."
  [workload opts]
  (println (str "\n==== " (name workload)
                (when (seq (:nemesis opts)) (str " " (str/join " " (map name (:nemesis opts)))))
                " ===="))
  (core/run (config workload opts)))

(defn- parse-args
  "Words in any order: workload names (default: all four), `crash`, and
   `time=<seconds>`."
  [args]
  (let [flags     (set (remove #(str/includes? % "=") args))
        settings  (into {} (map #(str/split % #"=" 2))
                        (filter #(str/includes? % "=") args))
        chosen    (filterv (comp flags name) (keys handlers))
        workloads (if (seq chosen) chosen [:bank :register :set :counter])]
    [workloads (cond-> {}
                 (flags "crash") (assoc :nemesis [:crash])
                 (some-> (get settings "time") parse-long)
                 (assoc :time-limit (parse-long (get settings "time"))))]))

(defn -main
  "clojure -M:jepsen [workload...] [crash] [time=<seconds>]"
  [& args]
  (let [[workloads opts] (parse-args args)
        results (doall (for [w workloads]
                         [w (:valid? (run-workload w opts))]))]
    (println "\n==== summary ====")
    (doseq [[w valid?] results]
      (println (format "  %-10s :valid? %s" (name w) (pr-str valid?))))
    (shutdown-agents)
    (System/exit (if (every? second results) 0 1))))
