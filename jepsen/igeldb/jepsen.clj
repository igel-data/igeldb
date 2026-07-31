(ns igeldb.jepsen
  "Verify IgelDB with jepsen-lite (github.com/yito88/jepsen-lite), in three
   shapes. Each workload maps to IgelDB as:

     :bank      multi-key atomic transfers via `with-tx`; the checker proves the
                total is conserved -- the direct test of Phase 3 snapshot
                isolation. THE interesting one.
     :register  a linearizable CAS built from a transaction (IgelDB has no native
                CAS): read the snapshot, compare, write, commit; a value mismatch
                or a commit conflict is a certain :fail. Knossos checks it.
     :set       durability of acknowledged writes; under a fault it exercises WAL
                replay / manifest recovery.
     :counter   read-modify-write in a transaction (lenient checker).

   Run:  clojure -M:jepsen                                  ; every workload
         clojure -M:jepsen --workload bank                  ; one of them
         clojure -M:jepsen --workload set --fault crash     ; in-process crash
         clojure -M:jepsen --profile process --workload set --fault crash
         clojure -M:jepsen --profile process --workload set --fault power-off
         clojure -M:jepsen --workload bank --time-limit 10 --concurrency 8
         clojure -M:jepsen --help

   `lite.runner` owns that command line, the workload repetition, the summary
   and the exit status; what stays here is the target-specific half -- how
   IgelDB is opened, reached and deployed -- gathered into `suite` at the end.

   Three kinds of crash, and the differences matter:

   :crash (:in-process)  `close!` + reopen -- a CLEAN shutdown and recovery. It
     exercises WAL replay / manifest recovery and the durability of
     already-acknowledged writes across a restart. Closing cleanly is required
     here because IgelDB's background threads must stop before the store is
     reopened on the same dir.

   :crash (:local-process)  IgelDB runs in a separate process -- `igeldb.driver`,
     which embeds it behind a small HTTP API -- and jepsen-lite SIGKILLs that
     process mid-run and starts it again. No `close!`, no flush, no shutdown
     hook: recovery has to come from what actually reached the disk. The
     handlers move to `igeldb.client`, which speaks HTTP; everything else --
     workloads, checkers, outcome signalling -- is unchanged, because a
     target's protocol and its deployment are separate axes in jepsen-lite.

     What it still does NOT test is loss of writes the OS took but never
     flushed: SIGKILL kills the process, not the page cache. A store that
     fsyncs what it acknowledges and one that merely write()s it come through a
     kill identically.

   :power-off (:local-process + lazyfs)  the same separate process, with the
     filesystem under its data directory replaced by lazyfs -- a FUSE
     filesystem that holds writes in a cache of its own until an fsync. Each
     fault clears that cache (and waits for lazyfs to confirm) before the
     SIGKILL, so IgelDB restarts on a disk that lost precisely the writes it
     never made durable. This is the fault that actually asks whether the WAL
     fsync before a commit is acknowledged (`igeldb.wal/commit-batch!`) is
     doing its job -- and whether the files and directory entries it depends on
     were fsynced too.

     Linux only, and it needs /dev/fuse and a built lazyfs checkout; point
     JEPSEN_LITE_LAZYFS (or `--lazyfs <dir>`) at it. Anywhere else jepsen-lite
     stops the run and says what is missing rather than quietly running a plain
     kill, which would report durability nobody tested."
  (:require [clj-yaml.core :as yaml]
            [igeldb.client :as igel-client]
            [igeldb.core :as igel]
            [lite.client :as client]
            [lite.handlers :as handlers]
            [lite.resource :as resource])
  (:import (java.io File)))

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

(defmacro ^:private on-conflict
  "Runs `body`, turning a lost commit into a certain failure: the transaction
   rolled back, so whatever it was attempting did not take effect."
  [reason & body]
  `(try
     ~@body
     (catch clojure.lang.ExceptionInfo e#
       (if (conflict? e#) (client/fail! ~reason) (throw e#)))))

;; ---- handlers -------------------------------------------------------------
;;
;; `lite.handlers` unpacks each workload's ops and returns what its checker
;; expects, so what is left here is IgelDB calls: a CAS is a transaction, a
;; transfer is a transaction, and neither knows a workload exists.

(defn- register-cas
  "Snapshot-read, compare, write, commit. Returns *exactly* false on a
   mismatch, which `handlers/register` turns into a certain :fail -- nil would
   not do: it is not false, and the CAS would be reported as having taken
   effect when it did not. A commit conflict is a certain :fail too: the tx
   rolled back, so the CAS definitely did not happen."
  [kvs key old new]
  (on-conflict "cas conflict"
    (igel/with-tx [tx kvs]
      (if (= old (->long (igel/tx-get tx (kbytes key))))
        (do (igel/tx-put tx (kbytes key) (vbytes new)) true)
        false))))

;; Bank accounts are the workload's business, not ours: `:init` names them and
;; their opening balances, and a read has to cover exactly those. So the keys
;; are the account numbers as decimal text, and a scan of ["0", ":") returns
;; every one of them -- ':' sorts just past '9', and multi-digit accounts sort
;; inside the range too. Nothing here assumes how many accounts there are or
;; what they start with.
(def ^:private bank-lo (->bytes "0"))
(def ^:private bank-hi (->bytes ":"))

(defn- bank-read
  "Every account's balance from one snapshot, so a read never catches a
   transfer half-done."
  [kvs]
  (let [ks (mapv first (igel/scan kvs bank-lo bank-hi))]
    (igel/with-tx [tx kvs]
      (into {} (map (fn [k] [(->long k) (or (->long (igel/tx-get tx k)) 0)])) ks))))

(defn- bank-transfer
  "Debit one account and credit another in one `with-tx`. Insufficient funds
   and commit conflicts both roll back -> :fail."
  [kvs from to amount]
  (on-conflict "transfer conflict"
    (igel/with-tx [tx kvs]
      (let [fb (or (->long (igel/tx-get tx (kbytes from))) 0)
            tb (or (->long (igel/tx-get tx (kbytes to))) 0)]
        (if (<= amount fb)
          (do (igel/tx-put tx (kbytes from) (vbytes (- fb amount)))
              (igel/tx-put tx (kbytes to) (vbytes (+ tb amount))))
          (client/fail! "insufficient funds"))))))

;; A set element -> a fixed-width key, so lexicographic order is numeric order
;; and a single scan over ["00..0", ":") covers every element.
(defn- set-key ^bytes [element] (->bytes (format "%020d" element)))
(def ^:private set-lo (set-key 0))
(def ^:private set-hi (->bytes ":"))

(def ^:private counter-key (->bytes "counter"))

(def ^:private handlers
  "Workload -> the handler that calls IgelDB directly."
  {:register (handlers/register
              {:read  (fn [kvs key] (->long (igel/select kvs (kbytes key))))
               :write (fn [kvs key value]
                        (igel/write! kvs (kbytes key) (vbytes value)))
               :cas   register-cas})
   :bank     (handlers/bank
              ;; The opening balances land in one transaction, so no read can
              ;; see a half-opened bank.
              {:init     (fn [kvs balances]
                           (igel/with-tx [tx kvs]
                             (doseq [[account balance] balances]
                               (igel/tx-put tx (kbytes account)
                                            (vbytes balance)))))
               :read     bank-read
               :transfer bank-transfer})
   :set      (handlers/set
              {:add  (fn [kvs element]
                       (igel/write! kvs (set-key element) (vbytes element)))
               :read (fn [kvs]
                       (mapv (fn [[_ v]] (->long v))
                             (igel/scan kvs set-lo set-hi)))})
   :counter  (handlers/counter
              ;; Read-modify-write in a transaction; an increment that loses the
              ;; commit conflict is a :fail -- it was not applied.
              {:add  (fn [kvs amount]
                       (on-conflict "counter conflict"
                         (igel/with-tx [tx kvs]
                           (let [cur (or (->long (igel/tx-get tx counter-key)) 0)]
                             (igel/tx-put tx counter-key
                                          (vbytes (+ cur amount)))))))
               :read (fn [kvs] (->long (igel/select kvs counter-key)))})})

;; ---- in-process -----------------------------------------------------------
;;
;; `open` opens (recovers) the store from a fixed dir; `close` closes the
;; handle. The on-disk data is the durable store, so it survives a close/reopen
;; -- which is exactly what the crash nemesis does. jepsen-lite keeps at most
;; one conn live at a time (it closes the old before opening the new), so there
;; are never two KVS handles on one dir.

(defn- adapter
  [config-path]
  (client/adapter {:open  (fn [] (igel/gen-kvs config-path))
                   :close (fn [kvs] (igel/close! kvs))}))

(defn- config-file!
  "A config for one run inside `dir`; returns its path. A small memtable makes
   a few hundred ops flush and compact, so the run exercises the SSTable and
   recovery paths, not just the memtable."
  [dir]
  (let [path (str dir File/separator "config.yaml")]
    (spit path (yaml/generate-string {:sstable-dir (str dir File/separator "sstable")
                                      :wal-dir (str dir File/separator "wal")
                                      :memtable-size 16384
                                      :sync-window-time 5}))
    path))

(defn config
  "A jepsen-lite run config for `workload` against IgelDB in jepsen-lite's own
   JVM. `opts`:

     :nemesis      faults, e.g. [:crash]
     :time-limit   how many seconds to run for
     :concurrency  how many workers issue ops"
  [workload {:keys [nemesis time-limit concurrency]}]
  ;; A directory of this run's own, created and never reused. A crash test
  ;; means nothing if what it recovers turns out to be the last run's.
  (let [dir (resource/run-dir! "./jepsen-data" (name workload))]
    (cond-> {:adapter  (adapter (config-file! dir))
             :handler  (get handlers workload)
             :workload workload
             :name     (str "igeldb-" (name workload))
             :target   {:type :in-process}}
      concurrency (assoc :concurrency concurrency)
      time-limit  (assoc :time-limit time-limit)
      nemesis     (assoc :nemesis nemesis)
      ;; Without a time limit the store answers so quickly that a fault can land
      ;; after the workload; crash often enough to exercise the recovery path.
      (and nemesis (not time-limit))
      (assoc :nemesis-opts {:crashes 8 :crash-interval 1/500}))))

;; ---- a separate process: kill -9 and power-off ----------------------------
;;
;; The same workloads against the same store, with one difference: IgelDB is
;; running in a process of its own, and jepsen-lite is holding the handle. The
;; adapter changes because the protocol does (HTTP, not method calls); the
;; target changes because the deployment does. Nothing else moves.
;;
;; :power-off is the same target again plus four lines of `:lazyfs`. The
;; workloads, the handlers, the adapter and the checkers are untouched -- what
;; changes is the filesystem IgelDB's data directory sits on.

(defn- driver-command
  "An ordinary command line: this JVM, this classpath, the driver's -main.
   The program itself and not a shell wrapper -- jepsen-lite signals what it
   started, and a shell would take the signal instead of the store.

   `TieredStopAtLevel=1` because every fault pays for a JVM start, and a
   restart that takes four seconds instead of two halves the number of kills a
   run of a given length can fit in."
  [port data-dir]
  [(str (System/getProperty "java.home") File/separator "bin"
        File/separator "java")
   "-XX:TieredStopAtLevel=1"
   "-cp" (System/getProperty "java.class.path")
   "clojure.main" "-m" "igeldb.driver"
   "--port"     (str port)
   "--data-dir" data-dir])

(defn kill-config
  "A run config for `workload` against IgelDB in a killable process. `opts` are
   `config`'s, plus `:lazyfs-dir`.

   With `:power-off` among the faults there is one more thing to arrange, and
   getting it wrong is silent: the driver's data directory has to *be* the
   lazyfs mount point. lazyfs can only drop writes that went through it, so a
   driver writing anywhere else sails through every power-off having lost
   nothing -- and the run passes while testing the opposite of what it claims."
  [workload {:keys [nemesis time-limit concurrency lazyfs-dir]}]
  (let [powering-off? (boolean (some #{:power-off} nemesis))
        ;; "poweroff", not "power-off". lazyfs scans the token after
        ;; `--config-path` for the substring "-o" -- meaning to catch a missing
        ;; value followed by a FUSE option -- and a *path* holding those two
        ;; characters trips it too, whereupon it quietly loads its default
        ;; config with a different fault FIFO and every clear-cache goes
        ;; nowhere. jepsen-lite has a fallback for this; not walking into it is
        ;; cheaper than relying on it.
        base     (resource/run-dir! "./jepsen-data"
                                    (str (if powering-off? "poweroff-" "kill-")
                                         (name workload)))
        ;; `run-dir!` hands back a canonical path, which matters more than it
        ;; looks: jepsen-lite waits for the lazyfs mount by looking the path up
        ;; in the mount table, and the kernel lists what it mounted. A path
        ;; carrying a `./` is absolute, correct, and matches nothing there.
        data-dir (if powering-off? (str base File/separator "data") base)
        port     (resource/free-port)
        url      (str "http://127.0.0.1:" port)]
    (resource/ensure-dir! data-dir)
    (cond-> {:adapter  (igel-client/adapter {:url url})
             :handler  (get igel-client/handlers workload)
             :workload workload
             :name     (str "igeldb-" (if powering-off? "poweroff-" "kill-")
                            (name workload))
             ;; The driver's log lives beside the mount and never under it, or
             ;; a power-off would drop the driver's own account of what it did.
             ;; The same goes for everything lazyfs needs for itself: a FIFO
             ;; behind the filesystem being set up is one nothing can open.
             :target   (cond-> {:type    :local-process
                                :command (driver-command port data-dir)
                                :url     url
                                :log     (str base File/separator "driver.log")}
                         powering-off?
                         (assoc :lazyfs
                                {:dir         (or lazyfs-dir
                                                  (System/getenv "JEPSEN_LITE_LAZYFS"))
                                 :mount-point data-dir
                                 :root        (str base File/separator "root")}))}
      concurrency (assoc :concurrency concurrency)
      nemesis     (assoc :nemesis nemesis)
      ;; A restart takes about two seconds. Leave a healthy window after it so
      ;; acknowledged writes can flush and rotate the manifest before the next
      ;; fault; back-to-back ones would only ever test connection refusal. A
      ;; power-off also waits for lazyfs to confirm the cache is clear, so it
      ;; gets a little more room again.
      nemesis     (assoc :nemesis-opts
                         {:fault-interval (if powering-off? 4 3)})
      ;; Restarting a JVM takes about two seconds, so a run needs a clock to run
      ;; against or the faults land after the workload has finished. A power-off
      ;; gets longer still: every op it makes goes through FUSE.
      true        (assoc :time-limit (or time-limit
                                         (cond powering-off? 30
                                               nemesis       15
                                               :else         5))))))

;; ---- the suite ------------------------------------------------------------

(def suite
  "The target-specific part of the test. `lite.runner` owns the CLI, workload
   repetition, summary and exit status; these two builders keep the deployment
   details here with IgelDB."
  {:name              "igeldb"
   :workloads         [:bank :register :set :counter]
   :default-workloads :all
   :default-profile   :in-process
   :profiles
   {:in-process {:build config}
    :process    {:build kill-config}}
   :options
   {:lazyfs {:key :lazyfs-dir
             :doc "path to a built lazyfs checkout (:power-off)"}}})
