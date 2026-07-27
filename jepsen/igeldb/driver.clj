(ns igeldb.driver
  "IgelDB behind a small HTTP API, so that jepsen-lite can run it as a
   `:local-process` target and kill -9 it.

   IgelDB is embedded -- a library, not a server -- so it cannot be a process
   anything else can signal. This is the thin driver that makes it one: it
   opens a store on a data directory and exposes its operations over HTTP.
   Nothing here knows about jepsen-lite, and nothing here is part of IgelDB;
   it is a test program, and the process `kill -9` lands on.

   Started, killed and started again by `igeldb.jepsen`; run it by hand with

     clojure -M:jepsen-driver --port 8080 --data-dir ./jepsen-data/manual

   ## Keys and values on the wire
   ## ===========================
   ##
   The workloads speak EDN -- integer register keys, `:elements`, `:counter`,
   integer account numbers -- and IgelDB speaks byte arrays ordered
   lexicographically. Two shapes of key, each with a prefix of its own so that
   a range scan has well-defined bounds:

     k:<edn>                 a value under a key
     c:<edn>:<0-padded n>    one element of a collection, so that a scan of
                             `c:<edn>:` returns the whole collection in order"
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [igeldb.core :as igel])
  (:import (com.sun.net.httpserver HttpExchange HttpHandler HttpServer)
           (java.io IOException)
           (java.net InetSocketAddress)
           (java.nio.charset StandardCharsets)
           (java.util.concurrent Executors)))

;; ---- encoding -------------------------------------------------------------

(defn- ->bytes ^bytes [^String s] (.getBytes s "UTF-8"))
(defn- ->string [^bytes b] (when b (String. b "UTF-8")))

(defn- value-key ^bytes [k] (->bytes (str "k:" (pr-str k))))
(defn- value-key-range [] [(->bytes "k:") (->bytes "k;")])

(defn- element-key ^bytes [collection element]
  ;; Fixed width, so lexicographic order is numeric order.
  (->bytes (format "c:%s:%020d" (pr-str collection) (long element))))

(defn- element-key-range [collection]
  [(->bytes (str "c:" (pr-str collection) ":"))
   (->bytes (str "c:" (pr-str collection) ";"))])

(defn- decode-value-key [^bytes k] (edn/read-string (subs (->string k) 2)))
(defn- encode ^bytes [v] (->bytes (pr-str v)))
(defn- decode [^bytes v] (some-> (->string v) edn/read-string))

;; ---- rejections -----------------------------------------------------------

(defrecord Rejected [reason])

(defn- rejected
  "The store refusing an operation: a CAS that didn't match, a transfer with no
   funds, a transaction that lost a conflict. Answered with 409 -- certain, and
   not a crash, so the client side turns it into an ordinary `:fail`."
  [reason]
  (->Rejected reason))

(defn- conflict?
  "IgelDB's write-write conflict: the transaction rolled back, so whatever it
   was trying to do certainly did not happen."
  [e]
  (boolean (:igeldb/conflict (ex-data e))))

(defmacro ^:private on-conflict
  "Runs `body`, turning a lost commit into a 409."
  [& body]
  `(try
     ~@body
     (catch clojure.lang.ExceptionInfo e#
       (if (conflict? e#) (rejected "transaction conflict") (throw e#)))))

;; ---- the operations -------------------------------------------------------
;;
;; Every read-modify-write goes through a transaction, because that is what the
;; workloads are really testing: bank's transfer has to move money in one step,
;; and register's CAS has to compare and set without another client slipping in
;; between. IgelDB has no native CAS, so it is built from a transaction.

(defn- op-read
  [kvs {:keys [key]}]
  (decode (igel/select kvs (value-key key))))

(defn- op-read-all
  "Every value, from one snapshot -- which is what makes bank's read
   meaningful. The keys come from a scan and the values from inside a
   transaction, so a transfer can't be caught half-done."
  [kvs _request]
  (let [[from to] (value-key-range)
        keys      (mapv first (igel/scan kvs from to))]
    (igel/with-tx [tx kvs]
      (into {} (map (fn [k] [(decode-value-key k) (decode (igel/tx-get tx k))]))
            keys))))

(defn- op-read-collection
  [kvs {:keys [key]}]
  (let [[from to] (element-key-range key)]
    (mapv (fn [[_ v]] (decode v)) (igel/scan kvs from to))))

(defn- op-write
  [kvs {:keys [key value]}]
  (igel/write! kvs (value-key key) (encode value))
  value)

(defn- op-write-all
  [kvs {:keys [values]}]
  (on-conflict
   (igel/with-tx [tx kvs]
     (doseq [[k v] values] (igel/tx-put tx (value-key k) (encode v)))
     values)))

(defn- op-cas
  [kvs {:keys [key old new]}]
  (on-conflict
   (igel/with-tx [tx kvs]
     (let [k (value-key key)]
       (if (= old (decode (igel/tx-get tx k)))
         (do (igel/tx-put tx k (encode new)) new)
         (rejected "cas mismatch"))))))

(defn- op-append
  "One key per element, so adds never contend with each other. A collection
   kept under a single key would make every concurrent add a write-write
   conflict, and the run would measure IgelDB's conflict detection rather than
   its durability."
  [kvs {:keys [key element]}]
  (igel/write! kvs (element-key key element) (encode element))
  element)

(defn- op-add
  [kvs {:keys [key amount]}]
  (on-conflict
   (igel/with-tx [tx kvs]
     (let [k     (value-key key)
           total (+ (or (decode (igel/tx-get tx k)) 0) amount)]
       (igel/tx-put tx k (encode total))
       total))))

(defn- op-transfer
  [kvs {:keys [from to amount]}]
  (on-conflict
   (igel/with-tx [tx kvs]
     (let [from-key (value-key from)
           to-key   (value-key to)
           balance  (or (decode (igel/tx-get tx from-key)) 0)]
       (if (<= amount balance)
         (do (igel/tx-put tx from-key (encode (- balance amount)))
             (igel/tx-put tx to-key
                          (encode (+ (or (decode (igel/tx-get tx to-key)) 0)
                                     amount)))
             amount)
         (rejected "insufficient funds"))))))

(def ^:private routes
  {"/read"            op-read
   "/read-all"        op-read-all
   "/read-collection" op-read-collection
   "/write"           op-write
   "/write-all"       op-write-all
   "/cas"             op-cas
   "/append"          op-append
   "/add"             op-add
   "/transfer"        op-transfer})

;; ---- the server -----------------------------------------------------------

(defn- request-body
  [^HttpExchange exchange]
  (with-open [in (.getRequestBody exchange)]
    (let [body (slurp in :encoding "UTF-8")]
      (if (str/blank? body) {} (edn/read-string body)))))

(defn- respond!
  [^HttpExchange exchange status body]
  (let [bytes (.getBytes (pr-str body) StandardCharsets/UTF_8)]
    (.set (.getResponseHeaders exchange) "Content-Type" "application/edn")
    (.sendResponseHeaders exchange status (alength bytes))
    (with-open [out (.getResponseBody exchange)]
      (.write out bytes))))

(defn- exchange-handler
  [kvs]
  (reify HttpHandler
    (handle [_this exchange]
      (with-open [^HttpExchange ex exchange]
        (try
          (let [path (.getPath (.getRequestURI ex))
                op   (get routes path)]
            (cond
              (not= "POST" (.getRequestMethod ex))
              (respond! ex 405 {:error (str "use POST for " path)})

              (nil? op)
              (respond! ex 404 {:error (str "no such operation " path)})

              :else
              (let [result (op kvs (request-body ex))]
                (if (instance? Rejected result)
                  (respond! ex 409 {:error (:reason result)})
                  (respond! ex 200 {:value result})))))
          (catch Throwable t
            ;; Never leave a client waiting on a response that isn't coming.
            (try
              (respond! ex 500 {:error (str (.getName (class t)) ": "
                                            (ex-message t))})
              (catch IOException _ nil))))))))

(defn serve
  "Starts the HTTP API over an open store. Returns `{:url ..., :stop ...}`."
  ([kvs port] (serve kvs port "127.0.0.1"))
  ([kvs port host]
   (let [pool   (Executors/newCachedThreadPool)
         server (HttpServer/create (InetSocketAddress. ^String host (int port)) 0)]
     ;; Without an executor every request is served on the one dispatcher
     ;; thread, and a store that answers one client at a time can't be caught
     ;; out by a workload -- least of all bank, whose whole question is what
     ;; concurrent clients see.
     (.setExecutor server pool)
     (.createContext server "/" (exchange-handler kvs))
     (.start server)
     {:url  (str "http://" host ":" (.getPort (.getAddress server)))
      :stop (fn [] (.stop server 0) (.shutdownNow pool) nil)})))

(defn config-path!
  "IgelDB is configured from a YAML file, so write one into the data directory.
   A tiny memtable and one-edit manifest rotation threshold make even a short
   crash run flush and rotate manifests. That puts manifest replacement and
   recovery in the SIGKILL window instead of merely testing WAL replay.

   Written the same way every time, because a restart after a crash has to open
   the store it left behind."
  [data-dir]
  (let [dir  (doto (io/file data-dir) (.mkdirs))
        path (io/file dir "config.yaml")]
    (spit path (str "sstable-dir: " (.getPath (io/file dir "sstable")) "\n"
                    "wal-dir: " (.getPath (io/file dir "wal")) "\n"
                    "memtable-size: 256\n"
                    "manifest-rotation-edits: 1\n"
                    "sync-window-time: 5\n"))
    (.getPath path)))

(defn -main
  "`--port n --data-dir path`. Runs until it is stopped -- or killed."
  [& args]
  (let [opts     (into {} (map (fn [[k v]] [(str/replace k #"^--" "") v]))
                       (partition 2 args))
        data-dir (get opts "data-dir" "./jepsen-data/driver")
        kvs      (igel/gen-kvs (config-path! data-dir))
        {:keys [url]} (serve kvs (parse-long (get opts "port" "0")))]
    ;; A clean stop closes the store properly. A `kill -9` doesn't get one --
    ;; no hooks run, nothing is flushed, and whether the acknowledged writes
    ;; are still there is exactly the question being asked.
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. ^Runnable (fn [] (igel/close! kvs))))
    (println (str "igeldb listening on " url " (data in " data-dir ")"))
    (flush)
    @(promise)))
