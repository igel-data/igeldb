(ns igeldb.client
  "The other half of the separate-process setup: a jepsen-lite ClientAdapter
   that speaks HTTP to `igeldb.driver`, and one handler per workload.

   Compare it with the in-process adapter in `igeldb.jepsen`. The handlers there
   call IgelDB directly; these make an HTTP request instead. Everything else --
   which workloads exist, what each `:f` means, what the checkers do, how an
   outcome is signalled -- is identical, because the protocol a target speaks
   and the way it is deployed are separate concerns in jepsen-lite. This
   namespace is the protocol half; the target-type is the other."
  (:require [clojure.edn :as edn]
            [lite.client :as client :refer [fail! info!]]
            [lite.handlers :as handlers])
  (:import (java.io IOException)
           (java.net ConnectException URI)
           (java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers
                          HttpResponse HttpResponse$BodyHandlers
                          HttpTimeoutException)
           (java.time Duration)))

(def ^:private default-request-timeout
  "Long enough that a healthy store never trips it."
  (Duration/ofSeconds 5))

(def ^:private default-refusal-threshold 8)
(def ^:private default-refusal-backoff-ms 50)

(defn- refused?
  "Did the connection never get made? Then the operation certainly did not
   happen, which is a `fail!` and not an `info!`."
  [^Throwable t]
  (loop [t t]
    (cond (nil? t)                       false
          (instance? ConnectException t) true
          :else                          (recur (.getCause t)))))

(defn- backoff-after-refusal!
  "Stops a dead target from turning a short run into tens of thousands of
   immediate failures. The counter lives on the shared connection, so the
   threshold applies to all workers together rather than once per worker."
  [{:keys [consecutive-refusals refusal-threshold refusal-backoff-ms]}]
  (let [n (swap! consecutive-refusals inc)]
    (when (> n refusal-threshold)
      (Thread/sleep (long refusal-backoff-ms)))))

(defn- post
  "One request to the driver, mapped onto the outcomes a handler can signal:

     2xx                -> the value the store returned
     4xx                -> the store rejected the op -- a CAS mismatch, no
                           funds, a lost commit conflict. It certainly did not
                           take effect               -> fail!
     5xx                -> it broke midway; whether the op took effect is
                           unknowable                -> info!
     timeout            -> indeterminate             -> info!
     connection refused -> never arrived, certain    -> fail!
     other I/O          -> indeterminate             -> info!

   The last two are what a `kill -9` mid-run produces: a request that never
   reached a dead store is a certain failure, and one whose connection died
   in flight may or may not have been committed before the process went. Saying
   `:info` there is not vagueness -- it is the only honest answer, and the
   checkers know what to do with it."
  [{:keys [^HttpClient client url ^Duration timeout consecutive-refusals]
    :as conn} path body]
  (let [request (-> (HttpRequest/newBuilder (URI/create (str url path)))
                    (.timeout timeout)
                    (.header "Content-Type" "application/edn")
                    (.POST (HttpRequest$BodyPublishers/ofString (pr-str body)))
                    (.build))
        ^HttpResponse response
        (try
          (let [response (.send client request
                                (HttpResponse$BodyHandlers/ofString))]
            ;; Any response, including a 4xx/5xx, proves the target is reachable.
            (reset! consecutive-refusals 0)
            response)
          (catch HttpTimeoutException _
            (info! {:timeout path}))
          (catch IOException e
            (if (refused? e)
              (do (backoff-after-refusal! conn)
                  (fail! {:connection-refused path}))
              (info! {:io path, :message (ex-message e)}))))
        status (.statusCode response)
        parsed (edn/read-string (.body response))]
    (cond
      (<= 200 status 299) (:value parsed)
      (<= 400 status 499) (fail! (:error parsed))
      :else               (info! {:status status, :error (:error parsed)}))))

(defn adapter
  "An adapter which connects to the IgelDB driver's HTTP API.

   Note what `open` does not do: nothing is created, started or seeded. The
   store's data was on disk before this connection existed -- which is what
   makes reopening it after a crash a question worth asking."
  [{:keys [url request-timeout refusal-threshold refusal-backoff-ms]}]
  (client/adapter
   {:open
    (fn []
      {:url     url
       :timeout (or request-timeout default-request-timeout)
       :consecutive-refusals (atom 0)
       :refusal-threshold (or refusal-threshold default-refusal-threshold)
       :refusal-backoff-ms (or refusal-backoff-ms default-refusal-backoff-ms)
       :client  (-> (HttpClient/newBuilder)
                    (.connectTimeout (Duration/ofSeconds 2))
                    (.build))})
    :close
    (fn [conn]
      ;; Tolerate an already-closed conn: `close` is re-runnable.
      (when-let [c (:client conn)]
        (when (instance? java.lang.AutoCloseable c)
          (.close ^java.lang.AutoCloseable c))))}))

;; ---- handlers -------------------------------------------------------------
;;
;; `lite.handlers` unpacks each workload's ops and returns what its checker
;; expects -- which is not always what the driver hands back, and getting that
;; wrong by hand is easy: a counter's checker wants the increment, not the new
;; total, and a CAS's wants the `[old new]` pair the op carried.

(def handlers
  "Workload -> the handler that speaks HTTP for it."
  {:register (handlers/register
              ;; A mismatch comes back 409 and `post` calls fail! -- an
              ;; ordinary failed op, not a violation.
              {:read  #(post %1 "/read" {:key %2})
               :write #(post %1 "/write" {:key %2, :value %3})
               :cas   #(post %1 "/cas" {:key %2, :old %3, :new %4})})
   :bank     (handlers/bank
              ;; The opening balances go over the wire like any other op. The
              ;; handler doesn't know they are special, and the store doesn't
              ;; know it is a bank.
              {:init     #(post %1 "/write-all" {:values %2})
               :read     #(post % "/read-all" {})
               :transfer #(post %1 "/transfer" {:from %2, :to %3, :amount %4})})
   :set      (handlers/set
              {:add  #(post %1 "/append" {:key :elements, :element %2})
               :read #(post % "/read-collection" {:key :elements})})
   :counter  (handlers/counter
              {:add  #(post %1 "/add" {:key :counter, :amount %2})
               :read #(post % "/read" {:key :counter})})})
