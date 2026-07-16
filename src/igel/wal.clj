(ns igel.wal
  (:require [clojure.core.async :as async]
            [clojure.java.io :as java-io]
            [igel.io :as io])
  (:import (java.io FileOutputStream BufferedOutputStream)))

(def ^:const ^:private DEFAULT_WINDOW_TIME 200)

(defn wal-file-path
  [^long id config]
  (str (:wal-dir config) \/ id ".wal"))

(defn load-existing-wal
  [config]
  (let [wal-file (->> (:wal-dir config)
                      io/list-files
                      (filter #(and (.isFile %)
                                    (.endsWith (.getName %) ".wal")))
                      first)]
    (when-not (nil? wal-file)
      (with-open [in-stream (java-io/input-stream wal-file)]
        (loop [result (transient [])]
          (let [pair (io/read-kv-pair! in-stream)]
            (if (= [nil nil] pair)
              (persistent! result)
              (recur (conj! result pair)))))))))

(defn spawn-wal-writer
  [flush-req-chan flush-wal-chan config]
  (io/make-dir (:wal-dir config))
  ;; wait for the first flush
  (let [[wal-index data-chan] (async/<!! flush-wal-chan)]
  ;; Start the WAL writer loop
    (async/go-loop [wal-id wal-index
                    data-chan data-chan]
      (let [file-stream (FileOutputStream. (wal-file-path wal-id config))
            out-stream (BufferedOutputStream. file-stream 4096)
            sync-window (or (:sync-window-time config) DEFAULT_WINDOW_TIME)]
      ;; WAL loop until a flush is completed
      ;; TODO: error handling
      ;;
      ;; `window-chan` is an `async/timeout` channel and must be recreated every
      ;; time it fires. A timed-out channel stays closed, and taking from a
      ;; closed channel returns nil immediately, so reusing one instance would
      ;; make `alt!` match the window on every iteration: group commit would
      ;; never batch and an empty batch would busy-loop the CPU.
        (loop [comp-channels (transient #{})
               window-chan (async/timeout sync-window)]
          ;; `next` is [next-comp-channels next-window-chan] to keep going, or
          ;; nil once the data channel is closed (a flush was requested).
          (let [next (async/alt!
                       data-chan ([[k d comp-chan]]
                                  (when-not (nil? k)
                                    (io/append-wal! out-stream [k d])
                                    [(conj! comp-channels comp-chan) window-chan]))
                       window-chan ([]
                                    (if (> (count comp-channels) 0)
                                      (do
                                        (.flush out-stream)
                                        (-> file-stream .getFD .sync)
                                        (mapv #(async/>!! % :done)
                                              (persistent! comp-channels))
                                        (async/>! flush-req-chan :try-flush)
                                        [(transient #{}) (async/timeout sync-window)])
                                      [comp-channels (async/timeout sync-window)])))]
            (if (nil? next)
              (do
                (.flush out-stream)
                (-> file-stream .getFD .sync)
                (.close out-stream)
                (.close file-stream)
                (mapv #(async/>!! % :done)
                      (persistent! comp-channels))
                (async/>! flush-req-chan :flush))
              (recur (first next) (second next)))))
      ;; The current WAL loop finished
      ;; Wait for the new data-chan from the flush writer
        (let [[wal-id data-chan] (async/<! flush-wal-chan)]
          (recur wal-id data-chan))))))
