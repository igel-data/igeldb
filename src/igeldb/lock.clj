(ns igeldb.lock
  (:require [clojure.java.io :as io]
            [igeldb.io :as igel-io])
  (:import (java.nio.channels FileChannel)
           (java.nio.file OpenOption StandardOpenOption)))

(def ^:private lock-file-name ".igeldb.lock")

(defrecord DirectoryLock [path ^FileChannel channel lock])

(defn release-all!
  "Release directory locks in reverse acquisition order. Idempotent."
  [locks]
  (doseq [{:keys [^FileChannel channel]} (reverse locks)]
    ;; Closing a FileChannel releases all of its locks. Avoid calling FileLock
    ;; methods directly because Babashka's SCI does not expose them.
    (when channel
      (try (.close channel) (catch Throwable _)))))

(defn- locked-ex
  [path]
  (ex-info (str "IgelDB directory is already open: " path)
           {:igeldb/directory-locked true
            :path path
            :retriable false}))

(defn- acquire-one!
  [dir]
  (igel-io/make-dir dir)
  (let [canonical-dir (.getCanonicalPath (io/file dir))
        path (str canonical-dir "/" lock-file-name)
        channel (FileChannel/open
                 (.toPath (io/file path))
                 (into-array OpenOption [StandardOpenOption/CREATE
                                         StandardOpenOption/WRITE]))]
    (try
      (if-let [lock (.tryLock channel)]
        (->DirectoryLock path channel lock)
        (do
          (.close channel)
          (throw (locked-ex canonical-dir))))
      (catch Throwable e
        (when (.isOpen channel) (.close channel))
        ;; Babashka does not expose OverlappingFileLockException as an importable
        ;; class, so identify this same-JVM contention without importing it.
        (if (= "java.nio.channels.OverlappingFileLockException"
               (.getName (class e)))
          (throw (locked-ex canonical-dir))
          (throw e))))))

(defn acquire-all!
  "Exclusively lock every canonical directory in `dirs`.

  A persistent `.igeldb.lock` file is used in each directory. The file itself is
  deliberately not deleted on release: deleting it after another process has
  acquired the released inode could allow a third process to lock a new inode."
  [dirs]
  (let [canonical-dirs (->> dirs
                            (map #(do (igel-io/make-dir %)
                                      (.getCanonicalPath (io/file %))))
                            distinct
                            sort)]
    (reduce (fn [locks dir]
              (try
                (conj locks (acquire-one! dir))
                (catch Throwable e
                  (release-all! locks)
                  (throw e))))
            []
            canonical-dirs)))
