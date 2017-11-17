(ns argumentica.directory-storage
  (:require (argumentica [btree :as btree]))
  (:import [java.nio.file Files Paths OpenOption]
           [java.nio.file.attribute FileAttribute])
  (:use clojure.test))


(defrecord DirectoryStorage [path])


(defn string-to-path [string]
  (Paths/get string
             (into-array String [])))

(defmethod btree/get-from-storage
  DirectoryStorage
  [this key]
  (Files/readAllBytes (string-to-path (str (:path this) "/" key))))

(defmethod btree/put-to-storage
  DirectoryStorage
  [this key bytes]
  (Files/write (string-to-path (str (:path this) "/" key))
               bytes
               (into-array OpenOption []))
  this)

(defn create-directories [path]
  (Files/createDirectories (string-to-path path)
                           (into-array FileAttribute [])))


(defn create [directory-path]
  (create-directories directory-path)
  (->DirectoryStorage directory-path))

(comment (String. (btree/get-from-storage (DirectoryStorage. "src/argumentica")
                                          "directory_storage.clj"))

         (btree/put-to-storage (DirectoryStorage. "data")
                               "test.txt"
                               (.getBytes "test"
                                          "UTF-8")))
