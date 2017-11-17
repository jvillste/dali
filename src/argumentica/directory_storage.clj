(ns argumentica.directory-storage
  (:require [argumentica.index :as index])
  (:import [java.nio.file Files Paths OpenOption]
           [java.nio.file.attribute FileAttribute])
  (:use clojure.test))


(defrecord DirectoryStorage [path])


(defn string-to-path [string]
  (Paths/get string
             (into-array String [])))

(defmethod index/get-from-storage
  DirectoryStorage
  [this key]
  (Files/readAllBytes (string-to-path (str (:path this) "/" key))))

(defmethod index/put-to-storage
  DirectoryStorage
  [this key bytes]
  (Files/write (string-to-path (str (:path this) "/" key))
               bytes
               (into-array OpenOption []))
  this)

(defn create-directories [path]
  (Files/createDirectories (string-to-path path)
                           (into-array FileAttribute [])))

(comment (String. (get-from-storage (DirectoryStorage. "src/argumentica")
                                    "index.clj"))

         (put-to-storage (DirectoryStorage. "")
                         "/Users/jukka/Downloads/test.txt"
                         (.getBytes "test"
                                    "UTF-8")))
