(ns argumentica.directory-storage
  (:require (argumentica [btree :as btree]
                         [storage :as storage]))
  (:import [java.nio.file Files Paths OpenOption LinkOption]
           [java.nio.file.attribute FileAttribute])
  (:use clojure.test))


(defrecord DirectoryStorage [path])


(defn string-to-path [string]
  (Paths/get string
             (into-array String [])))

(defn file-exists? [path]
  (Files/exists path
                (into-array LinkOption
                            [LinkOption/NOFOLLOW_LINKS])))

(defn key-path [directory-storage key]
  (string-to-path (str (:path directory-storage) "/" key)))

(defmethod storage/get-from-storage!
  DirectoryStorage
  [this key]
  (if (file-exists? (key-path this key))
    (Files/readAllBytes (string-to-path (str (:path this) "/" key)))
    nil))

(defmethod storage/put-to-storage!
  DirectoryStorage
  [this key bytes]
  (Files/write (key-path this key)
               bytes
               (into-array OpenOption []))
  this)

(defmethod storage/storage-contains?
  DirectoryStorage
  [this key]
  (file-exists? (key-path this key)))

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
