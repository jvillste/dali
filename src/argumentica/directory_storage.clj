(ns argumentica.directory-storage
  (:require (argumentica [btree :as btree]
                         [storage :as storage])
            [me.raynes.fs :as fs]
            [clojure.java.io :as io])
  (:import [java.io File]
           [java.nio.file Files Paths OpenOption LinkOption]
           [java.nio.file.attribute FileAttribute]
           java.nio.file.NoSuchFileException)
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
    (do (println "WARNING: Tried to get nonexistent file from storage: " (str (key-path this key)))
        nil)))

(defmethod storage/stream-from-storage!
  DirectoryStorage
  [this key]
  (if (file-exists? (key-path this key))
    (io/input-stream (string-to-path (str (:path this) "/" key)))
    (do (println "WARNING: Tried to stream nonexistent file from storage: " (str (key-path this key)))
      nil)))

(defmethod storage/put-to-storage!
  DirectoryStorage
  [this key bytes]
  (Files/write (key-path this key)
               bytes
               (into-array OpenOption []))
  this)

(defmethod storage/remove-from-storage!
  DirectoryStorage
  [this key]
  (try
    (Files/delete (key-path this key))
    (catch NoSuchFileException e
        (println "WARNING: Tried to remove nonexistent file from storage: " (str (key-path this key)))))
  this)

(defmethod storage/storage-keys!
  DirectoryStorage
  [this]
  (map (fn [file]
         (.getName file))
       (.listFiles (File. (:path this)))))

(defmethod storage/storage-contains?
  DirectoryStorage
  [this key]
  (file-exists? (key-path this key)))

(defn create [directory-path]
  (fs/mkdirs directory-path)
  (->DirectoryStorage directory-path))

(comment

  (String. (btree/get-from-storage (DirectoryStorage. "src/argumentica")
                                   "directory_storage.clj"))

  (btree/put-to-storage (DirectoryStorage. "data")
                        "test.txt"
                        (.getBytes "test"
                                   "UTF-8")))
