(ns argumentica.hash-map-storage
  (:require [argumentica.storage :as storage]
            [clojure.java.io :as io]))

(defrecord HashMapStorage [hash-map-atom])

(defn create []
  (->HashMapStorage (atom {})))

(defmethod storage/get-from-storage!
  HashMapStorage
  [storage key]
  (get @(:hash-map-atom storage)
       key))

(defmethod storage/stream-from-storage!
  HashMapStorage
  [storage key]
  (if (contains? @(:hash-map-atom storage)
                 key)
    (io/input-stream (get @(:hash-map-atom storage)
                          key))
    (do (println "WARNING: Tried to stream nonexistent key from storage: " key)
        nil)))

(defmethod storage/put-to-storage!
  HashMapStorage
  [storage key value]
  (swap! (:hash-map-atom storage)
         assoc
         key
         value))

(defmethod storage/storage-keys!
  HashMapStorage
  [storage]
  (keys @(:hash-map-atom storage)))

(defmethod storage/remove-from-storage!
  HashMapStorage
  [storage key]
  (swap! (:hash-map-atom storage)
         dissoc
         key))

(defmethod storage/storage-contains?
  HashMapStorage
  [this key]
  (contains? @(:hash-map-atom this) key))
