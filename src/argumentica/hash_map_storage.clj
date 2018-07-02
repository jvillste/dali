(ns argumentica.hash-map-storage
  (:require [argumentica.storage :as storage]))

(defrecord HashMapStorage [hash-map-atom])

(defn create []
  (->HashMapStorage (atom {})))

(defmethod storage/get-from-storage!
  HashMapStorage
  [storage key]
  (get @(:hash-map-atom storage)
       key))

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
