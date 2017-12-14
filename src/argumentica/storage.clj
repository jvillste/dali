(ns argumentica.storage)

(defmulti put-to-storage!
  (fn [storage key value]
    (type storage)))

(defmulti get-from-storage!
  (fn [storage key]
    (type storage)))

(defmulti remove-from-storage!
  (fn [storage key]
    (type storage)))

(defmulti storage-keys!
  (fn [storage]
    (type storage)))


