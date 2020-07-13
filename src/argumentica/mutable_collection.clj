(ns argumentica.mutable-collection)

(defprotocol MutableCollection
  (add! [this value]))
