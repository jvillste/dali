(ns argumentica.db.index-atom
  (:require [argumentica.index :as index]
            [argumentica.comparator :as comparator])
  (:use clojure.test))


(defrecord IndexAtom [index-atom])

(defn create [index]
  (->IndexAtom (atom index)))

(defmethod index/add!
  IndexAtom
  [this value]
  (index/add! @(:index-atom this)
              value))

(defmethod index/inclusive-subsequence
  IndexAtom
  [this key]
  (index/inclusive-subsequence @(:index-atom this)
                               key))

(defn swap-index! [this function & arguments]
  (apply swap! (:index-atom this) function arguments))
