(ns argumentica.btree-index
  (:require (argumentica [index :as index]
                         [btree :as btree])))

(defrecord Index [index-atom])

(defmethod index/inclusive-subsequence
  Index
  [this value]
  (btree/inclusive-subsequence (:index-atom this)
                               value))

(defmethod index/add-to-index
  Index
  [this value]
  (btree/add-to-atom (:index-atom this)
                     value))

(defmethod index/unload-index
  Index
  [this]
  (swap! (:index-atom this)
         btree/unload-btree))

(defn create
  ([node-size storage]
   (->Index (atom (btree/create (btree/full-after-maximum-number-of-values node-size)
                                storage))))
  
  ([node-size storage root-id]
   (->Index (atom (btree/create (btree/full-after-maximum-number-of-values node-size)
                                storage
                                root-id)))))
