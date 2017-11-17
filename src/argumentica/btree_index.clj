(ns argumentica.btree-index
  (:require (argumentica [db :as db]
                         [btree :as btree])))

(defrecord Index [index-atom])

(defmethod db/inclusive-subsequence
  Index
  [this value]
  (btree/inclusive-subsequence (:index-atom this)
                               value))

(defmethod db/add-to-index
  Index
  [this value]
  (btree/add-to-atom (:index-atom this)
                     value))

(defmethod db/unload-index
  Index
  [this]
  (swap! (:index-atom this)
         btree/unload-btree))

(defn create
  ([node-size storage]
   (create node-size
           storage
           nil))
  
  ([node-size storage root-id]
   (->Index (atom (btree/create (btree/full-after-maximum-number-of-values node-size)
                                storage
                                root-id)))))
