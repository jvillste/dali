(ns argumentica.btree-index
  (:require (argumentica [index :as index]
                         [btree :as btree]
                         [directory-storage :as directory-storage]
                         [hash-map-storage :as hash-map-storage])))

(defrecord BtreeIndex [btree-index-atom])

(defmethod index/inclusive-subsequence
  BtreeIndex
  [this value]
  (btree/inclusive-subsequence (:btree-index-atom this)
                               value))

(defn swap-btree! [btree-index function & arguments]
  (apply swap!
         (:btree-index-atom btree-index)
         function
         arguments))

(defn btree [btree-index]
  @(:btree-index-atom btree-index))

(defmethod index/add!
  BtreeIndex
  [this value]
  (swap-btree! this
               btree/add
               value))

(defn create [btree]
  (->BtreeIndex (atom btree)))

(defn create-directory-btree-index [base-path node-size]
  (->BtreeIndex (atom (btree/create-from-options :metadata-storage (directory-storage/create (str base-path "/metadata"))
                                                 :node-storage (directory-storage/create (str base-path "/nodes"))
                                                 :full? (btree/full-after-maximum-number-of-values node-size)))))

(defn create-memory-btree-index [node-size]
  (->BtreeIndex (atom (btree/create-from-options :metadata-storage (hash-map-storage/create)
                                                 :node-storage (hash-map-storage/create)
                                                 :full? (btree/full-after-maximum-number-of-values node-size)))))

(defn create-memory-btree-index-from-btree-index [btree-index]
  (->BtreeIndex (atom (btree/create-from-options :metadata-storage (-> btree-index :btree-index-atom deref :metadata-storage)
                                                 :node-storage (-> btree-index :btree-index-atom deref :node-storage)))))
