(ns argumentica.btree-index
  (:require (argumentica [index :as index]
                         [btree :as btree]
                         [directory-storage :as directory-storage]
                         [hash-map-storage :as hash-map-storage])
            [argumentica.comparator :as comparator]
            [argumentica.mutable-collection :as mutable-collection])
  (:import [argumentica.comparator DatomComparator]))

(defn swap-btree! [btree-index function & arguments]
  (apply swap!
         (:btree-index-atom btree-index)
         function
         arguments))

(defrecord BtreeIndex [btree-index-atom]
  clojure.lang.Sorted
  (comparator [this]
    (DatomComparator.))
  (entryKey [this entry]
    entry)
  (seq [this ascending?]
    (if ascending?
      (btree/inclusive-subsequence (:btree-index-atom this)
                                   ::comparator/min)
      (btree/inclusive-reverse-subsequence (:btree-index-atom this)
                                           ::comparator/max)))
  (seqFrom [this value ascending?]
    (if ascending?
      (btree/inclusive-subsequence (:btree-index-atom this)
                                   value)
      (btree/inclusive-reverse-subsequence (:btree-index-atom this)
                                           value)))
  mutable-collection/MutableCollection
  (add! [this value]
    (swap-btree! this
                 btree/add
                 value)))

(defmethod index/inclusive-subsequence
  BtreeIndex
  [this value]
  (btree/inclusive-subsequence (:btree-index-atom this)
                               value))

(defmethod index/inclusive-reverse-subsequence
  BtreeIndex
  [this value]
  (btree/inclusive-reverse-subsequence (:btree-index-atom this)
                                       value))

(defn btree [btree-index]
  @(:btree-index-atom btree-index))

(defmethod index/add!
  BtreeIndex
  [this value]
  (swap-btree! this
               btree/add
               value))

#_(defmethod index/last-stored-transaction-number
  BtreeIndex
  [this]
  (:last-indexed-transaction-number (btree this)))

(defn create [btree]
  (->BtreeIndex (atom btree)))

(defn store-root! [btree-index last-transaction-number]
  (swap-btree! btree-index
               (fn [btree]
                 (-> btree
                     (btree/store-root {:last-transaction-number last-transaction-number})
                     (assoc :last-indexed-transaction-number last-transaction-number)))))

(defn- last-stored-transaction-number [btree]
  (-> (btree/get-latest-root btree)
      :metadata
      :last-transaction-number))

(defn create-directory-btree-index [base-path node-size]
  (let [btree (btree/create-from-options :metadata-storage (directory-storage/create (str base-path "/metadata"))
                                         :node-storage (directory-storage/create (str base-path "/nodes"))
                                         :full? (btree/full-after-maximum-number-of-values node-size))]
    (->BtreeIndex (atom (assoc btree
                               :last-indexed-transaction-number (last-stored-transaction-number btree))))))

(defn create-memory-btree-index [node-size]
  (->BtreeIndex (atom (btree/create-from-options :metadata-storage (hash-map-storage/create)
                                                 :node-storage (hash-map-storage/create)
                                                 :full? (btree/full-after-maximum-number-of-values node-size)))))

(defn create-memory-btree-index-from-btree-index [btree-index]
  (->BtreeIndex (atom (btree/create-from-options :metadata-storage (-> btree-index :btree-index-atom deref :metadata-storage)
                                                 :node-storage (-> btree-index :btree-index-atom deref :node-storage)))))
