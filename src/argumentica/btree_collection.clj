(ns argumentica.btree-collection
  (:require (argumentica [index :as index]
                         [btree :as btree]
                         [directory-storage :as directory-storage]
                         [hash-map-storage :as hash-map-storage])
            [argumentica.comparator :as comparator]
            [argumentica.mutable-collection :as mutable-collection]
            [argumentica.util :as util]
            [schema.core :as schema])
  (:import [argumentica.comparator DatomComparator]))

(defn- swap-btree! [btree-collection function & arguments]
  (apply util/locking-swap-volatile!
         (:btree-volatile btree-collection)
         function
         arguments))

(defrecord BtreeCollection [btree-volatile]
  clojure.lang.Sorted
  (comparator [this]
    (DatomComparator.))
  (entryKey [this entry]
    entry)
  (seq [this ascending?]
    (prn 'seq ascending?) ;; TODO: remove-me
    (if ascending?
      (btree/inclusive-subsequence (:btree-volatile this)
                                   ::comparator/min)
      (btree/inclusive-reverse-subsequence (:btree-volatile this)
                                           ::comparator/max)))
  (seqFrom [this value ascending?]
    (prn 'seqFrom ascending?) ;; TODO: remove-me

    (if ascending?
      (btree/inclusive-subsequence (:btree-volatile this)
                                   value)
      (btree/inclusive-reverse-subsequence (:btree-volatile this)
                                           value)))
  mutable-collection/MutableCollection
  (add! [this value]
    (swap-btree! this
                 btree/add
                 value)))

#_(defn- btree [btree-collection]
  @(:btree-volatile btree-collection))


(defn- create-for-btree [btree]
  (->BtreeCollection (volatile! btree)))

(defn store-root! [btree-collection last-transaction-number]
  (swap-btree! btree-collection
               (fn [btree]
                 (-> btree
                     (btree/store-root {:last-transaction-number last-transaction-number})
                     (assoc :last-indexed-transaction-number last-transaction-number)))))

(def create-options {(schema/optional-key :node-size) schema/Int})

(def ^:private default-create-options {:node-size 1001})

(util/defno create-disk-based [base-path options :- create-options]
  (let [options (merge default-create-options
                       options)]
   (create-for-btree (btree/create-from-options :metadata-storage (directory-storage/create (str base-path "/metadata"))
                                                :node-storage (directory-storage/create (str base-path "/nodes"))
                                                :full? (btree/full-after-maximum-number-of-values (:node-size options))))))

(util/defno create-memory-based [options :- create-options]
  (let [options (merge default-create-options
                       options)]
    (->BtreeCollection (volatile! (btree/create-from-options :metadata-storage (hash-map-storage/create)
                                                             :node-storage (hash-map-storage/create)
                                                             :full? (btree/full-after-maximum-number-of-values (:node-size options)))))))

#_(defn create-memory-btree-collection-from-btree-collection [btree-collection]
  (->BtreeCollection (volatile! (btree/create-from-options :metadata-storage (-> btree-collection :btree-volatile deref :metadata-storage)
                                                           :node-storage (-> btree-collection :btree-volatile deref :node-storage)))))
