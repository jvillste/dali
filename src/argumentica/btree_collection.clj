(ns argumentica.btree-collection
  (:require [argumentica.btree :as btree]
            [argumentica.comparator :as comparator]
            [argumentica.directory-storage :as directory-storage]
            [argumentica.hash-map-storage :as hash-map-storage]
            [argumentica.mutable-collection :as mutable-collection]
            [argumentica.transducible-collection :as transducible-collection]
            [argumentica.util :as util]
            [schema.core :as schema])
  (:import argumentica.comparator.DatomComparator))

(defn btree [btree-collection]
  @(:btree-atom btree-collection))

(defn locking-apply-to-btree! [btree-collection function & arguments]
  (locking (:btree-atom btree-collection)
    (reset! (:btree-atom btree-collection)
            (apply function
                   @(:btree-atom btree-collection)
                   arguments)))
  btree-collection)

(defrecord BtreeCollection [btree-atom]
  ;; clojure.lang.Sorted
  ;; (comparator [this]
  ;;   (DatomComparator.))
  ;; (entryKey [this entry]
  ;;   entry)
  ;; (seq [this ascending?]
  ;;   (locking (:btree-atom this)
  ;;     (if ascending?
  ;;       (btree/inclusive-subsequence (:btree-atom this)
  ;;                                    ::comparator/min)
  ;;       (btree/inclusive-reverse-subsequence (:btree-atom this)
  ;;                                            ::comparator/max))))
  ;; (seqFrom [this value ascending?]
  ;;   (locking (:btree-atom this)
  ;;     (if ascending?
  ;;       (btree/inclusive-subsequence (:btree-atom this)
  ;;                                    value)
  ;;       (btree/inclusive-reverse-subsequence (:btree-atom this)
  ;;                                            value))))
  mutable-collection/MutableCollection
  (add! [this value]
    (locking-apply-to-btree! this
                             btree/add
                             value))
  transducible-collection/TransducibleCollection
  (transduce [this value options]
    (locking (:btree-atom this)
      (btree/transduce-btree (:btree-atom this)
                             value
                             options))))

(defn- create-for-btree [btree]
  (->BtreeCollection (atom btree)))

(def create-options {(schema/optional-key :node-size) schema/Int})

(def ^:private default-create-options {:node-size 1001})

(util/defno create-on-disk [base-path options :- create-options]
  (let [options (merge default-create-options
                       options)]
    (create-for-btree (btree/create-from-options :metadata-storage (directory-storage/create (str base-path "/metadata"))
                                                 :node-storage (directory-storage/create (str base-path "/nodes"))
                                                 :full? (btree/full-after-maximum-number-of-values (:node-size options))))))

(util/defno create-in-memory [options :- create-options]
  (let [options (merge default-create-options
                       options)]
    (create-for-btree (btree/create-from-options :metadata-storage (hash-map-storage/create)
                                                 :node-storage (hash-map-storage/create)
                                                 :full? (btree/full-after-maximum-number-of-values (:node-size options))))))
