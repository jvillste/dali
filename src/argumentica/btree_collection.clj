(ns argumentica.btree-collection
  (:require [argumentica.btree :as btree]
            [argumentica.comparator :as comparator]
            [argumentica.directory-storage :as directory-storage]
            [argumentica.hash-map-storage :as hash-map-storage]
            [argumentica.mutable-collection :as mutable-collection]
            [argumentica.transducible-collection :as transducible-collection]
            [argumentica.sorted-reducible :as sorted-reducible]
            [argumentica.util :as util]
            [schema.core :as schema]
            [clojure.test :refer :all]
            [argumentica.storage :as storage]
            [argumentica.reduction :as reduction])
  (:import argumentica.comparator.DatomComparator
           [clojure.lang IReduceInit IReduce]))

(defprotocol BtreeCollectionProtocol
  (btree-atom [this]))

(defn btree [btree-collection]
  @(:btree-atom btree-collection))

(defn locking-apply-to-btree! [btree-collection function & arguments]
  (locking (btree-atom btree-collection)
    (reset! (btree-atom btree-collection)
            (apply function
                   @(btree-atom btree-collection)
                   arguments)))
  btree-collection)

;; idea from https://juxt.pro/blog/ontheflycollections-with-reducible



(deftype BtreeCollection [btree-atom]
  BtreeCollectionProtocol
  (btree-atom [this] btree-atom)

  clojure.lang.Sequential
  clojure.lang.Seqable
  (seq [this]
    (locking btree-atom
      (btree/inclusive-subsequence btree-atom
                                   ::comparator/min)))
  clojure.lang.Sorted
  (comparator [this]
    (DatomComparator.))
  (entryKey [this entry]
    entry)
  (seq [this ascending?]
    (locking btree-atom
      (if ascending?
        (btree/inclusive-subsequence btree-atom
                                     ::comparator/min)
        (btree/inclusive-subsequence btree-atom
                                     ::comparator/max
                                     {:direction :backwards}))))
  (seqFrom [this value ascending?]
    (locking btree-atom
      (if ascending?
        (btree/inclusive-subsequence btree-atom
                                     value)
        (btree/inclusive-subsequence btree-atom
                                     value
                                     {:direction :backwards}))))
  sorted-reducible/SortedReducible
  (subreducible-method [sorted starting-key direction]
    (btree/btree-reducible btree-atom starting-key direction))
  mutable-collection/MutableCollection
  (add! [this value]
    (locking-apply-to-btree! this
                             btree/add-3
                             value))
  transducible-collection/TransducibleCollection
  (transduce [this value options]
    (locking btree-atom
      (btree/transduce-btree btree-atom
                             value
                             options))))

(defmethod print-method BtreeCollection [this ^java.io.Writer writer]
  (.write writer (with-out-str (clojure.pprint/pprint (seq this)))))

(defn- create-for-btree [btree]
  (->BtreeCollection (atom btree)))

(def create-options {(schema/optional-key :node-size) schema/Int
                     (schema/optional-key :create-atom) (schema/pred fn?)})

(def ^:private default-create-options {:node-size 1001
                                       :create-atom atom})

(util/defno create-on-disk [base-path options :- create-options]
  (let [options (merge default-create-options
                       options)]
    (->BtreeCollection ((:create-atom options) (btree/create-from-options-2 :node-storage (directory-storage/create base-path)
                                                                            :full? (btree/full-after-maximum-number-of-values (:node-size options)))))))

(util/defno create-in-memory [options :- create-options]
  (let [options (merge default-create-options
                       options)]
    (->BtreeCollection ((:create-atom options) (btree/create-from-options-2 :node-storage (hash-map-storage/create)
                                                                            :full? (btree/full-after-maximum-number-of-values (:node-size options)))))))

(defn store-root! [btree-collection]
  (locking-apply-to-btree! btree-collection
                           btree/store-root-2))

(defn create-test-btree-collection []
  (reduce mutable-collection/add!
          (create-in-memory {:node-size 3})
          [1 2 3 4]))

(deftest test-sorted
  (is (= [2 3 4]
         (subseq (create-test-btree-collection)
                 <=
                 2))))

(deftest test-sorted-reducible
  (is (= [2 3 4]
         (into [] (sorted-reducible/subreducible (create-test-btree-collection)
                                                 2))))

  (is (= [2 1]
         (into [] (sorted-reducible/subreducible (create-test-btree-collection)
                                                 2
                                                 :backwards))))
  (is (= 9
         (reduce + (sorted-reducible/subreducible (create-test-btree-collection)
                                                  2))))

  (is (= 19
         (reduce + 10 (sorted-reducible/subreducible (create-test-btree-collection)
                                                     2))))

  (is (= [3 4 5]
         (transduce (map inc)
                    conj
                    (sorted-reducible/subreducible (create-test-btree-collection)
                                                   2))))

  (is (= [3]
         (transduce (comp (take 1)
                          (map inc))
                    conj
                    (sorted-reducible/subreducible (create-test-btree-collection)
                                                   2))))


  (is (= [3]
         (into [] (eduction (comp (take 1)
                                  (map inc))
                            (sorted-reducible/subreducible (create-test-btree-collection)
                                                           2))))))
