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
            [clojure.test :refer :all])
  (:import argumentica.comparator.DatomComparator
           [clojure.lang IReduceInit IReduce]))

(defn btree [btree-collection]
  @(:btree-atom btree-collection))

(defn locking-apply-to-btree! [btree-collection function & arguments]
  (locking (:btree-atom btree-collection)
    (reset! (:btree-atom btree-collection)
            (apply function
                   @(:btree-atom btree-collection)
                   arguments)))
  btree-collection)

(defn reduce-btree [reducing-function initial-reduced-value btree-atom starting-key direction]
  (loop [reduced-value initial-reduced-value
         sequence (btree/sequence-for-value btree-atom
                                            starting-key
                                            direction)]
    (if (reduced? reduced-value)
      (unreduced reduced-value)
      (let [next-sequence (btree/next-sequence sequence
                                               btree-atom
                                               direction)]
        (if (nil? (first next-sequence))
          reduced-value
          (recur (reducing-function reduced-value
                                    (first sequence))
                 (rest next-sequence)))))))

;; idea from https://juxt.pro/blog/ontheflycollections-with-reducible

(defn btree-reducible [btree-atom starting-key direction]
  (reify
    IReduceInit
    (reduce [this reducing-function initial-reduced-value]
      (btree/reduce-btree reducing-function
                          initial-reduced-value
                          btree-atom
                          starting-key
                          direction))

    IReduce
    (reduce [this reducing-function]
      (btree/reduce-btree reducing-function
                          (reducing-function)
                          btree-atom
                          starting-key
                          direction))))

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
  sorted-reducible/SortedReducible
  (subreducible-method [sorted starting-key direction]
    (btree-reducible btree-atom starting-key direction))
  mutable-collection/MutableCollection
  (add! [this value]
    (locking-apply-to-btree! this
                             btree/add-3
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
    (create-for-btree (btree/create-from-options-2 :node-storage (directory-storage/create (str base-path "/nodes"))
                                                   :full? (btree/full-after-maximum-number-of-values (:node-size options))))))

(util/defno create-in-memory [options :- create-options]
  (let [options (merge default-create-options
                       options)]
    (create-for-btree (btree/create-from-options-2 :node-storage (hash-map-storage/create)
                                                   :full? (btree/full-after-maximum-number-of-values (:node-size options))))))

(defn create-test-btree-collection []
  (reduce mutable-collection/add!
          (create-in-memory {:node-size 3})
          [1 2 3 4]))

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
