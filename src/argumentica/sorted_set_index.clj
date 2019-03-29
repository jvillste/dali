(ns argumentica.sorted-set-index
  (:require [argumentica.index :as index]
            [argumentica.comparator :as comparator])
  (:use clojure.test))

(defrecord SortedSetIndex [sorted-set-atom])

(defn create []
  (->SortedSetIndex (atom (sorted-set-by comparator/compare-datoms))))

(defn creator [_name]
  (create))

(defmethod index/add!
  SortedSetIndex
  [this value]
  (swap! (:sorted-set-atom this)
         conj
         value))

(defmethod index/inclusive-subsequence
  SortedSetIndex
  [this key]
  (subseq @(:sorted-set-atom this)
          >=
          key))

#_(defmethod index/last-indexed-transaction-number
  BtreeIndex
  [this]
  (btree/last-transaction-number @(:btree-index-atom this)))

(defmethod index/inclusive-reverse-subsequence
  SortedSetIndex
  [this key]
  (rsubseq @(:sorted-set-atom this)
           <=
           key))


(deftest test-sorted-set-index
  (let [sorted-set-index (create)]
    (doto sorted-set-index
      (index/add! [1 :name 1 :set "Foo"])
      (index/add! [1 :name 2 :set "Bar"])
      (index/add! [1 :age 2 :set 30])
      (index/add! [1 :name 3 :set "Baz"]))

    (is (= '([1 :name 2 :set "Bar"]
             [1 :name 3 :set "Baz"])
           (index/inclusive-subsequence sorted-set-index
                                        [1 :name 2 ::comparator/min ::comparator/min])))))
