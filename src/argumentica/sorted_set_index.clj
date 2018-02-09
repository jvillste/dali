(ns argumentica.sorted-set-index
  (:require [argumentica.index :as index]
            [argumentica.comparator :as comparator])
  (:use clojure.test))

(defrecord SortedSetIndex [sorted-set-atom])

(defn create []
  (->SortedSetIndex (atom (sorted-set-by comparator/cc-cmp))))

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
      (index/add! [1 :name 2 :set "Bar"]))
    
    (is (= [[1 :name 2 :set "Bar"]]
           (index/inclusive-subsequence sorted-set-index
                                        [1 :name 2 nil nil])))))
