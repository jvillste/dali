(ns argumentica.db.sorted-datom-set-branch
  (:require [argumentica.index :as index]
            [argumentica.comparator :as comparator]
            [argumentica.util :as util]
            [argumentica.db.common :as common])
  (:use clojure.test))


(defrecord SortedDatomSetBranch [base-sorted-datom-set
                                 base-transaction-number
                                 datom-transaction-number-index
                                 branch-datom-set])

(util/defn-alias create ->SortedDatomSetBranch)

(defmethod index/add!
  SortedDatomSetBranch
  [this value]
  (index/add! (:branch-datom-set this)
              value))

(defn merge-sequences [seq-1 seq-2 comparator]
  (lazy-seq
   (cond
     (empty? seq-1)
     seq-2
     (empty? seq-2)
     seq-1
     (> 0 (comparator (first seq-1) (first seq-2)))
     (cons (first seq-1) (merge-sequences (rest seq-1) seq-2 comparator))
     :else
     (cons (first seq-2) (merge-sequences seq-1 (rest seq-2) comparator)))))

(deftest test-merge-sequences
  (is (= '(1 2)
         (merge-sequences [1]
                          [2]
                          compare)))

  (is (= '(1 2)
         (merge-sequences [2]
                          [1]
                          compare)))

  (is (= '(1 2 3 4 5)
         (merge-sequences [2 4]
                          [1 3 5]
                          compare)))

  (is (= '(1 2 3 4 5 6)
         (merge-sequences [2 4 5]
                          [1 3 6]
                          compare))))


(defmethod index/inclusive-subsequence
  SortedDatomSetBranch
  [this key]
  (merge-sequences (filter (fn [datom]
                             (>= (:base-transaction-number this)
                                 (get datom (:datom-transaction-number-index this))))
                           (index/inclusive-subsequence (:base-sorted-datom-set this)
                                                        key))
                   (index/inclusive-subsequence (:branch-datom-set this)
                                                key)
                   comparator/compare-datoms))
