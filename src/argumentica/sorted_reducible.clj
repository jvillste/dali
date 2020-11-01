(ns argumentica.sorted-reducible
  (:require [schema.core :as schema]
            [clojure.test :refer :all]
            [argumentica.comparator :as comparator])
  (:import [clojure.lang IReduceInit IReduce Seqable Sequential ISeq]))

(defprotocol SortedReducible
  (subreducible-method [this starting-key direction]))

(extend-protocol SortedReducible
  clojure.lang.Sorted
  (subreducible-method [sorted starting-key direction]
    (if (= :forwards direction)
      (subseq sorted >= starting-key)
      (rsubseq sorted <= starting-key))))

(defn subreducible
  ([this]
   (subreducible-method this
                        nil
                        :forwards))
  ([this starting-key]
   (subreducible-method this
                        starting-key
                        :forwards))
  ([this starting-key direction]
   (subreducible-method this
                        starting-key
                        direction)))

(deftest test-subreducible
  (is (= [1 2 3]
         (subreducible (sorted-set 1 2 3))))

  (is (= [2 3]
         (subreducible (sorted-set 1 2 3)
                       2)))

  (is (= [2 1]
         (subreducible (sorted-set 1 2 3)
                       2
                       :backwards))))
