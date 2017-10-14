(ns argumentica.storage
  (:use [clojure.test]))

(defn create-node []
  {:values (sorted-set)})

(defn create []
  {:nodes {0 (create-node)}
   :next-node-id 1
   :root 0})

(defn full? [node]
  (> 3 (count (:values node))))

(defn median-value [values]
  (get values
       (int (/ (count values)
               2))))

(deftest test-median-value
  (is (= 2
         (median-value [1 2 3])))

  (is (= 3
         (median-value [1 2 3 4 5]))))

(defn split-sorted-set [source-sorted-set]
  (let [median-value (median-value (vec source-sorted-set))]
    {:lesser-sorted-set (into (sorted-set)
                              (subseq source-sorted-set
                                      <
                                      median-value))
     :greater-sorted-set (into (sorted-set)
                               (subseq source-sorted-set
                                       >
                                       median-value))
     :median-value median-value}))

(deftest test-split-sorted-set
  (is (= {:lesser-sorted-set #{1 2}
          :greater-sorted-set #{4 5}
          :median-value 3}
         (split-sorted-set (sorted-set 1 2 3 4 5)))))

(defn split-child [node child-index]
  
  )

(defn add [storage value]
  )
