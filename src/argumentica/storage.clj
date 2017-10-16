(ns argumentica.storage
  (:use [clojure.test]))

(defn create-node []
  {:values (sorted-set)})

(defn create []
  {:nodes {0 (create-node)}
   :next-node-id 1
   :root-id 0})

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

(defn split-child [storage node-id child-index]
  
  )

(defn add-value [storage node-id value]
  )

(defn split-root [storage]
  (let [old-root-id (:root-id storage)
        root (get-in storage [:nodes old-root-id])
        {:keys [lesser-sorted-set greater-sorted-set median-value]} (split-sorted-set (:values root))
        new-root-id (:next-node-id storage)
        new-child-id (inc (:next-node-id storage))]
    (-> storage
        (update-in [:next-node-id] + 2)
        (assoc :root-id new-root-id)
        (assoc-in [:nodes old-root-id :values] {:values lesser-sorted-set})
        (assoc-in [:nodes new-root-id] {:values (sorted-set median-value)
                                        :children [old-root-id
                                                   new-child-id]})
        (assoc-in [:nodes new-child-id] {:values greater-sorted-set}))))


(deftest test-split-root
  (is (= {:nodes
          {0 {:values {:values #{1 2}}},
           1 {:values #{3}, :children [0 2]},
           2 {:values #{4 5}}},
          :next-node-id 3,
          :root-id 1}
         (split-root {:nodes {0 {:values (sorted-set 1 2 3 4 5)}}
                      :next-node-id 1
                      :root-id 0}))))



(defn add [storage value]
    (let [root (get-in storage [:nodes (:root-id storage)])]
      (if (full? root)
        (let [storage (split-root storage)]
          )
        
        ))
    )

