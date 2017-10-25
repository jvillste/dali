(ns argumentica.storage
  (:require [flow-gl.tools.trace :as trace]
            [flow-gl.debug :as debug])
  (:use [clojure.test]))

(defn create-node []
  {:values (sorted-set)})

(defn full-after-maximum-number-of-values [maximum]
  (fn [node]
    (= maximum
       (count (:values node)))))

(defn create
  ([]
   (create (full-after-maximum-number-of-values 5)))
  
  ([full?]
   {:nodes {0 (create-node)}
    :next-node-id 1
    :root-id 0
    :full? full?}))




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
    {:lesser-values (into (sorted-set)
                          (subseq source-sorted-set
                                  <
                                  median-value))
     :greater-values (into (sorted-set)
                           (subseq source-sorted-set
                                   >
                                   median-value))
     :median-value median-value}))

(deftest test-split-sorted-set
  (is (= {:lesser-values #{1 2}
          :greater-values #{4 5}
          :median-value 3}
         (split-sorted-set (sorted-set 1 2 3 4 5)))))

(defn distribute-children [storage old-node-id new-node-id]
  (if-let [child-ids (get-in storage [:nodes old-node-id :child-ids])]
    (let [[lesser-child-ids greater-child-ids] (partition (/ (count child-ids)
                                                             2)
                                                          child-ids)]
      (-> storage
          (assoc-in [:nodes old-node-id :child-ids] (vec lesser-child-ids))
          (assoc-in [:nodes new-node-id :child-ids] (vec greater-child-ids))))
    storage))

(defn insert-after [sequence old-value new-value]
  (loop [head []
         tail sequence]
    (if-let [value (first tail)]
      (if (= value old-value)
        (concat head (list value new-value) (rest tail))
        (recur (conj head value)
               (rest tail)))
      (conj head
            new-value))))


(deftest test-insert-after
  (is (= '(1 2 3 4)
         (insert-after '(1 2 4)
                       2
                       3)))

  (is (= '(0 2 3 4)
         (insert-after '(0 2 3)
                       3
                       4))))

(defn split-child [storage parent-id old-child-id]
  (let [{:keys [lesser-values greater-values median-value]} (split-sorted-set (get-in storage [:nodes old-child-id :values]))
        new-child-id (:next-node-id storage)]
    (-> storage
        (update :next-node-id inc)
        (assoc-in [:nodes old-child-id :values] lesser-values)
        (update-in [:nodes parent-id :values] conj median-value)
        (update-in [:nodes parent-id :child-ids] (fn [child-ids]
                                                   (vec (insert-after child-ids
                                                                      old-child-id
                                                                      new-child-id))) )
        (assoc-in [:nodes new-child-id] {:values greater-values})
        (distribute-children old-child-id new-child-id))))

(deftest test-split-child
  (is (= {:nodes {0 {:values #{1}},
                  1 {:values #{2 3}, :child-ids '(0 3 2)},
                  2 {:values #{4 5}},
                  3 {:values #{3}}},
          :next-node-id 4,
          :root-id 1}
         (split-child {:nodes {0 {:values (sorted-set 1 2 3)},
                               1 {:values (sorted-set 3), :child-ids [0 2]},
                               2 {:values (sorted-set 4 5)}},
                       :next-node-id 3,
                       :root-id 1}
                      1
                      0)))

  (is (= {:nodes
          {0 {:values #{0}},
           7 {:values #{8}},
           1 {:values #{1}, :child-ids [0 2]},
           4 {:values #{6}},
           6 {:values #{5}, :child-ids [3 4]},
           3 {:values #{4}},
           2 {:values #{2}},
           9 {:values #{9}, :child-ids [7 8]},
           5 {:values #{3 7}, :child-ids [1 6 9]},
           8 {:values #{10 11}}},
          :next-node-id 10,
          :root-id 5}
         (split-child {:nodes {0 {:values (sorted-set 0)}
                               1 {:values (sorted-set 1), :child-ids [0 2]}
                               2 {:values (sorted-set 2)}
                               3 {:values (sorted-set 4)}
                               4 {:values (sorted-set 6)}
                               5 {:values (sorted-set 3), :child-ids [1 6]}
                               6 {:values (sorted-set 5 7 9), :child-ids [3 4 7 8]}
                               7 {:values (sorted-set 8)}
                               8 {:values (sorted-set 10 11)}}
                       :next-node-id 9,
                       :root-id 5}
                      5
                      6))))

(defn split-root [storage]
  (let [new-root-id (:next-node-id storage)]
    (-> storage
        (update :next-node-id inc)
        (assoc :root-id new-root-id)
        (assoc-in [:nodes new-root-id] {:child-ids [(:root-id storage)]
                                        :values (sorted-set)})
        (split-child new-root-id (:root-id storage)))))

(deftest test-split-root
  (is (= {:nodes
          {0 {:values #{1 2}},
           1 {:values #{3}, :child-ids [0 2]},
           2 {:values #{4 5}}},
          :next-node-id 3,
          :root-id 1}
         (split-root {:nodes {0 {:values (sorted-set 1 2 3 4 5)}}
                      :next-node-id 1
                      :root-id 0})))

  (is (= {:nodes {0 {:values (sorted-set 0)}
                  1 {:values (sorted-set 1)
                     :child-ids [0 2]}
                  2 {:values (sorted-set 2)}
                  3 {:values (sorted-set 4)}
                  4 {:values (sorted-set 6 7)}
                  5 {:values (sorted-set 3)
                     :child-ids [1 6]}
                  6 {:values (sorted-set 5)
                     :child-ids [3 4]}},
          :next-node-id 7,
          :root-id 5}
         (split-root {:nodes {0 {:values (sorted-set 0)}
                              1 {:values (sorted-set 1 3 5), :child-ids [0 2 3 4]}
                              2 {:values (sorted-set 2)}
                              3 {:values (sorted-set 4)}
                              4 {:values (sorted-set 6 7)}},
                      :next-node-id 5,
                      :root-id 1}))))






(defn child-index [splitter-values value]
  (loop [splitter-values splitter-values
         child-index 0]
    (if-let [splitter-value (first splitter-values)]
      (if (= splitter-value
             value)
        nil
        (if (< value
               splitter-value)
          child-index
          (recur (rest splitter-values)
                 (inc child-index))))
      child-index)))

(deftest test-select-child-index
  (is (= 0
         (child-index [2 4] 1)))
  (is (= 1
         (child-index [2 4] 3)))
  (is (= 2
         (child-index [2 4] 5)))
  (is (= nil
         (child-index [2 4] 2))))

(defn child-id [parent-node value]
  (if-let [the-child-index (child-index (:values parent-node)
                                        value)]
    (nth (:child-ids parent-node)
         the-child-index)
    nil))

(deftest test-child-id
  (is (= 3
         (child-id {:values #{-1 3}, :child-ids '(0 3 2)}
                   2))))

(defn find-index [sequence value]
  (first (keep-indexed (fn [index sequence-value]
                         (if (= value sequence-value)
                           index
                           nil))
                       sequence)))

(deftest test-find-index
  (is (= 2
         (find-index [1 2 3] 3)))
  (is (= nil
         (find-index [1 2 3] 4))))

(defn leaf-node? [node]
  (not (:child-ids node)))

(defn node [storage node-id]
  (get-in storage [:nodes node-id]))

(defn add [storage value]
  (loop [storage (if ((:full? storage) (node storage
                                             (:root-id storage)))
                   (split-root storage)
                   storage)
         previous-node-id nil
         node-id (:root-id storage)]
    (let [the-node (node storage
                         node-id)]
      (if (leaf-node? the-node)
        (update-in storage
                   [:nodes node-id :values]
                   conj value)
        (if-let [the-child-id (child-id the-node
                                        value)]
          (let [storage (if ((:full? storage) (node storage
                                                    the-child-id))
                          (split-child storage
                                       node-id
                                       the-child-id)
                          storage)]
            (if-let [the-child-id (child-id (node storage
                                                  node-id)
                                            value)]
              (recur storage
                     node-id
                     the-child-id)
              storage))
          storage)))))


(deftest test-add
  (let [full? (fn [node]
                (= 5 (count (:values node))))]
    (testing "root is full"
      (is (= {:nodes
              {0 {:values #{1 2}},
               1 {:values #{3}, :child-ids [0 2]},
               2 {:values #{4 5 6}}},
              :next-node-id 3,
              :root-id 1,
              :full? full?}
             (add {:nodes {0 {:values (sorted-set 1 2 3 4 5)}}
                   :next-node-id 1
                   :root-id 0
                   :full? full?}
                  6))))

    (testing "no splits needed"
      (is (= {:nodes
              {0 {:values (sorted-set -1 1 2)},
               1 {:values (sorted-set 3), :child-ids [0 2]},
               2 {:values (sorted-set 4 5 6)}},
              :next-node-id 3,
              :root-id 1,
              :full? full?}
             (add {:nodes
                   {0 {:values (sorted-set 1 2)},
                    1 {:values (sorted-set 3), :child-ids [0 2]},
                    2 {:values (sorted-set 4 5 6)}},
                   :next-node-id 3,
                   :root-id 1,
                   :full? full?}
                  -1))))

    (testing "leaf is full"
      (is (= {:nodes
              {0 {:values #{-3 -2}},
               1 {:values #{-1 3}, :child-ids '(0 3 2)},
               2 {:values #{4 5 6}},
               3 {:values #{0 1 2}}},
              :next-node-id 4,
              :root-id 1,
              :full? full?}
             (add {:nodes
                   {0 {:values (sorted-set -3 -2 -1 0 1)},
                    1 {:values (sorted-set 3), :child-ids [0 2]},
                    2 {:values (sorted-set 4 5 6)}},
                   :next-node-id 3,
                   :root-id 1,
                   :full? full?}
                  2))))))

(defn next-cursor [storage cursor]
  (loop [cursor cursor]
    (if-let [parent (node storage
                          (last (drop-last cursor)))]
      (if-let [next-node-id-downwards (get (:child-ids parent)
                                           (inc (find-index (:child-ids parent)
                                                            (last cursor))))]
        (loop [cursor (conj (vec (drop-last cursor))
                            next-node-id-downwards)]
          (let [next-node-downwards (node storage
                                          (last cursor))]
            (if (leaf-node? next-node-downwards)
              cursor
              (recur (conj cursor
                           (first (:child-ids next-node-downwards)))))))
        
        (recur (drop-last cursor)))
      nil)))

(deftest test-next-cursor
  (is (= [1 2]
         (next-cursor {:nodes
                       {1 {:child-ids [0 2]}}}
                      [1 0])))

  (is (= nil
         (next-cursor {:nodes
                       {1 {:child-ids [0 2]}}}
                      [1 2])))


  (is (= [1 2]
         (next-cursor {:nodes {1 {:child-ids [0 2 3]}}
                       :next-node-id 4
                       :root-id 1}
                      [1 0])))

  (is (= [5 1 2]
         (next-cursor {:nodes {1 {:child-ids [0 2]}
                               5 {:child-ids [1 6]}
                               6 {:child-ids [3 4]}},
                       :next-node-id 7
                       :root-id 5}
                      [5 1 0])))

  (is (= [5 6 3]
         (next-cursor {:nodes {1 {:child-ids [0 2]}
                               5 {:child-ids [1 6]}
                               6 {:child-ids [3 4]}},
                       :next-node-id 7
                       :root-id 5}
                      [5 1 2]))))



(defn splitter-after-child [node child-id]
  ;; TODO: This is linear time. Could we find the last value in
  ;; the child node and then find the splitter both in logarithmic time?
  (nth (seq (:values node))
       (find-index (:child-ids node)
                   child-id)
       nil))

(deftest test-splitter-after-child
  (is (= 3
         (splitter-after-child {:values (sorted-set 3)
                                :child-ids [0 2]}
                               0)))
  (is (= nil
         (splitter-after-child {:values (sorted-set 3)
                                :child-ids [0 2]}
                               2)))

  (is (= nil
         (splitter-after-child {:values (sorted-set 3)
                                :child-ids [0 2]}
                               2))))



(defn drop-until-equal [sequence value]
  (drop-while #(not= % value)
              sequence))


(defn children-after [parent child-id]
  (rest (drop-until-equal (:child-ids parent)
                          child-id)))

(defn splitter-after-cursor [storage cursor]
  (loop [cursor cursor]
    (if-let [parent (node storage
                          (last (drop-last cursor)))]
      (if (empty? (children-after parent
                                  (last cursor)))
        (recur (drop-last cursor))
        (splitter-after-child parent
                              (last cursor)))
      nil)))


(deftest test-splitter-after-cursor
  (let [storage {:nodes {0 {:values (sorted-set 0)}
                         1 {:values (sorted-set 1)
                            :child-ids [0 2]}
                         2 {:values (sorted-set 2)}
                         3 {:values (sorted-set 4)}
                         4 {:values (sorted-set 6 7 8)}
                         5 {:values (sorted-set 3)
                            :child-ids [1 6]}
                         6 {:values (sorted-set 5)
                            :child-ids [3 4]}},
                 :next-node-id 7,
                 :root-id 5}]
    (is (= 3
           (splitter-after-cursor storage
                                  [5 1 2])))

    (is (= nil
           (splitter-after-cursor storage
                                  [5 6 4])))

    (is (= 5
           (splitter-after-cursor storage
                                  [5 6 3]))))


  (let [storage {:nodes
                 {0 {:values (sorted-set 1 2)},
                  1 {:values (sorted-set 3)
                     :child-ids [0 2]},
                  2 {:values (sorted-set 4 5 6)}}
                 :root-id 1}]

    (is (= 3
           (splitter-after-cursor storage
                                  [1 0])))

    (is (= nil
           (splitter-after-cursor storage
                                  [1 2])))))

(defn append-if-not-null [collection value]
  (if value
    (concat collection
            [value])
    collection))

(defn sequence-for-cursor [storage cursor]
  (append-if-not-null (seq (:values (node storage
                                          (last cursor))))
                      (splitter-after-cursor storage
                                             cursor)))

(deftest test-sequence-for-cursor
  (is (= [1 2 3]
         (sequence-for-cursor {:nodes
                               {0 {:values (sorted-set 1 2)},
                                1 {:values (sorted-set 3), :child-ids [0 2]},
                                2 {:values (sorted-set 4 5 6)}}}
                              [1 0])))
  (is (= [4 5 6]
         (sequence-for-cursor {:nodes
                               {0 {:values (sorted-set 1 2)},
                                1 {:values (sorted-set 3), :child-ids [0 2]},
                                2 {:values (sorted-set 4 5 6)}}}
                              [1 2]))))

(defn cursor-and-sequence-for-value [storage value]
  (loop [cursor [(:root-id storage)]
         node-id (:root-id storage)]
    (let [the-node (node storage
                         node-id)]
      (if (leaf-node? the-node)
        {:cursor (next-cursor storage
                              cursor)
         :sequence (append-if-not-null (subseq (:values the-node)
                                               >=
                                               value)
                                       (splitter-after-cursor storage
                                                              cursor))}
        (if-let [the-child-id (child-id the-node
                                        value)]
          (recur (conj cursor the-child-id)
                 the-child-id)
          {:cursor (if-let [child-id (get (:child-ids the-node)
                                          (inc (find-index (:values the-node)
                                                           value)))]
                     (conj cursor
                           child-id)
                     cursor)
           :sequence [value]})))))

(deftest test-cursor-and-sequence-for-value
  (let [storage {:nodes
                 {0 {:values (sorted-set 1 2)},
                  1 {:values (sorted-set 3)
                     :child-ids [0 2]},
                  2 {:values (sorted-set 4 5 6)}}
                 :root-id 1}]

    (is (= {:cursor [1 2]
            :sequence '(1 2 3)}
           (cursor-and-sequence-for-value storage
                                          0)))
    
    (is (= {:cursor [1 2]
            :sequence '(1 2 3)}
           (cursor-and-sequence-for-value storage
                                          1)))

    (is (= {:cursor [1 2]
            :sequence [3]}
           (cursor-and-sequence-for-value storage
                                          3)))

    (is (= {:cursor [1 2]
            :sequence [1 2 3]}
           (cursor-and-sequence-for-value storage
                                          -10)))

    (is (= {:cursor nil
            :sequence [5 6]}
           (cursor-and-sequence-for-value storage
                                          5)))

    (is (= {:cursor nil
            :sequence nil}
           (cursor-and-sequence-for-value storage
                                          50)))))

(defn inclusive-subsequence-for-sequence-and-cursor [storage sequence cursor]
  (if-let [value (first sequence)]
    (lazy-seq (cons value (inclusive-subsequence-for-sequence-and-cursor storage
                                                                         (rest sequence)
                                                                         cursor)))
    (if cursor
      (inclusive-subsequence-for-sequence-and-cursor storage
                                                     (sequence-for-cursor storage
                                                                          cursor)
                                                     (next-cursor storage
                                                                  cursor))
      [])))

(defn inclusive-subsequence [storage value]
  (clojure.pprint/pprint storage)
  (let [{:keys [sequence cursor]} (cursor-and-sequence-for-value storage
                                                                 value)]

    (inclusive-subsequence-for-sequence-and-cursor storage
                                                   sequence
                                                   cursor)))

(deftest test-inclusive-subsequence
  (let [storage {:nodes
                 {0 {:values (sorted-set 1 2)},
                  1 {:values (sorted-set 3)
                     :child-ids [0 2]},
                  2 {:values (sorted-set 4 5 6)}}
                 :root-id 1}]
    (is (= [1 2 3 4 5 6]
           (inclusive-subsequence storage
                                  0)))

    (is (= [2 3 4 5 6]
           (inclusive-subsequence storage
                                  2)))

    (is (= [3 4 5 6]
           (inclusive-subsequence storage
                                  3)))

    (is (= [4 5 6]
           (inclusive-subsequence storage
                                  4)))

    (is (= []
           (inclusive-subsequence storage
                                  7)))

    (let [values (range 200)]
      (is (= values
             (inclusive-subsequence (reduce add
                                            (create (full-after-maximum-number-of-values 3))
                                            values)
                                    (first values)))))))

(deftest test-storage
  (repeatedly 100 
              (let [values (take 200 (repeatedly (fn [] (rand-int 1000))))
                    smallest (rand 1000)]
                (is (= (subseq (apply sorted-set values)
                               >=
                               smallest)
                       (inclusive-subsequence (reduce add
                                                      (create (full-after-maximum-number-of-values 3))
                                                      values)
                                              smallest))))))

