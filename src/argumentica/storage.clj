(ns argumentica.storage
  (:require [flow-gl.tools.trace :as trace]
            [flow-gl.debug :as debug])
  (:use [clojure.test]))

(defn create-node []
  {:values (sorted-set)})

(defn create []
  {:nodes {0 (create-node)}
   :next-node-id 1
   :root-id 0})


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

(defn split-root [storage]
  (let [old-root-id (:root-id storage)
        {:keys [lesser-sorted-set greater-sorted-set median-value]} (split-sorted-set (get-in storage [:nodes old-root-id :values]))
        new-root-id (:next-node-id storage)
        new-child-id (inc (:next-node-id storage))]
    (-> storage
        (update :next-node-id + 2)
        (assoc :root-id new-root-id)
        (assoc-in [:nodes old-root-id :values] lesser-sorted-set)
        (assoc-in [:nodes new-root-id] {:values (sorted-set median-value)
                                        :child-ids [old-root-id
                                                    new-child-id]})
        (assoc-in [:nodes new-child-id] {:values greater-sorted-set}))))



(deftest test-split-root
  (is (= {:nodes
          {0 {:values #{1 2}},
           1 {:values #{3}, :child-ids [0 2]},
           2 {:values #{4 5}}},
          :next-node-id 3,
          :root-id 1}
         (split-root {:nodes {0 {:values (sorted-set 1 2 3 4 5)}}
                      :next-node-id 1
                      :root-id 0}))))


(defn insert-after [sequence old-value new-value]
  (loop [head '()
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
                       3))))

(defn split-child [storage parent-id old-child-id]
  (let [{:keys [lesser-sorted-set greater-sorted-set median-value]} (split-sorted-set (get-in storage [:nodes old-child-id :values]))
        new-child-id (:next-node-id storage)]
    (-> storage
        (update :next-node-id inc)
        (assoc-in [:nodes old-child-id :values] lesser-sorted-set)
        (update-in [:nodes parent-id :values] conj median-value)
        (update-in [:nodes parent-id :child-ids] insert-after old-child-id new-child-id)
        (assoc-in [:nodes new-child-id] {:values greater-sorted-set}))))

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
                      0))))

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
      (if-let [next-child-id (get (:child-ids parent)
                                  (inc (find-index (:child-ids parent)
                                                   (last cursor))))]
        (conj (vec(drop-last cursor))
              next-child-id)
        (recur (drop-last cursor)))
      nil)))

(deftest test-next-cursor
  (is (= [1 2]
         (next-cursor {:nodes
                       {0 {:values (sorted-set -3 -2 -1 0 1)},
                        1 {:values (sorted-set 3), :child-ids [0 2]},
                        2 {:values (sorted-set 4 5 6)}}}
                      [1 0])))

  (is (= nil
         (next-cursor {:nodes
                       {0 {:values (sorted-set -3 -2 -1 0 1)},
                        1 {:values (sorted-set 3), :child-ids [0 2]},
                        2 {:values (sorted-set 4 5 6)}}}
                      [1 2]))))

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

(defn add-splitter [storage cursor values]
  (let [splitter (if (< 1 (count cursor))
                   (splitter-after-child (node storage
                                               (last (drop-last cursor)))
                                         (last cursor))
                   nil)]
    (concat values
            (if splitter
              [splitter]
              []))))

(defn sequence-for-cursor [storage cursor]
  (add-splitter storage
                cursor
                (:values (node storage (last cursor)))))

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
         :sequence (add-splitter storage
                                 cursor
                                 (subseq (:values the-node)
                                         >=
                                         value))}
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
            :sequence []}
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
                                  7)))))
#_(defn chunked-to-seq [n]
    (println n)
    (lazy-seq (chunk-cons (range n) (to-seq (inc n)))))

(comment
  (take 5 (to-seq 10))

  (take 5 (chunked-to-seq 10)))


#_(defn persist [storage store]
    
    )

#_(trace/with-trace-logging
    (trace/trace-var #'split-child)
    )

