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

(defn next-seq-from [storage value]
  (loop [path []
         node-id (:root-id storage)]
    (let [the-node (node storage
                         node-id)]
      (if (leaf-node? the-node)
        (subseq (:values the-node)
                >=
                value)
        (if-let [the-child-id (child-id the-node
                                        value)]
          (recur storage
                 the-child-id)
          (let [splitter-index (find-index (:values the-node)
                                           value)]
            (concat [value]
                    (:values (node storage
                                   (get-in the-node
                                           [:child-ids (inc splitter-index)]))))))))))

(deftest test-next-seq-from
  (is (= '(-2 -1 0 1)
         (next-seq-from {:nodes
                         {0 {:values (sorted-set -3 -2 -1 0 1)},
                          1 {:values (sorted-set 3), :child-ids [0 2]},
                          2 {:values (sorted-set 4 5 6)}},
                         :next-node-id 3,
                         :root-id 1}
                        -2)))

  (is (= '(3 4 5 6)
         (next-seq-from {:nodes
                         {0 {:values (sorted-set -3 -2 -1 0 1)},
                          1 {:values (sorted-set 3), :child-ids [0 2]},
                          2 {:values (sorted-set 4 5 6)}},
                         :next-node-id 3,
                         :root-id 1}
                        3)))

  (is (= '(6)
         (next-seq-from {:nodes
                         {0 {:values (sorted-set -3 -2 -1 0 1)},
                          1 {:values (sorted-set 3), :child-ids [0 2]},
                          2 {:values (sorted-set 4 5 6)}},
                         :next-node-id 3,
                         :root-id 1}
                        6))))

#_(defn seq-from [storage value]
    (lazy-seq (cons n (to-seq (inc n)))))

(defn chunked-to-seq [n]
  (println n)
  (lazy-seq (chunk-cons (range n) (to-seq (inc n)))))

(comment
  (take 5 (to-seq 10))

  (take 5 (chunked-to-seq 10)))


(defn persist [storage store]
  
  )

#_(trace/with-trace-logging
    (trace/trace-var #'split-child)
    )

