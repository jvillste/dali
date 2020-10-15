(ns argumentica.btree
  (:require [argumentica.comparator :as comparator]
            [argumentica.cryptography :as cryptography]
            [argumentica.encode :as encode]
            [argumentica.hash-map-storage :as hash-map-storage]
            [argumentica.match :as match]
            [argumentica.storage :as storage]
            [argumentica.zip :as zip]
            [clojure.data.priority-map :as priority-map]
            [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as properties]
            [argumentica.log :as log]
            [argumentica.util :as util]
            [argumentica.transducible-collection :as transducible-collection]
            [argumentica.node-serialization :as node-serialization]
            [medley.core :as medley]
            [argumentica.transducing :as transducing])
  (:import java.io.ByteArrayInputStream))

(defn create-sorted-set [& keys]
  (apply sorted-set-by
         comparator/compare-datoms
         keys))

(defn child-map [& entries]
  (apply sorted-map-by
         comparator/compare-datoms
         entries))

(defn create-node []
  {:values (create-sorted-set)})

(defn full-after-maximum-number-of-values [maximum]
  (assert (odd? maximum)
          "Maximum node size must be odd")
  (assert (< 1 maximum)
          "Maximum node size must be creater than one")
  (fn [node]
    (= maximum
       (count (:values node)))))


(defn roots-from-metadata-storage [metadata-storage]
  (or (storage/get-edn-from-storage! metadata-storage
                                     "roots")
      #{}))

(defn roots [btree]
  (roots-from-metadata-storage (:metadata-storage btree)))

(def descending #(compare %2 %1))

(defn latest-of-roots [roots]
  (first (sort-by :stored-time
                  descending
                  roots)))

(defn get-latest-root [btree]
  (latest-of-roots (roots-from-metadata-storage (:metadata-storage btree))))

(defn last-transaction-number [btree]
  (-> (get-latest-root btree)
      :metadata
      :last-transaction-number))

(defn create-from-options [& {:keys [full?
                                     node-storage
                                     metadata-storage
                                     latest-root]
                              :or {full? (full-after-maximum-number-of-values 1001)
                                   metadata-storage (hash-map-storage/create)
                                   node-storage (hash-map-storage/create)
                                   latest-root :no-latest-root-given}}]

  (conj {:full? full?
         :node-storage node-storage
         :metadata-storage metadata-storage
         :usages (priority-map/priority-map)}
        (if-let [latest-root (if (= latest-root
                                    :no-latest-root-given)
                               (latest-of-roots (roots-from-metadata-storage metadata-storage))
                               latest-root)]
          {:latest-root latest-root
           :nodes {}
           :next-node-id 0
           :root-id (:storage-key latest-root)}
          {:nodes {0 (create-node)}
           :next-node-id 1
           :root-id 0})))

(defn create-from-options-2 [& {:keys [full?
                                       node-storage
                                       metadata-storage
                                       latest-root]
                                :or {full? (full-after-maximum-number-of-values 1001)
                                     metadata-storage (hash-map-storage/create)
                                     node-storage (hash-map-storage/create)
                                     latest-root nil}}]

  (conj {:full? full?
         :node-storage node-storage
         :metadata-storage metadata-storage
         :usages (priority-map/priority-map)}
        (if-let [latest-root (or latest-root
                                 (latest-of-roots (roots-from-metadata-storage metadata-storage)))]
          {:latest-root latest-root
           :root {:storage-key (:storage-key latest-root)}}
          {:root (create-node)})))

(defn create
  ([]
   (create (full-after-maximum-number-of-values 1001)
           (hash-map-storage/create)))

  ([full?]
   (create full?
           (hash-map-storage/create)))

  ([full? storage]
   (create-from-options :full? full?
                        :node-storage storage))

  ([full? storage root-storage-key]
   (create-from-options :full? full?
                        :node-storage storage
                        :root-id root-storage-key)))



(comment
  (create-from-options :full? max))


(defn loaded-node-id? [node-id]
  (number? node-id))

(defn storage-key? [node-id]
  (string? node-id))

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
    {:lesser-values (into (create-sorted-set)
                          (subseq source-sorted-set
                                  <
                                  median-value))
     :greater-values (into (create-sorted-set)
                           (subseq source-sorted-set
                                   >
                                   median-value))
     :median-value median-value}))

(deftest test-split-sorted-set
  (is (= {:lesser-values #{1 2}
          :greater-values #{4 5}
          :median-value 3}
         (split-sorted-set (create-sorted-set 1 2 3 4 5)))))

(defn distribute-children [btree old-node-id new-node-id]
  (if-let [child-ids (get-in btree [:nodes old-node-id :child-ids])]
    (let [[lesser-child-ids greater-child-ids] (partition (/ (count child-ids)
                                                             2)
                                                          child-ids)]
      (-> btree
          (assoc-in [:nodes old-node-id :child-ids] (vec lesser-child-ids))
          (assoc-in [:nodes new-node-id :child-ids] (vec greater-child-ids))))
    btree))

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

(defn record-usage [btree node-id]
  (-> btree
      (update :usages
              assoc node-id (or (:next-usage-number btree)
                                0))
      (update :next-usage-number
              (fnil inc 0))))


(defn record-usage-2 [btree path]
  (-> btree
      (update :usages
              assoc (vec path) (or (:next-usage-number btree)
                                   0))
      (update :next-usage-number
              (fnil inc 0))))

(defn insert-to-vector [vector index value]
  (into (vec (take index vector))
        (concat [value]
                (drop index vector))))

(deftest test-insert-to-vector
  (is (= [1 :x 2 3]
         (insert-to-vector [1 2 3] 1 :x))))

(defn split-child [btree parent-id old-child-id]
  (let [{:keys [lesser-values greater-values median-value]} (split-sorted-set (get-in btree [:nodes old-child-id :values]))
        new-child-id (:next-node-id btree)]
    (-> btree
        (update :next-node-id inc)
        (record-usage new-child-id)
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
          :root-id 1
          :usages {3 0},
          :next-usage-number 1}
         (split-child {:nodes {0 {:values (create-sorted-set 1 2 3)},
                               1 {:values (create-sorted-set 3), :child-ids [0 2]},
                               2 {:values (create-sorted-set 4 5)}},
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
          :root-id 5
          :usages {9 0},
          :next-usage-number 1}
         (split-child {:nodes {0 {:values (create-sorted-set 0)}
                               1 {:values (create-sorted-set 1), :child-ids [0 2]}
                               2 {:values (create-sorted-set 2)}
                               3 {:values (create-sorted-set 4)}
                               4 {:values (create-sorted-set 6)}
                               5 {:values (create-sorted-set 3), :child-ids [1 6]}
                               6 {:values (create-sorted-set 5 7 9), :child-ids [3 4 7 8]}
                               7 {:values (create-sorted-set 8)}
                               8 {:values (create-sorted-set 10 11)}}
                       :next-node-id 9,
                       :root-id 5}
                      5
                      6))))

(defn- new-node-path [splitted-node-path]
  (conj (vec (drop-last splitted-node-path))
        (inc (last splitted-node-path))))

(deftest test-new-node-path
  (is (= [0 1 1] (new-node-path [0 1 0]))))

(defn parent-path [child-path]
  (vec (drop-last 2 child-path)))

(defn split-sequence-in-three [sequence]
  (let [count (count sequence)
        middle-index (int (/ count 2 ))]

    (assert (odd? count))

    [(take middle-index
           sequence)

     (nth sequence
          middle-index)

     (drop (inc middle-index)
           sequence)]))

(deftest test-split-sequence-in-three
  (is (= '[(1) 2 (3)]
         (split-sequence-in-three [1 2 3]))))

(defn assoc-if-not-empty [map key value]
  (if (empty? value)
    map
    (assoc map key value)))

(defn split-sequence-in-two [sequence]
  (let [half-count (int (/ (count sequence)
                           2))]
    [(take half-count sequence)
     (drop half-count sequence)]))

(deftest test-split-sequence-in-two
  (is (= '[(1 2) (3 4)]
         (split-sequence-in-two [1 2 3 4])))

  (is (= '[(1) (2 3)]
         (split-sequence-in-two [1 2 3]))))

(defn split-child-2 [btree child-path]
  (let [old-node (get-in btree child-path)
        [lesser-values median-value greater-values] (split-sequence-in-three (seq (:values old-node)))
        new-node-path (new-node-path child-path)
        [lesser-children greater-children] (split-sequence-in-two (:children old-node))]
    (-> btree
        (record-usage-2 new-node-path) ;; TODO: could this be somewhere elese? Do we need it?
        (update-in child-path
                   (fn [old-node]
                     (-> old-node
                         (assoc :values (apply create-sorted-set lesser-values))
                         (assoc-if-not-empty :children (vec lesser-children)))))
        (update-in (parent-path child-path)
                   (fn [parent-node]
                     (-> parent-node
                         (update :values conj median-value)
                         (update :children
                                 insert-to-vector
                                 (inc (last child-path))
                                 (-> {:values (apply create-sorted-set greater-values)}
                                     (assoc-if-not-empty :children (vec greater-children))))))))))

(deftest test-split-child-2
  (is (= {:root {:values #{2 4},
                 :children [{:values #{1}}
                            {:values #{3}}
                            {:values #{5}}]},
          :usages {[:root :children 1] 0},
          :next-usage-number 1}
         (split-child-2 {:root {:values (create-sorted-set 4)
                                :children [{:values (create-sorted-set 1 2 3)}
                                           {:values (create-sorted-set 5)}]}}
                        [:root :children 0])))

  (is (= {:root
          {:values #{4 8},
           :children [{:values #{2},
                       :children [{:values #{1}}
                                  {:values #{3}}]}
                      {:values #{6}
                       :children [{:values #{5}}
                                  {:values #{7}}]}
                      {:values #{9}}]},
          :usages {[:root :children 1] 0},
          :next-usage-number 1}
         (split-child-2 {:root {:values (create-sorted-set 8)
                                :children [{:values (create-sorted-set 2 4 6)
                                            :children [{:values (create-sorted-set 1)}
                                                       {:values (create-sorted-set 3)}
                                                       {:values (create-sorted-set 5)}
                                                       {:values (create-sorted-set 7)}]}
                                           {:values (create-sorted-set 9)}]}}
                        [:root :children 0])))

  (is (= {:root {:children [{:values #{2}
                             :children [{:values #{1}} {:values #{3}}]}
                            {:values #{6}
                             :children [{:values #{5}} {:values #{7}}]}],
                 :values #{4}},
          :usages {[:root :children 1] 0},
          :next-usage-number 1}
         (split-child-2 {:root {:children [{:children [{:values (create-sorted-set 1)}
                                                       {:values (create-sorted-set 3)}
                                                       {:values (create-sorted-set 5)}
                                                       {:values (create-sorted-set 7)}]
                                            :values (create-sorted-set 2 4 6)}]
                                :values (create-sorted-set)}}
                        [:root :children 0]))))

(defn change-last [sequence new-last-value]
  (concat (drop-last sequence)
          [new-last-value]))


(defn split-node [btree path children-key splitter-from-children create-child-node]
  (let [old-node (get-in btree path)
        [lesser-children greater-children] (split-sequence-in-two (seq (children-key old-node)))
        new-node-splitter (splitter-from-children lesser-children)]
    (-> btree
        (record-usage-2 (change-last path new-node-splitter)) ;; TODO: could this be somewhere elese? Do we need it?
        (update-in (parent-path path)
                   update
                   :children
                   assoc

                   new-node-splitter
                   (create-child-node lesser-children)

                   (last path)
                   (create-child-node greater-children)))))

(defn split-inner-node [btree path]
  (split-node btree
              path
              :children
              (fn [children]
                (first (last children)))
              (fn [children]
                {:children (apply child-map (apply concat children))})))

(deftest test-split-inner-node
  (is (= {:root {:children {4 {:children {4 {:values #{1 2 3 4}}}},
                            :argumentica.comparator/max {:children {:argumentica.comparator/max {:values #{5 6}}}}}},
          :usages {[:root :children 4] 0},
          :next-usage-number 1}
         (split-inner-node {:root {:children (child-map ::comparator/max {:children (child-map 4 {:values (create-sorted-set 1 2 3 4)}
                                                                                               ::comparator/max {:values (create-sorted-set 5 6)})})}}
                           [:root :children ::comparator/max]))))

(defn split-leaf-node [btree path]
  (split-node btree
              path
              :values
              (fn [values]
                (last values))
              (fn [values]
                {:values (apply create-sorted-set values)})))

(deftest test-split-leaf-node
  (is (= {:root
          {:children
           #:argumentica.comparator{:max
                                    {:children
                                     {2 {:values #{1 2}},
                                      4 {:values #{3 4}},
                                      :argumentica.comparator/max
                                      {:values #{5 6}}}}}},
          :usages
          {[:root :children :argumentica.comparator/max :children 2] 0},
          :next-usage-number 1}
         (split-leaf-node {:root {:children (child-map ::comparator/max {:children (child-map 4 {:values (create-sorted-set 1 2 3 4)}
                                                                                              ::comparator/max {:values (create-sorted-set 5 6)})})}}
                          [:root :children ::comparator/max :children 4]))))

(defn inner-node? [node]
  (contains? node :children))

(defn leaf-node-3? [node]
  (not (inner-node? node)))

(defn split-child-3 [btree child-path]
  (let [old-node (get-in btree child-path)]
    (if (inner-node? old-node)
      (split-inner-node btree child-path)
      (split-leaf-node btree child-path))))

(deftest test-split-child-2
  (is (= {:root {:children {1 {:values #{1}},
                            3 {:values #{2 3}},
                            :argumentica.comparator/max {:values #{4 5}}}},
          :usages {[:root :children 1] 0},
          :next-usage-number 1}
         (split-child-3 {:root {:children (child-map 3 {:values (create-sorted-set 1 2 3)}
                                                     ::comparator/max {:values (create-sorted-set 4 5)})}}
                        [:root :children 3]))))

(defn add-root-to-usages [usages root-id]
  (apply priority-map/priority-map
         (mapcat (fn [[cursor priority]]
                   [(concat [root-id]
                            cursor)
                    priority])
                 usages)))

(deftest test-add-root-to-usages
  (is (= {'(1 2 3) 1, '(1 3 4) 2}
         (add-root-to-usages (priority-map/priority-map [2 3] 1
                                                        [3 4] 2)
                             1))))



(defn split-root [btree]
  (let [new-root-id (:next-node-id btree)]
    (-> btree
        (update :next-node-id inc)
        (assoc :root-id new-root-id)
        (assoc-in [:nodes new-root-id] {:child-ids [(:root-id btree)]
                                        :values (create-sorted-set)})
        (record-usage new-root-id)
        (split-child new-root-id (:root-id btree)))))

(deftest test-split-root
  (is (= {:nodes
          {0 {:values #{1 2}},
           1 {:values #{3}, :child-ids [0 2]},
           2 {:values #{4 5}}},
          :next-node-id 3,
          :usages {0 0, 1 1, 2 2},
          :next-usage-number 3
          :root-id 1}
         (split-root {:nodes {0 {:values (create-sorted-set 1 2 3 4 5)}}
                      :next-node-id 1
                      :next-usage-number 1
                      :usages (priority-map/priority-map 0 0)
                      :root-id 0})))

  (is (= {:nodes {0 {:values (create-sorted-set 0)}
                  1 {:values (create-sorted-set 1)
                     :child-ids [0 2]}
                  2 {:values (create-sorted-set 2)}
                  3 {:values (create-sorted-set 4)}
                  4 {:values (create-sorted-set 6 7)}
                  5 {:values (create-sorted-set 3)
                     :child-ids [1 6]}
                  6 {:values (create-sorted-set 5)
                     :child-ids [3 4]}},
          :next-node-id 7,
          :usages {0 0, 1 0, 4 0, 3 0, 2 0, 5 0, 6 1}
          :root-id 5
          :next-usage-number 2}
         (split-root {:nodes {0 {:values (create-sorted-set 0)}
                              1 {:values (create-sorted-set 1 3 5), :child-ids [0 2 3 4]}
                              2 {:values (create-sorted-set 2)}
                              3 {:values (create-sorted-set 4)}
                              4 {:values (create-sorted-set 6 7)}},
                      :usages (priority-map/priority-map 0 0
                                                         1 0
                                                         2 0
                                                         3 0
                                                         4 0)
                      :next-node-id 5,
                      :root-id 1}))))

(defn split-root-2 [btree]
  (-> btree
      (update :root
              (fn [old-root-node]
                {:children [old-root-node]
                 :values (create-sorted-set)}))
      (record-usage-2 [:root])
      (split-child-2 [:root :children 0])))

(deftest test-split-child-2
  (is (= {:root {:children [{:values #{1}}
                            {:values #{3}}]
                 :values #{2}},
          :usages {[:root] 0
                   [:root :children 1] 1}
          :next-usage-number 2}
         (split-root-2 {:root {:values (create-sorted-set 1 2 3)}})))

  (is (= {:root
          {:children [{:values #{2}
                       :children [{:values #{1}}
                                  {:values #{3}}]}
                      {:values #{6}
                       :children [{:values #{5}}
                                  {:values #{7}}]}]
           :values #{4}}
          :usages {[:root] 0
                   [:root :children 1] 1}
          :next-usage-number 2}
         (split-root-2 {:root {:values (create-sorted-set 2 4 6)
                               :children [{:values (create-sorted-set 1)}
                                          {:values (create-sorted-set 3)}
                                          {:values (create-sorted-set 5)}
                                          {:values (create-sorted-set 7)}]}}))))

(defn split-root-3 [btree]
  (-> btree
      (update :root
              (fn [old-root-node]
                {:children (child-map ::comparator/max old-root-node)}))
      (record-usage-2 [:root])
      (split-child-3 [:root :children ::comparator/max])))

(deftest test-split-child-3
  (is (= {:root {:children {1 {:values #{1}}
                            :argumentica.comparator/max {:values #{2 3}}}},
          :usages {[:root] 0
                   [:root :children 1] 1},
          :next-usage-number 2}
         (split-root-3 {:root {:values (create-sorted-set 1 2 3)}})))

  (is (= {:root
          {:children {1 {:children {1 {:values #{1}}}},
            :argumentica.comparator/max {:children {::comparator/max {:values #{2}}}}}},
          :usages {[:root] 0, [:root :children 1] 1},
          :next-usage-number 2}
         (split-root-3 {:root {:children (child-map 1 {:values (create-sorted-set 1)}
                                                    ::comparator/max {:values (create-sorted-set 2)})}}))))

(defn child-index [splitter-values value]
  (loop [splitter-values splitter-values
         child-index 0]
    (if-let [splitter-value (first splitter-values)]
      (if (= splitter-value
             value)
        nil
        (if (neg? (comparator/compare-datoms value
                                             splitter-value))
          child-index
          (recur (rest splitter-values)
                 (inc child-index))))
      child-index)))

(deftest test-child-index
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

(defn leaf-node-2? [node]
  (not (:children node)))

(defn node [btree node-id]
  (get-in btree [:nodes node-id]))

(defn add-value-in-node [btree node-id value]
  (-> btree
      (update-in [:nodes node-id :values]
                 conj value)
      (record-usage node-id)))

(defn add-value-in-node-2 [btree path value]
  (-> btree
      (update-in (concat path [:values])
                 conj value)
      (record-usage-2 path)))

(defn parent-id [cursor]
  (last (drop-last cursor)))


(def direction? #{:forwards :backwards})

(defn splitter-next-to-child [node child-id direction]
  (assert (direction? direction))

  ;; TODO: This is linear time. Could we find the last value in
  ;; the child node and then find the splitter both in logarithmic time?
  (nth (seq (:values node))
       (let [index (find-index (:child-ids node)
                               child-id)]
         (if (= direction :forwards)
           index
           (dec index)))
       nil))

(deftest test-splitter-next-to-child
  (is (= 3
         (splitter-next-to-child {:values (create-sorted-set 3)
                                  :child-ids [0 2]}
                                 0
                                 :forwards)))
  (is (= nil
         (splitter-next-to-child {:values (create-sorted-set 3)
                                  :child-ids [0 2]}
                                 2
                                 :forwards)))

  (is (= nil
         (splitter-next-to-child {:values (create-sorted-set 3)
                                  :child-ids [0 2]}
                                 0
                                 :backwards)))
  (is (= 3
         (splitter-next-to-child {:values (create-sorted-set 3)
                                  :child-ids [0 2]}
                                 2
                                 :backwards))))

(defn splitter-next-to-cursor [btree cursor direction]
  (assert (direction? direction))

  (loop [cursor cursor]
    (if-let [parent (node btree
                          (last (drop-last cursor)))]
      (if (if (= direction :forwards)
            (= (last cursor)
               (last (:child-ids parent)))
            (= (last cursor)
               (first (:child-ids parent))))
        (recur (drop-last cursor))
        (splitter-next-to-child parent
                                (last cursor)
                                direction))
      nil)))

(deftest test-splitter-next-to-cursor
  (let [btree {:nodes {0 {:values (create-sorted-set 0)}
                       1 {:values (create-sorted-set 1)
                          :child-ids [0 2]}
                       2 {:values (create-sorted-set 2)}
                       3 {:values (create-sorted-set 4)}
                       4 {:values (create-sorted-set 6 7 8)}
                       5 {:values (create-sorted-set 3)
                          :child-ids [1 6]}
                       6 {:values (create-sorted-set 5)
                          :child-ids [3 4]}},
               :next-node-id 7,
               :root-id 5}]
    (is (= 3
           (splitter-next-to-cursor btree
                                    [5 1 2]
                                    :forwards)))

    (is (= nil
           (splitter-next-to-cursor btree
                                    [5 6 4]
                                    :forwards)))

    (is (= 5
           (splitter-next-to-cursor btree
                                    [5 6 3]
                                    :forwards)))

    (is (= 1
           (splitter-next-to-cursor btree
                                    [5 1 2]
                                    :backwards))))


  (let [btree {:nodes
               {0 {:values (create-sorted-set 1 2)},
                1 {:values (create-sorted-set 3)
                   :child-ids [0 2]},
                2 {:values (create-sorted-set 4 5 6)}}
               :root-id 1}]

    (is (= 3
           (splitter-next-to-cursor btree
                                    [1 0]
                                    :forwards)))

    (is (= nil
           (splitter-next-to-cursor btree
                                    [1 2]
                                    :forwards)))))

(defn order-by-direction [direction values]
  (if (= :forwards direction)
    values
    (reverse values)))

(defn combine-splitter-and-values [values splitter direction]
  (if splitter
    (if (= :forwards direction)
      (concat values
              [splitter])
      (concat values
              [splitter]))
    (seq values)))

(defn sequence-for-cursor [btree cursor direction]
  (combine-splitter-and-values (order-by-direction direction
                                                   (seq (:values (node btree
                                                                       (last cursor)))))
                               (splitter-next-to-cursor btree
                                                        cursor
                                                        direction)
                               direction))

(deftest test-sequence-for-cursor
  (is (= [1 2 3]
         (sequence-for-cursor {:nodes
                               {0 {:values (create-sorted-set 1 2)},
                                1 {:values (create-sorted-set 3), :child-ids [0 2]},
                                2 {:values (create-sorted-set 4 5 6)}}}
                              [1 0]
                              :forwards)))

  (is (= [6 5 4 3]
         (sequence-for-cursor {:nodes
                               {0 {:values (create-sorted-set 1 2)},
                                1 {:values (create-sorted-set 3), :child-ids [0 2]},
                                2 {:values (create-sorted-set 4 5 6)}}}
                              [1 2]
                              :backwards)))

  (is (= [4 5 6]
         (sequence-for-cursor {:nodes
                               {0 {:values (create-sorted-set 1 2)},
                                1 {:values (create-sorted-set 3), :child-ids [0 2]},
                                2 {:values (create-sorted-set 4 5 6)}}}
                              [1 2]
                              :forwards))))

(defn loaded-node-count [btree]
  (count (keys (:nodes btree))))

(deftest test-loaded-node-count
  (is (= 3
         (loaded-node-count {:nodes {0 {:values #{0}}
                                     1 {:child-ids [0 2], :values #{1}}
                                     2 {:values #{2 3 4}}}}))))

(defn first-leaf-cursor [btree]
  (if (= 0 (loaded-node-count btree))
    nil
    (loop [cursor [(:root-id btree)]
           node-id (:root-id btree)]
      (let [the-node (node btree
                           node-id)]
        (if (leaf-node? the-node)
          cursor
          (let [first-child-id (first (:child-ids the-node))]
            (recur (conj cursor
                         first-child-id)
                   first-child-id)))))))

(deftest test-first-leaf-cursor
  (is (= [1 0]
         (first-leaf-cursor {:nodes {0 {:values (create-sorted-set 1 2)},
                                     1 {:values (create-sorted-set 3), :child-ids [0 2]},
                                     2 {:values (create-sorted-set 4 5 6)}}
                             :root-id 1}))))

(defn replace-node-id [btree parent-id old-child-id new-child-id]
  (if parent-id
    (update-in btree
               [:nodes parent-id :child-ids]
               (partial replace
                        {old-child-id new-child-id}))
    (assoc btree :root-id new-child-id)))

(defn node-to-bytes [node]
  (storage/edn-to-bytes node))

(comment
  (node-to-bytes {:a :b})
  (storage-key (node-to-bytes {:a :b}))
  (String. (zip/read-first-entry-from-zip (zip/byte-array-input-stream (node-to-bytes {:a :b})))
           "UTF-8"))

(defn bytes-to-node [byte-array]
  (-> (storage/bytes-to-edn byte-array)
      (update :values
              (fn [values]
                (into (create-sorted-set)
                      values)))))

(defn storage-key [bytes]
  (encode/base-16-encode (cryptography/sha-256 bytes)))

(deftest test-node-serialization
  (let [node (create-node)]
    (is (= node
           (bytes-to-node (node-to-bytes node))))))

(defn get-node-content [storage storage-key]
  (bytes-to-node (storage/get-from-storage! storage
                                            storage-key)))

(defn- store-node [btree the-storage-key the-node bytes]
  (when (not (storage/storage-contains? (:metadata-storage btree)
                                        the-storage-key))
    (storage/put-edn-to-storage! (:metadata-storage btree)
                                 the-storage-key
                                 (conj (select-keys the-node
                                                    [:child-ids])
                                       {:value-count (count (:values the-node))
                                        :storage-byte-count (count bytes)}))

    (storage/put-to-storage! (:node-storage btree)
                             the-storage-key
                             bytes)
    btree))

(defn- store-node-by-cursor [btree cursor]
  (let [the-node (node btree
                       (last cursor))
        bytes (node-to-bytes the-node)]
    (store-node btree
                (storage-key bytes)
                the-node
                bytes)))


(defn storage-contents [storage]
  (reduce (fn [result storage-key]
            (assoc result storage-key
                   (storage/get-edn-from-storage! storage
                                                  storage-key)))
          {}
          (storage/storage-keys! storage)))

(deftest test-store-node-by-cursor
  (let [nodes {1 {:values #{3},
                  :child-ids [0 2]},
               2 {:values #{4 5 6}}}
        resulting-btree (store-node-by-cursor {:nodes nodes
                                               :root-id 1
                                               :node-storage (hash-map-storage/create)
                                               :metadata-storage (hash-map-storage/create)}
                                              [1 2])]
    (is (= nodes (:nodes resulting-btree)))
    (is (= {"F413E2F25774977AB52D3713C2547AF016404BABB48F13CA0A4710D26DD510F0"
            {:values #{4 6 5}}}
           (storage-contents (:node-storage resulting-btree))))

    (is (= {"F413E2F25774977AB52D3713C2547AF016404BABB48F13CA0A4710D26DD510F0"
            {:value-count 3, :storage-byte-count 22}}
           (storage-contents (:metadata-storage resulting-btree))))))

(defn unload-node [btree cursor]
  (let [node-id-to-be-unloded (last cursor)
        the-parent-id (parent-id cursor)
        the-node (node btree
                       node-id-to-be-unloded)
        bytes (node-to-bytes the-node)
        the-storage-key (storage-key bytes)]

    (assert (or (leaf-node? the-node)
                (empty? (filter loaded-node-id?
                                (:child-ids the-node))))
            "Can not unload a node with loaded children")

    (store-node btree the-storage-key the-node bytes)

    (-> btree
        (replace-node-id the-parent-id
                         node-id-to-be-unloded
                         the-storage-key)
        (update :nodes dissoc node-id-to-be-unloded)
        (update :usages dissoc node-id-to-be-unloded))))

(deftest test-unload-node

  (is (thrown? AssertionError
               (unload-node {:nodes {0 {:values (create-sorted-set 1 2)},
                                     1 {:values (create-sorted-set 3), :child-ids [0 2]},
                                     2 {:values (create-sorted-set 4 5 6)}}
                             :root-id 1
                             :node-storage (hash-map-storage/create)
                             :metadata-storage (hash-map-storage/create)}
                            [1])))

  (is (match/contains-map? {:nodes {1 {:values #{3},
                                       :child-ids [match/any-string
                                                   2]},
                                    2 {:values #{4 5 6}}}}
                           (unload-node {:nodes {0 {:values (create-sorted-set 1 2)},
                                                 1 {:values (create-sorted-set 3), :child-ids [0 2]},
                                                 2 {:values (create-sorted-set 4 5 6)}}
                                         :root-id 1
                                         :node-storage (hash-map-storage/create)
                                         :metadata-storage (hash-map-storage/create)}
                                        [1 0])))

  (is (match/contains-map? {:nodes
                            {1 {:values #{3},
                                :child-ids [match/any-string
                                            match/any-string]}},
                            :root-id 1}
                           (unload-node {:nodes
                                         {1 {:values #{3},
                                             :child-ids ["B58E78A458A49C835829351A3853B584CA01124A1A96EB782BA23513124F01A7"
                                                         2]},
                                          2 {:values #{4 5 6}}},
                                         :node-storage (hash-map-storage/create)
                                         :metadata-storage (hash-map-storage/create)
                                         :root-id 1}
                                        [1 2])))

  (is (match/contains-map? {:nodes
                            {7 {:values #{9 8}},
                             4 {:values #{6}},
                             15 {:values #{7 5}, :child-ids [12 13 16]},
                             13 {:values #{6}},
                             6 {:values #{7 5}, :child-ids [3 4 7]},
                             3 {:values #{4}},
                             12 {:values #{4}},
                             11 {:values #{2}},
                             5 {:child-ids [match/any-string 6], :values #{3}},
                             14 {:child-ids [10 15], :values #{3}},
                             16 {:values #{9 8}},
                             10 {:child-ids [match/any-string 11], :values #{1}},
                             8 {:child-ids [match/any-string match/any-string], :values #{1}}},
                            :root-id 14,
                            :usages {7 16, 4 14, 15 30, 13 31, 6 13, 3 9, 12 26, 11 23, 5 12, 14 29, 16 33, 10 20},
                            :next-usage-number 34}
                           (unload-node {:nodes
                                         {7 {:values #{8 9}},
                                          4 {:values #{6}},
                                          15 {:values #{5 7}, :child-ids [12 13 16]},
                                          13 {:values #{6}},
                                          6 {:values #{5 7}, :child-ids [3 4 7]},
                                          3 {:values #{4}},
                                          12 {:values #{4}},
                                          11 {:values #{2}},
                                          9 {:values #{0}},
                                          5 {:child-ids ["796AD3EEF52891C9000283476C05418E626F8DB78CB80177180D0A33831EFC8C" 6], :values #{3}},
                                          14 {:child-ids [10 15], :values #{3}},
                                          16 {:values #{8 9}},
                                          10 {:child-ids [9 11], :values #{1}},
                                          8 {:child-ids ["C657A0DE1F4454290D14CAD2FC6472174DB247A474DC4AC7ED71F53D0669CE04"
                                                         "61C88379F997BA90CA05A124B47C52FB60649D0A281EC1892F2482D3BFFC4FFE"],
                                             :values #{1}}},
                                         :node-storage (hash-map-storage/create)
                                         :metadata-storage (hash-map-storage/create)
                                         :root-id 14,
                                         :usages {3 9, 5 12, 6 13, 4 14, 7 16, 9 19, 10 20, 11 23, 12 26, 14 29, 15 30, 13 31, 16 33},
                                         :next-usage-number 34}
                                        [14 10 9]))))

(defn- store-if-new! [storage storage-key bytes]
  (when (not (storage/storage-contains? storage
                                        storage-key))

    (storage/put-to-storage! storage
                             storage-key
                             bytes)))

(defn loaded? [node]
  (some? (:values node)))

(defn store-node [btree path]
  (let [the-node (get-in btree path)
        bytes (node-serialization/serialize the-node)
        the-storage-key (storage-key bytes)]

    (assert (empty? (filter (complement loaded?)
                            (:children the-node)))
            "Can not unload a node with loaded children")

    (store-if-new! (:node-storage btree)
                   the-storage-key
                   bytes)

    (-> btree
        (assoc-in (concat path [:storage-key]) the-storage-key)
        (update :usages dissoc path))))

(defn- extract-storage [storage]
  (into {} (for [storage-key (storage/storage-keys! storage)]
             [storage-key (node-serialization/deserialize (ByteArrayInputStream. (storage/get-from-storage! storage storage-key)))])))

(defn- extract-node-storage [btree]
  (update btree :node-storage extract-storage))

(deftest test-store-node
  (is (= {:root
          {:children
           [{:values #{2},
             :storage-key "4C9B597F8C171876E562EF78D7B7489B3C7616D0036ACA9988B647F867E7594E"}
            {:values #{4}}],
           :values #{3}},
          :node-storage {"4C9B597F8C171876E562EF78D7B7489B3C7616D0036ACA9988B647F867E7594E"
                         {:children [], :values #{2}}},
          :usages nil}
         (extract-node-storage (store-node {:root {:children [{:values (create-sorted-set 2)}
                                                              {:values (create-sorted-set 4)}]
                                                   :values (create-sorted-set 3)}
                                            :node-storage (hash-map-storage/create)}
                                           [:root :children 0])))))

(defn unload-node-2 [btree path]
  (-> btree
      (store-node path)
      (update-in path dissoc :values)))

(deftest test-unload-node-2
  (is (= {:root {:children
                 [{:storage-key "A32F78C54A49C7CA308E10AB0D84B96D7A60E29F8084E1722953462C29257623"}
                  {:values #{4}}],
                 :values #{3}},
          :node-storage {"A32F78C54A49C7CA308E10AB0D84B96D7A60E29F8084E1722953462C29257623"
                         {:values #{2}}},
          :usages {[:root :children 1] 1
                   [:root] 2}}
         (extract-node-storage (unload-node-2 (store-node {:root {:children [{:values (create-sorted-set 2)}
                                                                             {:values (create-sorted-set 4)}]
                                                                  :values   (create-sorted-set 3)}
                                                           :node-storage (hash-map-storage/create)
                                                           :usages (priority-map/priority-map [:root :children 0] 0
                                                                                              [:root :children 1] 1
                                                                                              [:root] 2)}
                                                          [:root :children 0])
                                              [:root :children 0])))))

(declare add)

(defn all-cursors [btree]
  (tree-seq (fn [cursor]
              (:child-ids (node btree (last cursor))))
            (fn [cursor]
              (for [child-id (:child-ids (node btree (last cursor)))]
                (concat cursor
                        [child-id])))
            [(:root-id btree)]))

(deftest test-all-cursors
  (is (= '([1]
           (1 0)
           (1 2))
         (all-cursors {:nodes {0 {},
                               1 {:child-ids [0 2]},
                               2 {}}
                       :root-id 1})))

  (is (= '([13]
           (13 5)
           (13 5 1)
           (13 5 1 0)
           (13 5 1 2)
           (13 5 6)
           (13 5 6 "C8422F10CCFE7877877470C9EB2C09ADA57B3C632C2DCECA61226C2245E96353")
           (13 5 6 "C91B1DD9307624570D8F2D395155C2158EE75AAC49FE3DA9000BB2BB157A04BA")
           (13 14)
           (13 14 9)
           (13 14 9 7)
           (13 14 9 8)
           (13 14 12)
           (13 14 12 10)
           (13 14 12 11)
           (13 14 12 15)
           (13 14 12 16))
         (all-cursors (-> (reduce add
                                  (create (full-after-maximum-number-of-values 3))
                                  (range 20))
                          (unload-node [13 5 6 3])
                          (unload-node [13 5 6 4]))))))

(defn set-node-content [btree parent-id storage-key node-content]
  (-> btree
      (assoc-in [:nodes (:next-node-id btree)]
                node-content)
      (replace-node-id parent-id
                       storage-key
                       (:next-node-id btree))
      (update :next-node-id
              inc)))

(declare unload-btree)

(deftest test-set-node-content
  (let [full? (full-after-maximum-number-of-values 3)
        btree (unload-btree (reduce add
                                    (create full?)
                                    (range 10)))]
    (is (= {8 {:child-ids
               [match/any-string
                match/any-string],
               :values #{3}}}
           (:nodes (set-node-content btree
                                     nil
                                     (:root-id btree)
                                     (get-node-content (:node-storage btree)
                                                       (:root-id btree))))))

    (is (= {8 {:child-ids
               [9 "E33374D7B0AEF7964CBA2A2A4B48BF1DFFB7B3F2F38782959C5D3C1D3EA8D444"],
               :values #{3}},
            9 "abc"}

           (:nodes (set-node-content {:nodes {8
                                              {:child-ids
                                               ["43B0199869DFD7D8B392D4F217CFB9E57D0CB52ABB4246EB60304E81AA7999B7"
                                                "E33374D7B0AEF7964CBA2A2A4B48BF1DFFB7B3F2F38782959C5D3C1D3EA8D444"],
                                               :values #{3}}},
                                      :next-node-id 9,
                                      :root-id 8,
                                      :full? full?
                                      :node-storage (:node-storage btree)}
                                     8
                                     "43B0199869DFD7D8B392D4F217CFB9E57D0CB52ABB4246EB60304E81AA7999B7"
                                     "abc"
                                     #_(get-node-content (:node-storage btree)
                                                         "43B0199869DFD7D8B392D4F217CFB9E57D0CB52ABB4246EB60304E81AA7999B7")))))))

(defn load-node [btree parent-id storage-key]
  (set-node-content btree
                    parent-id
                    storage-key
                    (get-node-content (:node-storage btree)
                                      storage-key)))

(defn load-node-to-atom [btree-atom parent-id storage-key]
  (swap! btree-atom
         load-node
         parent-id
         storage-key))


(defn load-root-if-needed [btree]
  (if (storage-key? (:root-id btree))
    (set-node-content btree
                      nil
                      (:root-id btree)
                      (get-node-content (:node-storage btree)
                                        (:root-id btree)))

    btree))

(defn split-root-if-needed [btree]
  (if ((:full? btree)
       (node btree
             (:root-id btree)))
    (split-root btree)
    btree))

(defn change-last [vector new-last]
  (conj (vec (drop-last vector))
        new-last))

(deftest test-change-last
  (is (= [1 2 4]
         (change-last [1 2 3] 4))))

(defn add [btree value]
  (loop [btree (-> btree
                   (load-root-if-needed)
                   (split-root-if-needed))

         cursor [(:root-id btree)]
         node-id (:root-id btree)]

    (let [the-node (node btree
                         node-id)]
      (if (leaf-node? the-node)
        (add-value-in-node btree
                           node-id
                           value)
        (if-let [the-child-id (child-id the-node
                                        value)]
          (if (storage-key? the-child-id)
            (recur (set-node-content btree
                                     (last cursor)
                                     the-child-id
                                     (get-node-content (:node-storage btree)
                                                       the-child-id))
                   (change-last cursor
                                (:next-node-id btree))

                   (:next-node-id btree))
            (let [btree (if ((:full? btree) (node btree
                                                  the-child-id))
                          (split-child btree
                                       node-id
                                       the-child-id)
                          btree)]
              (if-let [the-child-id (child-id (node btree
                                                    node-id)
                                              value)]
                (recur btree
                       (conj cursor
                             the-child-id)
                       the-child-id)
                btree)))
          btree)))))

(deftest test-add
  (let [full? (fn [node]
                (= 5 (count (:values node))))]

    (testing "root is full"
      (is (= {0 {:values #{1 2}},
              1 {:values #{3}, :child-ids [0 2]},
              2 {:values #{4 5 6}}}
             (:nodes (add {:nodes {0 {:values (create-sorted-set 1 2 3 4 5)}}
                           :next-node-id 1
                           :root-id 0
                           :full? full?}
                          6)))))

    (testing "no splits needed"
      (is (= {:nodes
              {0 {:values (create-sorted-set -1 1 2)},
               1 {:values (create-sorted-set 3), :child-ids [0 2]},
               2 {:values (create-sorted-set 4 5 6)}},
              :next-node-id 3,
              :root-id 1,
              :full? full?
              :next-usage-number 1
              :usages {0 0}}
             (add {:nodes
                   {0 {:values (create-sorted-set 1 2)},
                    1 {:values (create-sorted-set 3), :child-ids [0 2]},
                    2 {:values (create-sorted-set 4 5 6)}},
                   :next-node-id 3,
                   :root-id 1,
                   :full? full?}
                  -1))))

    (testing "leaf is full"
      (is (match/contains-map? {:nodes
                                {0 {:values #{-3 -2}},
                                 1 {:values #{-1 3}, :child-ids '(0 3 2)},
                                 2 {:values #{4 5 6}},
                                 3 {:values #{0 1 2}}},
                                :next-node-id 4,
                                :root-id 1,
                                :usages {3 4},
                                :next-usage-number 5}
                               (add {:nodes
                                     {0 {:values (create-sorted-set -3 -2 -1 0 1)},
                                      1 {:values (create-sorted-set 3), :child-ids [0 2]},
                                      2 {:values (create-sorted-set 4 5 6)}},
                                     :next-usage-number 3
                                     :next-node-id 3,
                                     :root-id 1,
                                     :full? full?}
                                    2))))


    (is (= {0 {:values #{0}},
            1 {:child-ids [0 2], :values #{1}},
            2 {:values #{2}},
            3 {:values #{4}},
            4 {:values #{6}},
            5 {:child-ids [1 6], :values #{3}},
            6 {:values #{5 7}, :child-ids [3 4 7]},
            7 {:values #{8 9}}}
           (:nodes (reduce add
                           (create (full-after-maximum-number-of-values 3))
                           (range 10)))))))


(defn load-node-2 [btree node-path]
  (update-in btree
             node-path
             merge
             (node-serialization/deserialize (storage/stream-from-storage! (:node-storage btree)
                                                                           (:storage-key (get-in btree
                                                                                                 node-path))))))

(deftest test-load-node-2

  (is (= {:root
          {:children
           [{:storage-key "A32F78C54A49C7CA308E10AB0D84B96D7A60E29F8084E1722953462C29257623",
             :values #{2}}
            {:values #{4}}],
           :values #{3}},
          :node-storage {"A32F78C54A49C7CA308E10AB0D84B96D7A60E29F8084E1722953462C29257623"
                         {:values #{2}}},
          :usages {[:root :children 1] 1
                   [:root] 2}}
         (extract-node-storage (load-node-2 (unload-node-2 (store-node {:root {:children [{:values (create-sorted-set 2)}
                                                                                          {:values (create-sorted-set 4)}]
                                                                               :values   (create-sorted-set 3)}
                                                                        :node-storage (hash-map-storage/create)
                                                                        :usages (priority-map/priority-map [:root :children 0] 0
                                                                                                           [:root :children 1] 1
                                                                                                           [:root] 2)}
                                                                       [:root :children 0])
                                                           [:root :children 0])
                                            [:root :children 0])))))



(defn load-root-if-needed-2 [btree]
  (if (loaded? (:root btree))
    btree
    (load-node-2 btree [:root])))

(defn split-root-if-needed-2 [btree]
  (if ((:full? btree) (:root btree))
    (split-root-2 btree)
    btree))

(defn add-2 [btree value]
  (loop [btree (-> btree
                   (load-root-if-needed-2)
                   (split-root-if-needed-2))
         path [:root]]

    (let [the-node (get-in btree
                           path)]
      (if (leaf-node-2? the-node)
        (add-value-in-node-2 btree
                             path
                             value)
        (if-let [the-child-index (child-index (:values the-node)
                                              value)]
          (let [child (get-in the-node [:children the-child-index])
                child-path (concat path [:children the-child-index])]
            (if (loaded? child)
              (let [btree (if ((:full? btree) child)
                            (split-child-2 btree child-path)
                            btree)]
                (if-let [the-child-index (child-index (:values (get-in btree path))
                                                      value)]
                  (recur btree
                         (concat path [:children the-child-index]))
                  btree))
              (recur (load-node-2 btree
                                  child-path)
                     child-path)))
          btree)))))

(defn- full-after-three [node]
  (= 3 (count (:values node))))

(deftest test-add-2
  (testing "no splits needed"
    (is (= {:root {:children [{:values #{1 2}}
                              {:values #{4}}]
                   :values #{3}},
            :full? full-after-three
            :usages {[:root :children 0] 0},
            :next-usage-number 1}
           (add-2 {:root {:children [{:values (create-sorted-set 2)}
                                     {:values (create-sorted-set 4)}]
                          :values (create-sorted-set 3)}
                   :full? full-after-three}
                  1))))

  (testing "leaf is full"
    (is (= {:root {:children [{:values #{1 2}}
                              {:values #{4}}
                              {:values #{6}}],
                   :values #{3 5}},
            :full? full-after-three
            :usages {[:root :children 1] 0
                     [:root :children 0] 1},
            :next-usage-number 2}
           (add-2 {:root {:children [{:values (create-sorted-set 2 3 4)}
                                     {:values (create-sorted-set 6)}]
                          :values (create-sorted-set 5)}
                   :full? full-after-three}
                  1))))

  (testing "root is full"
    (is (= {:root {:children [{:values #{1}}
                              {:values #{3 4}}]
                   :values #{2}},
            :full? full-after-three,
            :usages {[:root] 0, [:root :children 1] 2},
            :next-usage-number 3}
           (add-2 {:root {:values (create-sorted-set 1 2 3)}
                   :full? full-after-three}
                  4)))))

(defn cursor-and-btree-for-value-and-btree-atom [btree-atom value]
  (loop [btree @btree-atom
         cursor [(:root-id btree)]
         node-id (:root-id btree)]
    (if (storage-key? node-id)
      (do (load-node-to-atom btree-atom
                             (parent-id cursor)
                             node-id)
          (let [btree @btree-atom]
            (recur btree
                   [(:root-id btree)]
                   (:root-id btree))))
      (let [the-node (node btree
                           node-id)]
        (if (leaf-node? the-node)
          {:btree btree
           :cursor cursor}
          (if-let [the-child-id (child-id the-node
                                          value)]
            (recur btree
                   (conj cursor the-child-id)
                   the-child-id)
            {:btree btree
             :cursor cursor}))))))

(deftest test-cursor-and-btree-for-value
  (let [btree-atom (atom {:nodes
                          {0 {:values (create-sorted-set 1 2)},
                           1 {:values (create-sorted-set 3)
                              :child-ids [0 2]},
                           2 {:values (create-sorted-set 4 5 6)}}
                          :root-id 1
                          :node-storage (hash-map-storage/create)
                          :metadata-storage (hash-map-storage/create)
                          :usages (priority-map/priority-map)
                          :next-node-id 3})]
    (swap! btree-atom
           unload-btree)

    (is (match/contains-map? {:nodes {3 {:values #{3},
                                         :child-ids [4
                                                     match/any-string]},
                                      4 {:values #{1 2}}},
                              :root-id 3,
                              :usages {}
                              :next-node-id 5}
                             (dissoc (:btree (cursor-and-btree-for-value-and-btree-atom btree-atom
                                                                                        1))
                                     :node-storage)))

    (is (= [3 4]
           (:cursor (cursor-and-btree-for-value-and-btree-atom btree-atom
                                                               1))))

    (is (= [3]
           (:cursor (cursor-and-btree-for-value-and-btree-atom btree-atom
                                                               3))))))


(defn child-path [parent-path child-index]
  (vec (concat parent-path [:children child-index])))

(defn value-path
  "Retuns node path corresponding to the given value while loading all the nodes on the path.
  Both the path and loaded btree are returned."
  [btree value]
  (loop [btree btree
         path [:root]]

    (let [the-node (get-in btree path)]
      (if (loaded? the-node)
        (if (leaf-node-2? the-node)
          {:btree btree
           :path path}
          (if-let [the-child-index (child-index (:values the-node)
                                                value)]
            (recur btree
                   (child-path path the-child-index))
            {:btree btree
             :path path}))
        (recur (load-node-2 btree path)
               path)))))

(deftest test-value-path
  (testing "no need for loading"
    (is (= {:btree
            {:root {:children [{:values #{2}} {:values #{4}}], :values #{3}}},
            :path (:root :children 0)}
           (value-path {:root {:children [{:values (create-sorted-set 2)}
                                          {:values (create-sorted-set 4)}]
                               :values (create-sorted-set 3)}}
                       1))))

  (testing "the node is unloaded"
    (is (= {:btree
            {:root {:children [{:storage-key
                                "A32F78C54A49C7CA308E10AB0D84B96D7A60E29F8084E1722953462C29257623",
                                :values #{2}}
                               {:values #{4}}],
                    :values #{3}},
             :node-storage {"A32F78C54A49C7CA308E10AB0D84B96D7A60E29F8084E1722953462C29257623"
                            {:values #{2}}},
             :usages nil},
            :path (:root :children 0)}
           (-> (value-path (unload-node-2 {:root {:children [{:values (create-sorted-set 2)}
                                                             {:values (create-sorted-set 4)}]
                                                  :values (create-sorted-set 3)}
                                           :node-storage (hash-map-storage/create)}
                                          [:root :children 0])
                           1)
               (update :btree extract-node-storage))))))

(defn least-used-cursor [btree]
  (if (empty? (:nodes btree))
    nil
    (loop [node-id (:root-id btree)
           cursor [(:root-id btree)]]
      (let [the-node (node btree
                           node-id)]
        (if (leaf-node? the-node)
          cursor
          (if-let [least-used-child-id (first (sort-by (:usages btree)
                                                       (filter loaded-node-id?
                                                               (:child-ids the-node))))]
            (recur least-used-child-id
                   (conj cursor
                         least-used-child-id))
            cursor))))))

(deftest test-least-used-cursor
  (is (= [5 1 0]
         (least-used-cursor {:nodes
                             {0 {:values #{0}},
                              1 {:child-ids [0 2], :values #{1}},
                              2 {:values #{2}},
                              3 {:values #{4}},
                              4 {:values #{6}},
                              5 {:child-ids [1 6], :values #{3}},
                              6 {:values #{5 7}, :child-ids [3 4 7]},
                              7 {:values #{8 9}}},
                             :root-id 5,
                             :usages {0 2, 2 4, 3 6, 4 8, 7 9}})))

  (is (= [5 6]
         (least-used-cursor {:nodes
                             {5 {:child-ids ["abc" 6], :values #{3}},
                              6 {:values #{5 7}}}
                             :root-id 5,
                             :usages {6 1 5 2}})))

  (is (= [5]
         (least-used-cursor {:nodes
                             {5 {:child-ids ["abc" "def"], :values #{3}}}
                             :root-id 5,
                             :usages {5 2}})))

  (is (= nil
         (least-used-cursor {:nodes {}
                             :root-id "abc",
                             :usages {}})))

  (is (= [14 10 9]
         (least-used-cursor {:nodes
                             {7 {:values #{8 9}},
                              4 {:values #{6}},
                              15 {:values #{5 7}, :child-ids [12 13 16]},
                              13 {:values #{6}},
                              6 {:values #{5 7}, :child-ids [3 4 7]},
                              3 {:values #{4}},
                              12 {:values #{4}},
                              11 {:values #{2}},
                              9 {:values #{0}},
                              5 {:child-ids ["796AD3EEF52891C9000283476C05418E626F8DB78CB80177180D0A33831EFC8C" 6], :values #{3}},
                              14 {:child-ids [10 15], :values #{3}},
                              16 {:values #{8 9}},
                              10 {:child-ids [9 11], :values #{1}},
                              8 {:child-ids ["C657A0DE1F4454290D14CAD2FC6472174DB247A474DC4AC7ED71F53D0669CE04"
                                             "61C88379F997BA90CA05A124B47C52FB60649D0A281EC1892F2482D3BFFC4FFE"],
                                 :values #{1}}},
                             :root-id 14,
                             :usages {3 9, 5 12, 6 13, 4 14, 7 16, 9 19, 10 20, 11 23, 12 26, 14 29, 15 30, 13 31, 16 33},
                             :next-usage-number 34})))

  (is (= [14]
         (least-used-cursor {:nodes
                             {7 {:values #{9 8}},
                              4 {:values #{6}},
                              6 {:values #{7 5}, :child-ids [3 4 7]},
                              3 {:values #{4}},
                              5 {:child-ids ["796AD3EEF52891C9000283476C05418E626F8DB78CB80177180D0A33831EFC8C" 6], :values #{3}},
                              14 {:child-ids ["796AD3EEF52891C9000283476C05418E626F8DB78CB80177180D0A33831EFC8C" "132213E201F1B5592A2CE39DA6569D4D3E611127BDA7077A94178137CFD01E6D"], :values #{3}},
                              8 {:child-ids ["C657A0DE1F4454290D14CAD2FC6472174DB247A474DC4AC7ED71F53D0669CE04" "61C88379F997BA90CA05A124B47C52FB60649D0A281EC1892F2482D3BFFC4FFE"], :values #{1}}},
                             :next-node-id 17,
                             :root-id 14,
                             :usages {3 9, 5 12, 6 13, 4 14, 7 16, 14 29},
                             :next-usage-number 34}))))



(defn create-test-btree [node-size value-count]
  (reduce add
          (create (full-after-maximum-number-of-values node-size))
          (range value-count)))

(defn unload-least-used-node [btree]
  (if-let [least-used-path (ffirst (:usages btree))]
    (store-node btree
                least-used-path)
    btree))

(comment
  (ffirst (priority-map/priority-map :a 2 :b 1))
  ) ;; TODO: remove-me

(defn loaded-node-count-2 [btree]
  (count (:usages btree)))

(defn unload-excess-nodes [btree maximum-node-count]
  (loop [btree btree]
    (if (< maximum-node-count
           (loaded-node-count-2 btree))
      (recur (unload-least-used-node btree))
      btree)))

(defn unload-excess-nodes-2 [btree maximum-node-count]
  (loop [btree btree]
    (if (< maximum-node-count
           (loaded-node-count-2 btree))
      (recur (unload-least-used-node btree))
      btree)))


(comment
  (String. (byte-array [123, 58, 118, 97, 108, 117, 101, 115, 32, 35, 123, 49, 52, 125,
                        125, 10]))

  (IsFunction.)

  (schema/check {:a (schema/eq :b)}
                {:a :b}))


(deftest test-unload-excess-nodes
  (is (match/contains-map? {:nodes
                            {5 {:child-ids [match/any-string
                                            6],
                                :values #{3}},
                             6 {:values #{5 7},
                                :child-ids [match/any-string
                                            match/any-string
                                            7]},
                             7 {:values #{8 9}}},
                            :next-node-id 8,
                            :root-id 5,
                            :usages {5 12, 6 13, 7 16},
                            :next-usage-number 17}
                           (unload-excess-nodes (reduce add
                                                        (create (full-after-maximum-number-of-values 3))
                                                        (range 10))
                                                3)))

  (is (= 5
         (count (keys (:nodes (unload-excess-nodes (reduce add
                                                           (create (full-after-maximum-number-of-values 3))
                                                           (range 30))
                                                   5))))))
  (is (= 5
         (count (keys (:nodes (-> (create (full-after-maximum-number-of-values 3))
                                  (as-> btree
                                      (reduce add btree (range 10))
                                    (unload-excess-nodes btree 5)
                                    (reduce add btree (range 10))
                                    (unload-excess-nodes btree 5)))))))))

(defn btree-and-node-id-next-to-splitter [btree-atom splitter direction]
  (let [{:keys [cursor btree]} (cursor-and-btree-for-value-and-btree-atom btree-atom
                                                                          splitter)
        node-with-the-splitter (node btree
                                     (last cursor))
        node-id-next-to-splitter (get (:child-ids node-with-the-splitter)
                                      ((if (= :forwards direction)
                                         inc
                                         identity)
                                       (find-index (:values node-with-the-splitter)
                                                   splitter)))]
    {:node-id-next-to-splitter node-id-next-to-splitter
     :btree btree
     :cursor (conj cursor
                   node-id-next-to-splitter)}))

(defn sequence-after-splitter [btree-atom splitter direction]
  (let [{:keys [node-id-next-to-splitter btree cursor]} (btree-and-node-id-next-to-splitter btree-atom
                                                                                            splitter
                                                                                            direction)]

    (loop [btree btree
           cursor cursor
           node-id node-id-next-to-splitter]
      (if (storage-key? node-id)
        (do (load-node-to-atom btree-atom
                               (parent-id cursor)
                               node-id)
            (let [{:keys [node-id-next-to-splitter btree cursor]} (btree-and-node-id-next-to-splitter btree-atom
                                                                                                      splitter
                                                                                                      direction)]
              (recur btree
                     cursor
                     node-id-next-to-splitter)))
        (let [the-node (node btree
                             node-id)]

          (if (leaf-node? the-node)
            (combine-splitter-and-values (order-by-direction direction
                                                             (:values the-node))
                                         (splitter-next-to-cursor btree
                                                                  cursor
                                                                  direction)
                                         direction)
            (let [the-child-id ((if (= :forwards direction)
                                  first
                                  last)
                                (:child-ids the-node))]
              (recur btree
                     (conj cursor the-child-id)
                     the-child-id))))))))

(deftest test-sequence-after-splitter
  (let [btree-atom (atom {:nodes {0 {:values (create-sorted-set 0)}
                                  1 {:values (create-sorted-set 1)
                                     :child-ids [0 2]}
                                  2 {:values (create-sorted-set 2)}
                                  3 {:values (create-sorted-set 4)}
                                  4 {:values (create-sorted-set 6 7 8)}
                                  5 {:values (create-sorted-set 3)
                                     :child-ids [1 6]}
                                  6 {:values (create-sorted-set 5)
                                     :child-ids [3 4]}},
                          :next-node-id 7,
                          :root-id 5
                          :usages {}
                          :node-storage (hash-map-storage/create)
                          :metadata-storage (hash-map-storage/create)})]
    (swap! btree-atom
           unload-btree)

    (are [splitter sequence]
        (= sequence
           (sequence-after-splitter btree-atom
                                    splitter
                                    :forwards))

      3   [4 5]
      8   nil)

    (are [splitter sequence]
        (= sequence
           (sequence-after-splitter btree-atom
                                    splitter
                                    :backwards))

      3  [2 1]
      1  [0])))


(defn sequence-for-value [btree-atom value direction]
  (let [{:keys [cursor btree]} (cursor-and-btree-for-value-and-btree-atom btree-atom
                                                                          value)]
    (let [the-node (node btree
                         (last cursor))]
      (if (leaf-node? the-node)
        (combine-splitter-and-values (if (= :forwards direction)
                                       (subseq (:values the-node)
                                               >=
                                               value)
                                       (rsubseq (:values the-node)
                                                <=
                                                value))
                                     (splitter-next-to-cursor btree
                                                              cursor
                                                              direction)
                                     direction)
        [value]))))

(deftest test-sequence-for-value
  (let [btree-atom (atom {:nodes
                          {0 {:values (create-sorted-set 1 2)},
                           1 {:values (create-sorted-set 3)
                              :child-ids [0 2]},
                           2 {:values (create-sorted-set 4 5 6)}}
                          :root-id 1
                          :node-storage (hash-map-storage/create)
                          :metadata-storage (hash-map-storage/create)
                          :usages {}
                          :next-node-id 3})]
    (swap! btree-atom
           unload-btree)

    (are [value sequence]
        (= sequence
           (sequence-for-value btree-atom
                               value
                               :forwards))

      0   [1 2 3]
      1   [1 2 3]
      3   [3]
      -10 [1 2 3]
      5   [5 6]
      50  nil
      ::comparator/min [1 2 3])

    (are [value sequence]
        (= sequence
           (sequence-for-value btree-atom
                               value
                               :backwards))

      0   nil
      1   [1]
      3   [3]
      -10 nil
      5   [5 4 3]
      50  [6 5 4 3]
      ::comparator/min nil
      ::comparator/max [6 5 4 3])))

(defn- last-child? [parent child-index direction]
  (if (= direction :forwards)
    (= child-index
       (dec (count (:children parent))))
    (= child-index
       0)))

(defn following-value [sorted value direction]
  (first (if (= :forwards direction)
           (subseq sorted
                   >=
                   value)
           (rsubseq sorted
                    <=
                    value))))

(defn splitter-next-to-path [btree path value direction]
  (assert (direction? direction))

  (loop [path path]
    (if (= [:root] path)
      nil
      (let [parent (get-in btree (parent-path path))]
        (if (last-child? parent (last path) direction)
          (recur (parent-path path))
          (following-value (:values parent)
                           value
                           direction))))))

(defn sequence-after-value [btree value direction]
  (let [{:keys [path btree]} (value-path btree
                                         value)]
    (let [the-node (get-in btree path)]
      (if (leaf-node-2? the-node)
        (combine-splitter-and-values (if (= :forwards direction)
                                       (subseq (:values the-node)
                                               >=
                                               value)
                                       (rsubseq (:values the-node)
                                                <=
                                                value))
                                     (splitter-next-to-path btree
                                                            path
                                                            value
                                                            direction)
                                     direction)
        [value]))))

(deftest test-sequence-after-value
  (testing "leaf"
    (is (= '(2 3)
           (sequence-after-value {:root {:children [{:values (create-sorted-set 2)}
                                                    {:values (create-sorted-set 4)}]
                                         :values (create-sorted-set 3)}}
                                 1
                                 :forwards))))

  (testing "splitter"
    (is (= [3]
           (sequence-after-value {:root {:children [{:values (create-sorted-set 2)}
                                                    {:values (create-sorted-set 4)}]
                                         :values (create-sorted-set 3)}}
                                 3
                                 :forwards))))

  (testing "last child"
    (is (= [4 5]
           (sequence-after-value {:root {:children [{:values (create-sorted-set 2)}
                                                    {:values (create-sorted-set 4 5)}]
                                         :values (create-sorted-set 3)}}
                                 4
                                 :forwards)))))

(defn reduce-without-completion [reducing-function initial-value collection]
  (loop [reduced-value initial-value
         values collection]
    (if-let [value (first values)]
      (let [result (reducing-function reduced-value
                                      value)]
        (if (reduced? result)
          result
          (recur result
                 (rest values))))
      reduced-value)))


(defn minimum-greater-or-equal [sorted value]
  (first (subseq sorted
                 >=
                 value)))

(deftest test-minimum-greater-or-equal
  (is (= 3
         (minimum-greater-or-equal (sorted-set 1 3 5) 2)))

  (is (= 3
         (minimum-greater-or-equal (sorted-set 1 3 5) 3)))

  (is (= nil
         (minimum-greater-or-equal (sorted-set 1 3 5) 6)))

  (is (= ##Inf
         (minimum-greater-or-equal (sorted-set 1 3 5 ##Inf) 6)))

  (is (= [4 :b]
         (minimum-greater-or-equal (child-map 2 :a
                                              4 :b)
                                   4))))


(defn divider-for-value [node value]
  (first (minimum-greater-or-equal (:children node)
                                   value)))

(defn path-for-value [btree value]
  (loop [path [:root]]
    (let [node (get-in btree path)]
      (if (leaf-node-3? node)
        path
        (recur (concat path [:children (divider-for-value node value)]))))))

(deftest test-path-for-value
  (is (= '(:root :children :argumentica.comparator/max :children 4)
         (path-for-value {:root {:children (child-map 2 {:values (create-sorted-set 1 2)}
                                                      ::comparator/max {:children (child-map 4 {:values (create-sorted-set 3 4)}
                                                                                             ::comparator/max {:values (create-sorted-set 5 6)})})}}
                         4))))

(defn minimum-greater-than [sorted value]
  (first (subseq sorted
                 >
                 value)))

(deftest test-minimum-greater-than
  (is (= 3
         (minimum-greater-than (sorted-set 1 3 5) 1)))

  (is (= nil
         (minimum-greater-than (sorted-set 1 3 5) 5))))


(defn maximum-less-than [sorted value]
  (first (rsubseq sorted
                  <
                  value)))

(deftest test-maximum-less-than
  (is (= 3
         (maximum-less-than (sorted-set 1 3 5) 5)))

  (is (= nil
         (maximum-less-than (sorted-set 1 3 5) 1))))

(defn next-divider [node divider direction]
  (first (if (= :forwards direction)
           (minimum-greater-than (:children node)
                                 divider)
           (maximum-less-than (:children node)
                              divider))))

(deftest test-next-divider
  (is (= :argumentica.comparator/max
         (next-divider {:children (child-map 2 {}
                                             ::comparator/max {})}
                       2
                       :forwards)))

  (is (= 2
         (next-divider {:children (child-map 2 {}
                                             ::comparator/max {})}
                       ::comparator/max
                       :backwards)))

  (is (= nil
         (next-divider {:children (child-map 2 {}
                                             ::comparator/max {})}
                       2
                       :backwards))))

(defn first-leaf-path [btree path direction]
  (loop [path path]
    (let [node (get-in btree path)]
      (if (leaf-node-3? node)
        path
        (recur (concat path [:children (first (if (= :forwards direction)
                                                (first (:children node))
                                                (last (:children node))))]))))))

(deftest test-first-leaf-path
  (is (= [:root :children 2]
         (first-leaf-path {:root {:children (child-map 2 {}
                                                       ::comparator/max {:children (child-map 4 {}
                                                                                              ::comparator/max {})})}}
                          [:root]
                          :forwards)))

  (is (= [:root :children :argumentica.comparator/max :children 4]
         (first-leaf-path {:root {:children (child-map 2 {}
                                                       ::comparator/max {:children (child-map 4 {}
                                                                                              ::comparator/max {})})}}
                          [:root :children ::comparator/max]
                          :forwards)))

  (is (= '(:root :children :argumentica.comparator/max :children :argumentica.comparator/max)
         (first-leaf-path {:root {:children (child-map 2 {}
                                                       ::comparator/max {:children (child-map 4 {}
                                                                                              ::comparator/max {})})}}
                          [:root]
                          :backwards))))

(defn next-child-path [btree path direction]
  (loop [path path]
    (if (= [:root] path)
      nil

      (let [the-parent-path (parent-path path)
            parent (get-in btree the-parent-path)]
        (if-let [the-next-divider (next-divider parent
                                                (last path)
                                                direction)]
          (first-leaf-path btree
                           (concat the-parent-path
                                   [:children the-next-divider])
                           direction)
          (recur the-parent-path))))))

(deftest test-next-child-path
  (is (= '(:root :children :argumentica.comparator/max :children 4)
         (next-child-path {:root {:children (child-map 2 {}
                                                       ::comparator/max {:children (child-map 4 {}
                                                                                              ::comparator/max {})})}}
                          [:root :children 2]
                          :forwards)))

  (is (= '(:root :children ::comparator/max :children ::comparator/max)
         (next-child-path {:root {:children (child-map 2 {}
                                                       ::comparator/max {:children (child-map 4 {}
                                                                                              ::comparator/max {})})}}
                          [:root :children ::comparator/max :children 4]
                          :forwards)))

  (is (= nil
         (next-child-path {:root {:children (child-map 2 {}
                                                       ::comparator/max {:children (child-map 4 {}
                                                                                              ::comparator/max {})})}}
                          [:root :children ::comparator/max :children ::comparator/max]
                          :forwards))))

(defn subsequence [sorted starting-value direction]
  (if (= :forwards direction)
    (subseq sorted
            >=
            starting-value)
    (rsubseq sorted
             <=
             starting-value)))

;; TODO: load nodes on the way
(defn reduce-btree [reducing-function initial-reduced-value btree starting-value direction]
  (loop [reduced-value initial-reduced-value
         btree btree
         path (path-for-value btree starting-value)]

    (if path
      (let [node (get-in btree path)]
        (let [reduced-value (reduce-without-completion reducing-function
                                                       reduced-value
                                                       (subsequence (:values node)
                                                                    starting-value
                                                                    direction))]
          (if (reduced? reduced-value)
            (reducing-function @reduced-value)
            (recur reduced-value
                   btree
                   (next-child-path btree path direction)))))
      (reducing-function reduced-value))))

(deftest test-reduce-btree
  (is (= [1 2 3 4 5 6]
         (reduce-btree conj
                       []
                       {:root {:children (child-map 2 {:values (create-sorted-set 1 2)}
                                                    ::comparator/max {:children (child-map 4 {:values (create-sorted-set 3 4)}
                                                                                           ::comparator/max {:values (create-sorted-set 5 6)})})}}
                       1
                       :forwards)))

  (is (= [2 3 4]
         (reduce-btree ((comp (take 3)
                              (map inc)) conj)
                       []
                       {:root {:children (child-map 2 {:values (create-sorted-set 1 2)}
                                                    ::comparator/max {:children (child-map 4 {:values (create-sorted-set 3 4)}
                                                                                           ::comparator/max {:values (create-sorted-set 5 6)})})}}
                       1
                       :forwards)))

  (is (= [4 3 2 1]
         (reduce-btree conj
                       []
                       {:root {:children (child-map 2 {:values (create-sorted-set 1 2)}
                                                    ::comparator/max {:children (child-map 4 {:values (create-sorted-set 3 4)}
                                                                                           ::comparator/max {:values (create-sorted-set 5 6)})})}}
                       4
                       :backwards))))

(defn lazy-value-sequence [btree-atom sequence direction]
  (if-let [sequence (if (first (rest sequence))
                      sequence
                      (if-let [value (first sequence)]
                        (cons value
                              (sequence-after-splitter btree-atom
                                                       (first sequence)
                                                       direction))
                        nil))]
    (lazy-seq (cons (first sequence)
                    (lazy-value-sequence btree-atom
                                         (rest sequence)
                                         direction)))
    nil))

(defn inclusive-subsequence [btree-atom value]
  (lazy-value-sequence btree-atom
                       (sequence-for-value btree-atom
                                           value
                                           :forwards)
                       :forwards))

(deftest test-inclusive-subsequence
  (let [btree-atom (atom {:nodes
                          {0 {:values (create-sorted-set 1 2)},
                           1 {:values (create-sorted-set 3)
                              :child-ids [0 2]},
                           2 {:values (create-sorted-set 4 5 6)}}
                          :root-id 1
                          :next-node-id 3
                          :usages {}
                          :node-storage (hash-map-storage/create)
                          :metadata-storage (hash-map-storage/create)})]

    (swap! btree-atom
           unload-btree)

    (is (= 3
           (count (storage/storage-keys! (:metadata-storage @btree-atom)))))

    (is (= [1 2 3 4 5 6]
           (inclusive-subsequence btree-atom
                                  0)))


    (is (= [2 3 4 5 6]
           (inclusive-subsequence btree-atom
                                  2)))

    (is (= [3 4 5 6]
           (inclusive-subsequence btree-atom
                                  3)))

    (is (= [4 5 6]
           (inclusive-subsequence btree-atom
                                  4)))

    (is (= nil
           (inclusive-subsequence btree-atom
                                  7)))

    (let [values (range 200)]
      (is (= values
             (inclusive-subsequence (atom (reduce add
                                                  (create (full-after-maximum-number-of-values 3))
                                                  values))
                                    (first values)))))))


(defn inclusive-reverse-subsequence [btree-atom value]
  (lazy-value-sequence btree-atom
                       (sequence-for-value btree-atom
                                           value
                                           :backwards)
                       :backwards))


(deftest test-inclusive-reverse-subsequence
  (let [btree-atom (atom {:nodes
                          {0 {:values (create-sorted-set 1 2)},
                           1 {:values (create-sorted-set 3)
                              :child-ids [0 2]},
                           2 {:values (create-sorted-set 4 5 6)}}
                          :root-id 1
                          :next-node-id 3
                          :usages {}
                          :node-storage (hash-map-storage/create)
                          :metadata-storage (hash-map-storage/create)})]

    (swap! btree-atom
           unload-btree)

    (is (= 3
           (count (storage/storage-keys! (:metadata-storage @btree-atom)))))

    (is (= nil
           (inclusive-reverse-subsequence btree-atom
                                          0)))


    (is (= [2 1]
           (inclusive-reverse-subsequence btree-atom
                                          2)))

    (is (= [3 2 1]
           (inclusive-reverse-subsequence btree-atom
                                          3)))

    (is (= [4 3 2 1]
           (inclusive-reverse-subsequence btree-atom
                                          4)))

    (is (= [6 5 4 3 2 1]
           (inclusive-reverse-subsequence btree-atom
                                          7)))

    (let [values (range 200)]
      (is (= (reverse values)
             (inclusive-reverse-subsequence (atom (reduce add
                                                          (create (full-after-maximum-number-of-values 3))
                                                          values))
                                            (last values)))))))


(defn next-sequence [sequence btree-atom direction]
  (if (first (rest sequence))
    sequence
    (if-let [value (first sequence)]
      (cons value
            (sequence-after-splitter btree-atom
                                     value
                                     direction))
      nil)))

(util/defno transduce-btree [btree-atom starting-value options :- transducible-collection/transduce-options]
  (let [options (merge transducible-collection/default-transduce-options
                       options)
        reducing-function ((:transducer options) (:reducer options))]
    (loop [reduced-value (if (contains? options :initial-value)
                           (:initial-value options)
                           ((:reducer options)))
           sequence (sequence-for-value btree-atom
                                        starting-value
                                        (:direction options))]
      (loop [reduced-value reduced-value
             sequence sequence]
        (if-let [value-from-sequence (first sequence)]
          (let [result (reducing-function reduced-value
                                          value-from-sequence)]
            (if (reduced? result)
              (reducing-function @result)
              (recur result
                     (rest (next-sequence sequence
                                          btree-atom
                                          (:direction options))))))
          (reducing-function reduced-value))))))

(deftest test-transduce-btree
  (is (= [30 31 32 33 34 35 36 37 38 39]
         (transduce-btree (atom (create-test-btree 3 100))
                          30
                          {:transducer (take 10)
                           :reducer conj})))

  (is (= [30 29 28 27 26 25 24 23 22 21]
         (transduce-btree (atom (create-test-btree 3 100))
                          30
                          {:transducer (take 10)
                           :reducer conj
                           :direction :backwards})))

  (is (= [0 1]
         (transduce-btree (atom (reduce add
                                        (create  (full-after-maximum-number-of-values 3))
                                        [0 -1 1 0]))
                          0
                          {:reducer conj}))))

(defn iterator-atom [btree-atom starting-value direction]
  (atom {:sequence (sequence-for-value btree-atom
                                       starting-value
                                       direction)
         :btree-atom btree-atom
         :direction direction}))

(defn seq-first [iterator-atom]
  (first (:sequence @iterator-atom)))

(defn seq-next [iterator-atom]
  (swap! iterator-atom
         (fn [iterator]
           (assoc iterator
                  :sequence
                  (rest (next-sequence (:sequence iterator)
                                       (:btree-atom iterator)
                                       (:direction iterator))))))
  iterator-atom)

(defn next [iterator-atom]
  (let [iterator @iterator-atom]
    (when-let [value (first (:sequence iterator))]
      (swap! iterator-atom
             assoc
             :sequence
             (rest (next-sequence (:sequence iterator)
                                  (:btree-atom iterator)
                                  (:direction iterator))))
      value)))

(defn has-next? [iterator-atom]
  (some? (first (:sequence @iterator-atom))))

(defn seq-proxy [iterator-atom]
  (proxy [clojure.lang.ASeq] []
    (first []
      (seq-first iterator-atom))
    (next []
      (seq-next iterator-atom)
      (if (has-next? iterator-atom)
        this
        nil))))

(defrecord BtreeIterator [iterator-atom]
  java.util.Iterator
  (next [this]
    (next iterator-atom))
  (hasNext [this]
    (has-next? iterator-atom)))


(defn btree-seq [btree-atom starting-value direction]
  (seq-proxy (iterator-atom btree-atom
                            starting-value
                            direction)))

(defn iterator [btree-atom starting-value direction]
  (->BtreeIterator (iterator-atom btree-atom
                                  starting-value
                                  direction)))

(deftest test-iterator
  (is (= [3 4 5 6 7 8 9]
         (reduce conj []
                 (iterator-seq (iterator (atom (reduce add
                                                       (create (full-after-maximum-number-of-values 3))
                                                       (range 10)))
                                         3
                                         :forwards)))))
  )


(deftest test-btree
  (repeatedly 100
              (let [maximum 1000
                    values (take 200
                                 (repeatedly (fn [] (rand-int maximum))))
                    first-value (rand-int maximum)
                    btree-atom (atom (reduce add
                                             (create (full-after-maximum-number-of-values 3))
                                             values))
                    forward-subsequence (subseq (apply create-sorted-set
                                                       values)
                                                >=
                                                first-value)
                    backward-subsequence (rsubseq (apply create-sorted-set
                                                         values)
                                                  <=
                                                  first-value)]
                (is (= forward-subsequence
                       (inclusive-subsequence btree-atom
                                              first-value)))

                (is (= forward-subsequence
                       (transduce-btree btree-atom
                                        first-value
                                        {:reducer conj})))

                (is (= backward-subsequence
                       (inclusive-reverse-subsequence btree-atom
                                                      first-value)))

                (is (= backward-subsequence
                       (transduce-btree btree-atom
                                        first-value
                                        {:reducer conj
                                         :direction :backwards})))

                (is (= (reverse (sort (distinct values)))
                       (transduce-btree btree-atom
                                        (apply max values)
                                        {:reducer conj
                                         :direction :backwards})))))

  (let [values (range 10)]
    (is (= values
           (transduce-btree (atom (reduce add
                                          (create (full-after-maximum-number-of-values 3))
                                          values))
                            (first values)
                            {:reducer conj})))))

#_(defn write-metadata [btree]
    (put-to-storage (:node-storage btree)
                    "metadata.edn"
                    (.getBytes (pr-str (select-keys btree
                                                    [:root-id
                                                     :stored-node-metadata]))
                               "UTF-8")))

#_(defn load-from-metadata [full? storage]
    (let [metadata (binding [*read-eval* false]
                     (read-string (String. (get-from-storage storage
                                                             "metadata.edn")
                                           "UTF-8")))]
      (assoc (create full?
                     storage
                     (:root-id metadata))
             :stored-node-metadata (:stored-node-metadata metadata))))

(defn unload-btree [btree]
  (unload-excess-nodes btree 0))

(deftest test-unload-btree
  (testing "unload btree when some nodes are unloaded in the middle"
    (is (= '(0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19)
           (inclusive-subsequence (atom (-> (reduce add
                                                    (create (full-after-maximum-number-of-values 3))
                                                    (range 20))
                                            (unload-node [13 5 6 3])
                                            (unload-node [13 5 6 4])
                                            (unload-btree)))
                                  0))))

  (testing "unload btree when first nodes are unloaded"
    (is (= '(0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19)
           (inclusive-subsequence (atom (-> (reduce add
                                                    (create (full-after-maximum-number-of-values 3))
                                                    (range 20))

                                            (unload-node [13 5 1 0])
                                            (unload-node [13 5 1 2])
                                            (unload-btree)))
                                  0)))))

(defn remove-old-roots [btree]
  (storage/put-edn-to-storage! (:metadata-storage btree)
                               "roots"
                               [(get-latest-root btree)])
  btree)

(defn add-stored-root [btree metadata]
  (let [new-root {:storage-key (:root-id btree)
                  :stored-time (System/nanoTime)
                  :metadata metadata}]
    (storage/put-edn-to-storage! (:metadata-storage btree)
                                 "roots"
                                 (conj (roots btree)
                                       new-root))
    (assoc btree
           :latest-root
           new-root)))

(defn store-root [btree metadata]
  #_ (prn (all-cursors btree)
          (storage/storage-keys! (:node-storage (-> (reduce store-node-by-cursor
                                                            btree
                                                            (all-cursors btree))
                                                    (add-stored-root metadata))))) ;; TODO: remove-me

  (-> (reduce store-node-by-cursor
              btree
              (all-cursors btree))
      (add-stored-root metadata)))


(deftest test-store-root
  (let [original-btree {:nodes {1 {:values #{3},
                                   :child-ids [2]},
                                2 {:values #{4 5 6}}}
                        :root-id 1
                        :node-storage (hash-map-storage/create)
                        :metadata-storage (hash-map-storage/create)}
        resulting-btree (store-root original-btree
                                    {:this-is-metadata :data})]
    (is (= (:nodes original-btree)
           (:nodes resulting-btree)))
    (storage-contents (:metadata-storage resulting-btree))
    ;; (storage/storage-keys! (:node-storage resulting-btree))
    ;; (storage/get-edn-from-storage! (:metadata-storage resulting-btree))
    #_(is (= {:values #{4 5 6}}
             (get-node-content (:node-storage resulting-btree)
                               (first (storage/storage-keys! (:node-storage resulting-btree))))))

    #_(storage/storage-keys! (:metadata-storage resulting-btree))
    #_(is (= {:values #{4 5 6}}
             (get-node-content (:node-storage resulting-btree)
                               (first (storage/storage-keys! (:metadata-storage resulting-btree))))))))

(defn load-first-cursor [btree-atom]
  (loop [cursor (first-leaf-cursor @btree-atom)]
    (if (not (loaded-node-id? (last cursor)))
      (recur (first-leaf-cursor (load-node-to-atom btree-atom
                                                   (parent-id cursor)
                                                   (last cursor)))))))

(defn storage-keys-from-stored-nodes [storage root-key]
  (tree-seq (fn [storage-key]
              (not (leaf-node? (get-node-content storage
                                                 storage-key))))
            (fn [storage-key]
              (:child-ids (get-node-content storage
                                            storage-key)))
            root-key))


(defn storage-keys-from-metadata [metadata-storage root-key]
  (tree-seq (fn [storage-key]
              (:child-ids (storage/get-edn-from-storage! metadata-storage
                                                         storage-key)))
            (fn [storage-key]
              (:child-ids (storage/get-edn-from-storage! metadata-storage
                                                         storage-key)))
            root-key))

(deftest test-storage-keys-from-metadata-and-stored-nodes
  (let [btree (unload-btree (reduce add
                                    (create-from-options :full? (full-after-maximum-number-of-values 3))
                                    (range 20)))
        the-storage-keys-from-metadata (storage-keys-from-metadata (:metadata-storage btree)
                                                                   (:root-id btree))]
    (is (= 17 (count the-storage-keys-from-metadata)))
    (is (every? string? the-storage-keys-from-metadata))
    (is (= the-storage-keys-from-metadata
           (storage-keys-from-stored-nodes (:node-storage btree)
                                           (:root-id btree))))))

(defn get-metadata [btree key]
  (storage/get-edn-from-storage! (:metadata-storage btree)
                                 key))

(defn used-storage-keys [btree]
  (reduce (fn [keys storage-key]
            (apply conj keys
                   (storage-keys-from-metadata (:metadata-storage btree)
                                               (:storage-key storage-key))))
          #{}
          (get-metadata btree
                        "roots")))

(defn unused-storage-keys [btree]
  (remove (used-storage-keys btree)
          (storage/storage-keys! (:node-storage btree))))

(defn stored-node-sizes [btree]
  (map (fn [storage-key]
         (:storage-byte-count (get-metadata btree
                                            storage-key)))
       (used-storage-keys btree)))

(deftest test-stored-node-sizes
  (is (= '(21 21 21 165 165 21 302 21 165 23 21 165 21 165 21 165 20)
         (stored-node-sizes (store-root (reduce add
                                                (create-from-options :full? (full-after-maximum-number-of-values 3))
                                                (range 20))
                                        {})))))

(defn total-storage-size [btree]
  (reduce + (stored-node-sizes btree)))

(defn collect-storage-garbage [btree]
  (let [unused-storage-keys (unused-storage-keys btree)
        remove-from-storage (fn [storage]
                              (reduce storage/remove-from-storage!
                                      storage
                                      unused-storage-keys))]
    (-> btree
        (update :node-storage remove-from-storage)
        (update :metadata-storage remove-from-storage))))


(deftest test-garbage-collection
  #_(unused-storage-keys (reduce (fn [btree numbers]
                                   (unload-excess-nodes (reduce add
                                                                btree
                                                                numbers)
                                                        5))
                                 (create (full-after-maximum-number-of-values 3))
                                 (partition 40 (range 40))))

  #_(let [btree-atom (atom )]

      (doseq [numbers ]
        (doseq [number numbers]
          (add-to-atom btree-atom number))
        (swap! btree-atom unload-excess-nodes 5))

      (unused-storage-keys @btree-atom)
      (swap! btree-atom unload-excess-nodes 5)
      @btree-atom

      )

  #_(add (unload-btree (add (create (full-after-maximum-number-of-values 3))
                            1))
         2)

  )


#_(defn start []
    (let [{:keys [btree storage]} (unload-btree (reduce add
                                                        (create (full-after-maximum-number-of-values 3))
                                                        (range 30))
                                                {}
                                                assoc)
          root-node-key (:storage-key (node btree
                                            (:root-id btree)))]
      #_(load-btree storage
                    root-node-key)
      btree
      #_(String. (get storage
                      (:storage-key (node btree
                                          (:root-id btree))))
                 "UTF-8")))
