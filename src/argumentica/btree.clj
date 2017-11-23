(ns argumentica.btree
  (:require [flow-gl.tools.trace :as trace]
            [flow-gl.debug :as debug]
            (argumentica [match :as match]
                         [cryptography :as cryptography]
                         [encode :as encode]
                         [graph :as graph])
            [clojure.java.io :as io]
            [clojure.data.priority-map :as priority-map]
            [schema.core :as schema])
  (:use [clojure.test])
  (:import [java.io DataInputStream DataOutputStream]
           
           [java.nio.file.attribute FileAttribute]))

(defn create-node []
  {:values (sorted-set)})

(defn full-after-maximum-number-of-values [maximum]
  (fn [node]
    (= maximum
       (count (:values node)))))

(defn create
  ([]
   (create (full-after-maximum-number-of-values 100)
           {}))

  ([full?]
   (create full?
           {}))
  
  ([full? storage]
   {:nodes {0 (create-node)}
    :next-node-id 1
    :loaded-nodes 1
    :root-id 0
    :full? full?
    :storage storage
    :usages (priority-map/priority-map)})

  ([full? storage root-storage-key]
   {:nodes {}
    :next-node-id 0
    :loaded-nodes 0
    :root-id root-storage-key
    :full? full?
    :storage storage
    :usages (priority-map/priority-map)}))

(defmulti get-from-storage
  (fn [storage key]
    (type storage)))

(defmethod get-from-storage
  clojure.lang.PersistentHashMap
  [storage key]
  (get storage
       key))

(defmethod get-from-storage
  clojure.lang.PersistentArrayMap
  [storage key]
  (get storage
       key))

(defmulti put-to-storage
  (fn [storage key value]
    (type storage)))

(defmethod put-to-storage
  clojure.lang.PersistentHashMap
  [storage key value]
  (assoc storage
         key
         value))

(defmethod put-to-storage
  clojure.lang.PersistentArrayMap
  [storage key value]
  (assoc storage
         key
         value))




(defn loaded? [node-id]
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

(defn split-child [btree parent-id old-child-id]
  (let [{:keys [lesser-values greater-values median-value]} (split-sorted-set (get-in btree [:nodes old-child-id :values]))
        new-child-id (:next-node-id btree)]
    (-> btree
        (update :next-node-id inc)
        (update :loaded-nodes inc)
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
          :loaded-nodes 4
          :next-node-id 4,
          :root-id 1}
         (split-child {:nodes {0 {:values (sorted-set 1 2 3)},
                               1 {:values (sorted-set 3), :child-ids [0 2]},
                               2 {:values (sorted-set 4 5)}},
                       :loaded-nodes 3
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
          :loaded-nodes 10
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
                       :loaded-nodes 9
                       :root-id 5}
                      5
                      6))))

(defn split-root [btree]
  (let [new-root-id (:next-node-id btree)]
    (-> btree
        (update :next-node-id inc)
        (assoc :root-id new-root-id)
        (assoc-in [:nodes new-root-id] {:child-ids [(:root-id btree)]
                                        :values (sorted-set)})
        (update :loaded-nodes inc)
        (split-child new-root-id (:root-id btree)))))

(deftest test-split-root
  (is (= {:nodes
          {0 {:values #{1 2}},
           1 {:values #{3}, :child-ids [0 2]},
           2 {:values #{4 5}}},
          :next-node-id 3,
          :loaded-nodes 3
          :root-id 1}
         (split-root {:nodes {0 {:values (sorted-set 1 2 3 4 5)}}
                      :loaded-nodes 1
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
          :loaded-nodes 7
          :next-node-id 7,
          :root-id 5}
         (split-root {:nodes {0 {:values (sorted-set 0)}
                              1 {:values (sorted-set 1 3 5), :child-ids [0 2 3 4]}
                              2 {:values (sorted-set 2)}
                              3 {:values (sorted-set 4)}
                              4 {:values (sorted-set 6 7)}},
                      :loaded-nodes 5
                      :next-node-id 5,
                      :root-id 1}))))



(defn child-index [splitter-values value]
  (loop [splitter-values splitter-values
         child-index 0]
    (if-let [splitter-value (first splitter-values)]
      (if (= splitter-value
             value)
        nil
        (if (neg? (compare value
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

(defn node [btree node-id]
  (get-in btree [:nodes node-id]))

(defn record-usage [btree cursor]
  (-> btree 
      (update :usages
              assoc cursor (or (:next-usage-number btree)
                               0))
      (update :next-usage-number
              (fnil inc 0))))

(defn add [btree value]
  (loop [btree (if ((:full? btree) (node btree
                                         (:root-id btree)))
                 (split-root btree)
                 btree)
         node-id (:root-id btree)
         cursor [node-id]]
    (let [the-node (node btree
                         node-id)]
      (if (leaf-node? the-node)
        (-> btree
            (update-in [:nodes node-id :values]
                       conj value)
            (record-usage cursor))
        (if-let [the-child-id (child-id the-node
                                        value)]
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
                     the-child-id
                     (conj cursor
                           the-child-id))
              btree))
          btree)))))

(deftest test-add
  (let [full? (fn [node]
                (= 5 (count (:values node))))]

    (testing "root is full"
      (is (= {0 {:values #{1 2}},
              1 {:values #{3}, :child-ids [0 2]},
              2 {:values #{4 5 6}}}
             (:nodes (add {:nodes {0 {:values (sorted-set 1 2 3 4 5)}}
                           :loaded-nodes 1
                           :next-node-id 1
                           :root-id 0
                           :full? full?}
                          6)))))

    (testing "no splits needed"
      (is (= {:nodes
              {0 {:values (sorted-set -1 1 2)},
               1 {:values (sorted-set 3), :child-ids [0 2]},
               2 {:values (sorted-set 4 5 6)}},
              :next-node-id 3,
              :root-id 1,
              :full? full?
              :next-usage-number 1
              :usages {[1 0] 0}}
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
              :loaded-nodes 5
              :root-id 1,
              :full? full?
              :usages {[1 3] 0},
              :next-usage-number 1}
             (add {:nodes
                   {0 {:values (sorted-set -3 -2 -1 0 1)},
                    1 {:values (sorted-set 3), :child-ids [0 2]},
                    2 {:values (sorted-set 4 5 6)}},
                   :next-node-id 3,
                   :loaded-nodes 4
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





(defn parent-id [cursor]
  (last (drop-last cursor)))

(defn next-cursor [btree cursor]
  (loop [cursor cursor]
    (if-let [parent (node btree
                          (parent-id cursor))]
      (if-let [next-node-id-downwards (get (:child-ids parent)
                                           (inc (find-index (:child-ids parent)
                                                            (last cursor))))]
        (loop [cursor (conj (vec (drop-last cursor))
                            next-node-id-downwards)]
          (let [next-node-downwards (node btree
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

(defn splitter-after-cursor [btree cursor]
  (loop [cursor cursor]
    (if-let [parent (node btree
                          (last (drop-last cursor)))]
      (if (empty? (children-after parent
                                  (last cursor)))
        (recur (drop-last cursor))
        (splitter-after-child parent
                              (last cursor)))
      nil)))


(deftest test-splitter-after-cursor
  (let [btree {:nodes {0 {:values (sorted-set 0)}
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
           (splitter-after-cursor btree
                                  [5 1 2])))

    (is (= nil
           (splitter-after-cursor btree
                                  [5 6 4])))

    (is (= 5
           (splitter-after-cursor btree
                                  [5 6 3]))))


  (let [btree {:nodes
               {0 {:values (sorted-set 1 2)},
                1 {:values (sorted-set 3)
                   :child-ids [0 2]},
                2 {:values (sorted-set 4 5 6)}}
               :root-id 1}]

    (is (= 3
           (splitter-after-cursor btree
                                  [1 0])))

    (is (= nil
           (splitter-after-cursor btree
                                  [1 2])))))

(defn append-if-not-null [collection value]
  (if value
    (concat collection
            [value])
    collection))

(defn sequence-for-cursor [btree cursor]
  (append-if-not-null (seq (:values (node btree
                                          (last cursor))))
                      (splitter-after-cursor btree
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

(defn first-cursor [btree]
  (loop [cursor [(:root-id btree)]
         node-id (:root-id btree)]
    (let [the-node (node btree
                         node-id)]
      (if (leaf-node? the-node)
        cursor
        (let [first-child-id (first (:child-ids the-node))]
          (recur (conj cursor
                       first-child-id)
                 first-child-id))))))

(deftest test-first-cursor
  (is (= [1 0]
         (first-cursor {:nodes {0 {:values (sorted-set 1 2)},
                                1 {:values (sorted-set 3), :child-ids [0 2]},
                                2 {:values (sorted-set 4 5 6)}}
                        :root-id 1}))))


(defn replace-node-id [btree parent-id old-child-id new-child-id]
  (if parent-id
    (update-in btree
               [:nodes parent-id :child-ids]
               (partial replace
                        {old-child-id new-child-id}))
    (assoc btree :root-id new-child-id)))

(defn node-to-bytes [node]
  (.getBytes (prn-str node)
             "UTF-8"))

(defn bytes-to-node [bytes]
  (-> (binding [*read-eval* false]
        (read-string (String. bytes
                              "UTF-8")))
      (update :values
              (fn [values]
                (into (sorted-set)
                      values)))))

(defn storage-key [bytes]
  (encode/base-16-encode (cryptography/sha-256 bytes)))

(deftest test-node-serialization
  (let [node (create-node)]
    (is (= node
           (bytes-to-node (node-to-bytes node))))))

(defn cursors [cursor btree]
  (if-let [the-next-cursor (next-cursor btree
                                        cursor)]
    (lazy-seq (cons cursor
                    (cursors (next-cursor btree
                                          cursor)
                             btree)))
    [cursor]))

(deftest test-cursors
  (is (= '([1 0]
           [1 2])
         (let [btree {:nodes {0 {:values (sorted-set 1 2)},
                              1 {:values (sorted-set 3), :child-ids [0 2]},
                              2 {:values (sorted-set 4 5 6)}}
                      :root-id 1}]
           (cursors (first-cursor btree)
                    btree)))))

(defn unload-cursor [btree cursor]
  (loop [cursor cursor
         btree btree]
    (let [node-id-to-be-unloded (last cursor)
          parent-id (last (drop-last cursor))
          bytes (node-to-bytes (node btree
                                     node-id-to-be-unloded))
          the-storage-key (storage-key bytes)
          btree (-> btree
                    (replace-node-id parent-id
                                     node-id-to-be-unloded
                                     the-storage-key)
                    (update :nodes dissoc node-id-to-be-unloded)
                    (update :loaded-nodes dec)
                    (update :usages dissoc cursor)
                    (update :storage
                            put-to-storage
                            the-storage-key
                            bytes))]
      (if (and parent-id
               (every? (complement loaded?)
                       (:child-ids (node btree
                                         parent-id))))
        (recur (drop-last cursor)
               btree)
        btree))))

(deftest test-unload-cursor
  (is (= {1 {:values #{3},
             :child-ids ["B58E78A458A49C835829351A3853B584CA01124A1A96EB782BA23513124F01A7"
                         2]},
          2 {:values #{4 5 6}}}
         (:nodes (unload-cursor {:nodes {0 {:values (sorted-set 1 2)},
                                         1 {:values (sorted-set 3), :child-ids [0 2]},
                                         2 {:values (sorted-set 4 5 6)}}
                                 :loaded-nodes 3
                                 :root-id 1
                                 :storage {}}
                                [1 0]))))

  (is (= {:nodes {},
          :root-id "7761BC24541DD827301A1AFE64FB273ED86C1B7F14A474192575A4A3428C0732"
          :loaded-nodes 0}
         (-> (unload-cursor {:nodes
                             {1 {:values #{3},
                                 :child-ids ["B58E78A458A49C835829351A3853B584CA01124A1A96EB782BA23513124F01A7"
                                             2]},
                              2 {:values #{4 5 6}}},
                             :loaded-nodes 2
                             :storage {}
                             :root-id 1}
                            [1 2])
             (select-keys [:nodes :root-id :loaded-nodes])))))

(defn unload-btree [btree]
  (loop [btree btree
         cursors (cursors (first-cursor btree)
                          btree)]
    (if-let [cursor (first cursors)]
      (let [btree (unload-cursor btree
                                 cursor)]
        (recur btree
               (rest cursors)))
      btree)))


(deftest test-unload-btree
  (unload-btree {:nodes {0 {:values (sorted-set 0)}
                         1 {:values (sorted-set 1)
                            :child-ids [0 2]}
                         2 {:values (sorted-set 2)}
                         3 {:values (sorted-set 4)}
                         4 {:values (sorted-set 6 7 8)}
                         5 {:values (sorted-set 3)
                            :child-ids [1 6]}
                         6 {:values (sorted-set 5)
                            :child-ids [3 4]}},
                 :loaded-nodes 7
                 :next-node-id 7,
                 :root-id 5
                 :storage {}}))

(defn get-node-content [storage storage-key]
  (bytes-to-node (get-from-storage storage
                                   storage-key)))

(defn set-node-content [btree parent-id storage-key node-content]
  (-> btree
      (assoc-in [:nodes (:next-node-id btree)]
                node-content)
      (replace-node-id parent-id
                       storage-key
                       (:next-node-id btree))
      (update :next-node-id
              inc)

      (update :loaded-nodes
              (fnil inc 0))))

(deftest test-set-node-content
  (let [full? (full-after-maximum-number-of-values 3)
        btree (unload-btree (reduce add
                                    (create full?)
                                    (range 10)))]
    (is (= {8 {:child-ids
               ["43B0199869DFD7D8B392D4F217CFB9E57D0CB52ABB4246EB60304E81AA7999B7"
                "E33374D7B0AEF7964CBA2A2A4B48BF1DFFB7B3F2F38782959C5D3C1D3EA8D444"],
               :values #{3}}}
           (:nodes (set-node-content btree
                                     nil
                                     (:root-id btree)
                                     (get-node-content (:storage btree)
                                                       (:root-id btree))))))

    (is (= {8 {:child-ids
               [9
                "E33374D7B0AEF7964CBA2A2A4B48BF1DFFB7B3F2F38782959C5D3C1D3EA8D444"],
               :values #{3}},
            9 {:child-ids ["C4CC9E545538D9A3096C3CC138E25FA778DCFBF6836A2BBFE8AD20364C7A013F"
                           "B956188026D6E929C661B37B7E1C1D9D9BDC3643F5E8B3E9650D1FBDE21489CD"],
               :values #{1}}}
           
           (:nodes (set-node-content {:nodes {8
                                              {:child-ids
                                               ["43B0199869DFD7D8B392D4F217CFB9E57D0CB52ABB4246EB60304E81AA7999B7"
                                                "E33374D7B0AEF7964CBA2A2A4B48BF1DFFB7B3F2F38782959C5D3C1D3EA8D444"],
                                               :values #{3}}},
                                      :next-node-id 9,
                                      :root-id 8,
                                      :full? full?
                                      :storage (:storage btree)}
                                     8
                                     "43B0199869DFD7D8B392D4F217CFB9E57D0CB52ABB4246EB60304E81AA7999B7"
                                     (get-node-content (:storage btree)
                                                       "43B0199869DFD7D8B392D4F217CFB9E57D0CB52ABB4246EB60304E81AA7999B7")))))))


(defn load-node [btree-atom parent-id storage-key]
  (swap! btree-atom
         set-node-content
         parent-id
         storage-key
         (get-node-content (:storage @btree-atom)
                           storage-key)))



(defn cursor-and-btree-for-value [btree-atom value]
  (loop [btree @btree-atom
         cursor [(:root-id btree)]
         node-id (:root-id btree)]
    (if (storage-key? node-id)
      (do (load-node btree-atom
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
                          {0 {:values (sorted-set 1 2)},
                           1 {:values (sorted-set 3)
                              :child-ids [0 2]},
                           2 {:values (sorted-set 4 5 6)}}
                          :loaded-nodes 3
                          :root-id 1
                          :storage {}
                          :usages (priority-map/priority-map)
                          :next-node-id 3})]
    (swap! btree-atom
           unload-btree)

    (is (= {:nodes {3 {:values #{3},
                       :child-ids [4
                                   "6638B45DAD7C0BDDD3BBA79F164FCC125102B1B638B090910E958DE120A8EA6A"]},
                    4 {:values #{1 2}}},
            :root-id 3,
            :loaded-nodes 2
            :usages {}
            :next-node-id 5}
           (dissoc (:btree (cursor-and-btree-for-value btree-atom
                                                       1))
                   :storage)))

    (is (= [3 4]
           (:cursor (cursor-and-btree-for-value btree-atom
                                                1))))

    (is (= [3]
           (:cursor (cursor-and-btree-for-value btree-atom
                                                3))))))


(defn leaf-node-cursors-least-used-first [btree]
  (filter (fn [cursor]
            (leaf-node? (node btree
                              (last cursor))))
          (map first (:usages btree))))


(deftest test-leaf-node-cursors-least-used-first
  (is (= nil
         (leaf-node-cursors-least-used-first (reduce add
                                                     (create (full-after-maximum-number-of-values 3))
                                                     (range 10))))))

(defn unload-excess-nodes [btree maximum-node-count]
  (loop [btree btree]
    (if (< maximum-node-count
           (:loaded-nodes btree))
      (recur (unload-cursor btree
                            (first (leaf-node-cursors-least-used-first btree))))
      btree)))


(comment
  (String. (byte-array [123, 58, 118, 97, 108, 117, 101, 115, 32, 35, 123, 49, 52, 125,
                        125, 10]))

  (IsFunction.)

  (schema/check {:a (schema/eq :b)}
                {:a :b}))



(deftest test-unload-excess-nodes
  (is (match/contains-map? {:nodes
                            {1 {:child-ids [0 match/any-string],
                                :values #{1}},
                             5 {:child-ids [1 6]
                                :values #{3}},
                             6 {:values #{5 7},
                                :child-ids [3 match/any-string
                                            7]},
                             7 {:values #{8 9}}},
                            :usages {[5 6 7] 9},
                            :next-usage-number 10}
                           (dissoc (unload-excess-nodes (reduce add
                                                                (create (full-after-maximum-number-of-values 3))
                                                                (range 10))
                                                        3)
                                   :storage))))

(defn add-to-atom
  "Adds a value to an btree atom. Loads nodes from storage as needed.
  WARNING: Overrides any concurrent modifications to the atom."
  [btree-atom value]
  (let [{:keys [btree]} (cursor-and-btree-for-value btree-atom
                                                    value)]
    (reset! btree-atom
            (add btree
                 value))))


(deftest test-add-to-atom
  (let [full? (fn [node]
                (= 5 (count (:values node))))

        run-test (fn [btree value nodes-after-addition]
                   (let [btree-atom (atom btree)]
                     (swap! btree-atom
                            unload-btree)

                     (add-to-atom btree-atom
                                  value)

                     (is (= nodes-after-addition
                            (:nodes @btree-atom)))))]

    (testing "root is full"
      (run-test {:nodes {0 {:values (sorted-set 1 2 3 4 5)}}
                 :next-node-id 1
                 :loaded-nodes 1
                 :root-id 0
                 :storage {}
                 :full? full?}
                
                6
                
                {1 {:values #{1 2}},
                 2 {:child-ids [1 3], :values #{3}},
                 3 {:values #{4 5 6}}}))

    (testing "no splits needed"
      (run-test {:nodes {0 {:values (sorted-set 1 2)},
                         1 {:values (sorted-set 3), :child-ids [0 2]},
                         2 {:values (sorted-set 4 5 6)}},
                 :loaded-nodes 3
                 :next-node-id 3,
                 :root-id 1,
                 :storage {}
                 :full? full?}
                
                -1
                
                {3 {:values #{3},
                    :child-ids [4
                                "6638B45DAD7C0BDDD3BBA79F164FCC125102B1B638B090910E958DE120A8EA6A"]},
                 4 {:values #{-1 1 2}}}))

    (testing "leaf is full"
      (run-test {:nodes
                 {0 {:values (sorted-set -3 -2 -1 0 1)},
                  1 {:values (sorted-set 3), :child-ids [0 2]},
                  2 {:values (sorted-set 4 5 6)}},
                 :loaded-nodes 3
                 :next-node-id 3,
                 :root-id 1,
                 :storage {}
                 :full? full?}

                2

                {3 {:values #{-1 3},
                    :child-ids [4
                                5
                                "6638B45DAD7C0BDDD3BBA79F164FCC125102B1B638B090910E958DE120A8EA6A"]},
                 4 {:values #{-3 -2}},
                 5 {:values #{0 1 2}}}))))

(defn btree-and-node-id-after-splitter [btree-atom splitter]
  (let [{:keys [cursor btree]} (cursor-and-btree-for-value btree-atom
                                                           splitter)

        node-with-the-splitter (node btree
                                     (last cursor))
        node-id-after-splitter (get (:child-ids node-with-the-splitter)
                                    (inc (find-index (:values node-with-the-splitter)
                                                     splitter)))]
    
    {:node-id-after-splitter node-id-after-splitter
     :btree btree
     :cursor (conj cursor
                   node-id-after-splitter)}))

(defn sequence-after-splitter [btree-atom splitter]

  (let [{:keys [node-id-after-splitter btree cursor]} (btree-and-node-id-after-splitter btree-atom
                                                                                        splitter)]

    (loop [btree btree
           cursor cursor
           node-id node-id-after-splitter]
      (if (storage-key? node-id)
        (do (load-node btree-atom
                       (parent-id cursor)
                       node-id)
            (let [{:keys [node-id-after-splitter btree cursor]} (btree-and-node-id-after-splitter btree-atom
                                                                                                  splitter)]
              (recur btree
                     cursor
                     node-id-after-splitter)))
        (let [the-node (node btree
                             node-id)]
          (if (leaf-node? the-node)
            (append-if-not-null (:values the-node)
                                (splitter-after-cursor btree
                                                       cursor))
            (let [the-child-id (first (:child-ids the-node))]
              (recur btree
                     (conj cursor the-child-id)
                     the-child-id))))))))

(deftest test-sequence-after-splitter
  (let [btree-atom (atom {:nodes {0 {:values (sorted-set 0)}
                                  1 {:values (sorted-set 1)
                                     :child-ids [0 2]}
                                  2 {:values (sorted-set 2)}
                                  3 {:values (sorted-set 4)}
                                  4 {:values (sorted-set 6 7 8)}
                                  5 {:values (sorted-set 3)
                                     :child-ids [1 6]}
                                  6 {:values (sorted-set 5)
                                     :child-ids [3 4]}},
                          :loaded-nodes 7
                          :next-node-id 7,
                          :root-id 5
                          :storage {}})]
    (swap! btree-atom
           unload-btree)

    (are [splitter sequence]
        (= sequence
           (sequence-after-splitter btree-atom
                                    splitter))

      3   [4 5]
      8   nil)))


(defn sequence-for-value [btree-atom value]
  (let [{:keys [cursor btree]} (cursor-and-btree-for-value btree-atom
                                                           value)]
    (let [the-node (node btree
                         (last cursor))]
      (if (leaf-node? the-node)
        (append-if-not-null (subseq (:values the-node)
                                    >=
                                    value)
                            (splitter-after-cursor btree
                                                   cursor))
        [value]))))

(deftest test-sequence-for-value
  (let [btree-atom (atom {:nodes
                          {0 {:values (sorted-set 1 2)},
                           1 {:values (sorted-set 3)
                              :child-ids [0 2]},
                           2 {:values (sorted-set 4 5 6)}}
                          :root-id 1
                          :loaded-nodes 3
                          :storage {}
                          :next-node-id 3})]
    (swap! btree-atom
           unload-btree)

    (are [value sequence]
        (= sequence
           (sequence-for-value btree-atom
                               value))

      0   [1 2 3]
      1   [1 2 3]
      3   [3]
      -10 [1 2 3]
      5   [5 6]
      50  nil)))

(defn lazy-sequence [btree-atom sequence]
  (if-let [sequence (if (first (rest sequence))
                      sequence
                      (if-let [value (first sequence)]
                        (cons value
                              (sequence-after-splitter btree-atom
                                                       (first sequence)))
                        nil))]
    (lazy-seq (cons (first sequence)
                    (lazy-sequence btree-atom
                                   (rest sequence))))
    nil))

(defn inclusive-subsequence [btree-atom value]
  (lazy-sequence btree-atom
                 (sequence-for-value btree-atom
                                     value)))

(deftest test-inclusive-subsequence
  (let [btree-atom (atom {:nodes
                          {0 {:values (sorted-set 1 2)},
                           1 {:values (sorted-set 3)
                              :child-ids [0 2]},
                           2 {:values (sorted-set 4 5 6)}}
                          :root-id 1
                          :loaded-nodes 3
                          :next-node-id 3
                          :storage {}})]

    (swap! btree-atom
           unload-btree)
    
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

(deftest test-btree
  (repeatedly 100 
              (let [maximum 1000
                    values (take 200
                                 (repeatedly (fn [] (rand-int maximum))))
                    smallest (rand maximum)]
                (is (= (subseq (apply sorted-set
                                      values)
                               >=
                               smallest)
                       (inclusive-subsequence (atom (reduce add
                                                            (create (full-after-maximum-number-of-values 3))
                                                            values))
                                              smallest))))))


(deftest test-save-to-storage
  (let [full? (full-after-maximum-number-of-values 3)]
    (is {:nodes {},
         :next-node-id 8,
         :root-id "E8E43D7CC21CEAE35F88DF61E1846070DF17B13B8E683D3D9F592A324D640B92",
         :full? full?}
        (unload-btree (reduce add
                              (create full?)
                              (range 10))))))





(comment
  (let [usage (-> (priority-map/priority-map [1 2] 1 [1 3] 2 [1 4] 3)
                  (assoc [1 2] 4))]
    
    (drop 1 usage)))

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



