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
            [clojure.test.check.properties :as properties]))

(defn create-sorted-set [& keys]
  (apply sorted-set-by
         comparator/compare-datoms
         keys))

(defn create-node []
  {:values (create-sorted-set)})

(defn full-after-maximum-number-of-values [maximum]
  (assert (odd? maximum)
          "Maximum node size must be odd")
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

(defn node [btree node-id]
  (get-in btree [:nodes node-id]))

(defn add-value-in-node [btree node-id value]
  (-> btree
      (update-in [:nodes node-id :values]
                 conj value)
      (record-usage node-id)))

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



(defn splitter-besides-child [node child-id before]
  ;; TODO: This is linear time. Could we find the last value in
  ;; the child node and then find the splitter both in logarithmic time?
  (nth (seq (:values node))
       (let [index (find-index (:child-ids node)
                               child-id)]
         (if before
           (dec index)
           index)) 
       nil))

(deftest test-splitter-besides-child
  (is (= 3
         (splitter-besides-child {:values (create-sorted-set 3)
                                  :child-ids [0 2]}
                                 0
                                 false)))
  (is (= nil
         (splitter-besides-child {:values (create-sorted-set 3)
                                  :child-ids [0 2]}
                                 2
                                 false)))

  (is (= nil
         (splitter-besides-child {:values (create-sorted-set 3)
                                  :child-ids [0 2]}
                                 0
                                 true)))
  (is (= 3
         (splitter-besides-child {:values (create-sorted-set 3)
                                  :child-ids [0 2]}
                                 2
                                 true))))



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
        (splitter-besides-child parent
                                (last cursor)
                                false))
      nil)))


(deftest test-splitter-after-cursor
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
           (splitter-after-cursor btree
                                  [5 1 2])))

    (is (= nil
           (splitter-after-cursor btree
                                  [5 6 4])))

    (is (= 5
           (splitter-after-cursor btree
                                  [5 6 3]))))


  (let [btree {:nodes
               {0 {:values (create-sorted-set 1 2)},
                1 {:values (create-sorted-set 3)
                   :child-ids [0 2]},
                2 {:values (create-sorted-set 4 5 6)}}
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
                               {0 {:values (create-sorted-set 1 2)},
                                1 {:values (create-sorted-set 3), :child-ids [0 2]},
                                2 {:values (create-sorted-set 4 5 6)}}}
                              [1 0])))
  (is (= [4 5 6]
         (sequence-for-cursor {:nodes
                               {0 {:values (create-sorted-set 1 2)},
                                1 {:values (create-sorted-set 3), :child-ids [0 2]},
                                2 {:values (create-sorted-set 4 5 6)}}}
                              [1 2]))))

(defn loaded-node-count [btree]
  (count (keys (:nodes btree))))

(defn first-cursor [btree]
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

(deftest test-first-cursor
  (is (= [1 0]
         (first-cursor {:nodes {0 {:values (create-sorted-set 1 2)},
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


(defn unload-cursor [btree cursor]
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
                               bytes))

    (-> btree
        (replace-node-id the-parent-id
                         node-id-to-be-unloded
                         the-storage-key)
        (update :nodes dissoc node-id-to-be-unloded)
        (update :usages dissoc node-id-to-be-unloded))))

(deftest test-unload-cursor

  (is (thrown? AssertionError
               (unload-cursor {:nodes {0 {:values (create-sorted-set 1 2)},
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
                           (unload-cursor {:nodes {0 {:values (create-sorted-set 1 2)},
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
                           (unload-cursor {:nodes
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
                           (unload-cursor {:nodes
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

(defn cursors [cursor btree]
  (if-let [the-next-cursor (next-cursor btree
                                        cursor)]
    (lazy-seq (cons cursor
                    (cursors (next-cursor btree
                                          cursor)
                             btree)))
    [cursor]))

(declare add)

(deftest test-cursors
  (is (= '([1 0]
           [1 2])
         (let [btree {:nodes {0 {:values (create-sorted-set 1 2)},
                              1 {:values (create-sorted-set 3), :child-ids [0 2]},
                              2 {:values (create-sorted-set 4 5 6)}}
                      :root-id 1}]
           (cursors (first-cursor btree)
                    btree))))
  (is (= [[13 5 1 0]
          [13 5 1 2]
          [13 5 6 match/any-string]
          [13 5 6 match/any-string]
          [13 14 9 7]
          [13 14 9 8]
          [13 14 12 10]
          [13 14 12 11]
          [13 14 12 15]
          [13 14 12 16]]
         (let [btree (-> (reduce add
                                 (create (full-after-maximum-number-of-values 3))
                                 (range 20))
                         (unload-cursor [13 5 6 3])
                         (unload-cursor [13 5 6 4]))]
           (cursors [13 5 1 0]
                    btree)))))

(defn all-cursors [btree]
  (cursors (first-cursor btree)
           btree))

(defn get-node-content [storage storage-key]
  (bytes-to-node (storage/get-from-storage! storage
                                            storage-key)))

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

(defn load-nodes-for-value [btree value]
  (loop [btree (load-root-if-needed btree)
         cursor [(:root-id btree)]
         node-id (:root-id btree)]

    (let [the-node (node btree
                         node-id)]
      (if (leaf-node? the-node)
        {:btree btree
         :cursor cursor}
        (if-let [the-child-id (child-id the-node
                                        value)]
          (if (storage-key? the-child-id)
            (recur (set-node-content btree
                                     node-id
                                     the-child-id
                                     (get-node-content (:node-storage btree)
                                                       the-child-id))
                   (conj cursor
                         (:next-node-id btree))

                   (:next-node-id btree))
            (recur btree
                   (conj cursor
                         the-child-id)
                   the-child-id))
          {:cursor cursor
           :btree btree})))))


(defspec propertytest-load-nodes-for-value
  (properties/for-all [values (gen/vector gen/int)]
                      (if-let [value (first values)]
                        (let [{:keys [cursor btree]} (load-nodes-for-value (unload-btree (reduce add
                                                                                                 (create (full-after-maximum-number-of-values 3))
                                                                                                 values))
                                                                           value)]
                          (contains? (:values (get (:nodes btree)
                                                   (last cursor)))
                                     value))
                        true)))


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

(deftest test-loaded-node-count
  (is (= 3
         (loaded-node-count {:nodes {0 {:values #{0}}
                                     1 {:child-ids [0 2], :values #{1}}
                                     2 {:values #{2 3 4}}}}))))

(defn unload-least-used-node [btree]
  (if-let [cursor (least-used-cursor btree)]
    (unload-cursor btree
                   cursor)
    btree))

(defn unload-excess-nodes [btree maximum-node-count]
  (loop [btree btree]
    (if (< maximum-node-count
           (loaded-node-count btree))
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

(defn btree-and-node-id-besides-splitter [btree-atom splitter direction]
  (let [{:keys [cursor btree]} (cursor-and-btree-for-value-and-btree-atom btree-atom
                                                                          splitter)

        node-with-the-splitter (node btree
                                     (last cursor))
        node-id-after-splitter (get (:child-ids node-with-the-splitter)
                                    ((if (= :after direction)
                                       inc
                                       dec)
                                     (find-index (:values node-with-the-splitter)
                                                       splitter)))]

    {:node-id-after-splitter node-id-after-splitter
     :btree btree
     :cursor (conj cursor
                   node-id-after-splitter)}))

(defn sequence-after-splitter [btree-atom splitter]
  (let [{:keys [node-id-after-splitter btree cursor]} (btree-and-node-id-besides-splitter btree-atom
                                                                                          splitter
                                                                                          :after)]

    (loop [btree btree
           cursor cursor
           node-id node-id-after-splitter]
      (if (storage-key? node-id)
        (do (load-node-to-atom btree-atom
                               (parent-id cursor)
                               node-id)
            (let [{:keys [node-id-after-splitter btree cursor]} (btree-and-node-id-besides-splitter btree-atom
                                                                                                    splitter
                                                                                                    :after)]
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
                                    splitter))

      3   [4 5]
      8   nil)))


(defn sequence-for-value [btree-atom value]
  (let [{:keys [cursor btree]} (cursor-and-btree-for-value-and-btree-atom btree-atom
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
                               value))

      0   [1 2 3]
      1   [1 2 3]
      3   [3]
      -10 [1 2 3]
      5   [5 6]
      50  nil
      ::comparator/min [1 2 3])))

(defn lazy-value-sequence [btree-atom sequence]
  (if-let [sequence (if (first (rest sequence))
                      sequence
                      (if-let [value (first sequence)]
                        (cons value
                              (sequence-after-splitter btree-atom
                                                       (first sequence)))
                        nil))]
    (lazy-seq (cons (first sequence)
                    (lazy-value-sequence btree-atom
                                         (rest sequence))))
    nil))

(defn inclusive-subsequence [btree-atom value]
  (lazy-value-sequence btree-atom
                       (sequence-for-value btree-atom
                                           value)))

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

(deftest test-btree
  (repeatedly 100
              (let [maximum 1000
                    values (take 200
                                 (repeatedly (fn [] (rand-int maximum))))
                    smallest (rand maximum)]
                (is (= (subseq (apply create-sorted-set
                                      values)
                               >=
                               smallest)
                       (inclusive-subsequence (atom (reduce add
                                                            (create (full-after-maximum-number-of-values 3))
                                                            values))
                                              smallest))))))

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
                                            (unload-cursor [13 5 6 3])
                                            (unload-cursor [13 5 6 4])
                                            (unload-btree)))
                                  0))))

  (testing "unload btree when first nodes are unloaded"
    (is (= '(0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19)
           (inclusive-subsequence (atom (-> (reduce add
                                                    (create (full-after-maximum-number-of-values 3))
                                                    (range 20))

                                            (unload-cursor [13 5 1 0])
                                            (unload-cursor [13 5 1 2])
                                            (unload-btree)))
                                  0)))))

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
  ;; TODO: allow writing nodes to storage without unloading them
  (-> btree
      (unload-btree)
      (add-stored-root metadata)))

(defn load-first-cursor [btree-atom]
  (loop [cursor (first-cursor @btree-atom)]
    (if (not (loaded-node-id? (last cursor)))
      (recur (first-cursor (load-node-to-atom btree-atom
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
  (reduce (fn [keys root]
            (apply conj keys
                   (storage-keys-from-metadata (:metadata-storage btree)
                                               (:storage-key root))))
          #{}
          (get-metadata btree
                        "roots")))

(defn unused-storage-keys [btree]
  (filter (complement (used-storage-keys btree))
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

#_(defn collect-storage-garbage [btree]
    (update btree :node-storage
            (fn [storage]
              (reduce remove-from-storage
                      storage
                      (unused-storage-keys btree)))))


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
