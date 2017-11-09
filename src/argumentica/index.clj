(ns argumentica.index
  (:require [flow-gl.tools.trace :as trace]
            [flow-gl.debug :as debug]
            [argumentica.cryptography :as cryptography]
            [argumentica.encode :as encode]
            [argumentica.graph :as graph]
            [clojure.java.io :as io])
  (:use [clojure.test])
  (:import [java.io DataInputStream DataOutputStream]
           [java.nio.file Files Paths OpenOption]
           [java.nio.file.attribute FileAttribute]))

(defn create-node []
  {:values (sorted-set)})

(defn full-after-maximum-number-of-values [maximum]
  (fn [node]
    (= maximum
       (count (:values node)))))

(defn create
  ([]
   (create (full-after-maximum-number-of-values 5)
           {}))

  ([full?]
   (create full?
           {}))
  
  ([full? storage]
   {:nodes {0 (create-node)}
    :next-node-id 1
    :root-id 0
    :full? full?
    :storage storage}))

(defmulti get-from-storage
  (fn [storage key]
    (type storage)))

(defmethod get-from-storage
  (type {})
  [storage key]
  (get storage
       key))

(defmulti put-to-storage
  (fn [storage key value]
    (type storage)))

(defmethod put-to-storage
  (type {})
  [storage key value]
  (assoc storage
         key
         value))


(defrecord DirectoryStorage [path])


(defn string-to-path [string]
  (Paths/get string
             (into-array String [])))

(defmethod get-from-storage
  DirectoryStorage
  [this key]
  (Files/readAllBytes (string-to-path (str (:path this) "/" key))))

(defmethod put-to-storage
  DirectoryStorage
  [this key bytes]
  (Files/write (string-to-path (str (:path this) "/" key))
               bytes
               (into-array OpenOption [])))

(defn create-directories [path]
  (Files/createDirectories (string-to-path path)
                           (into-array FileAttribute [])))

(comment (String. (get-from-storage (DirectoryStorage. "src/argumentica")
                                    "index.clj"))

         (put-to-storage (DirectoryStorage. "")
                         "/Users/jukka/Downloads/test.txt"
                         (.getBytes "test"
                                    "UTF-8")))

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

(defn distribute-children [index old-node-id new-node-id]
  (if-let [child-ids (get-in index [:nodes old-node-id :child-ids])]
    (let [[lesser-child-ids greater-child-ids] (partition (/ (count child-ids)
                                                             2)
                                                          child-ids)]
      (-> index
          (assoc-in [:nodes old-node-id :child-ids] (vec lesser-child-ids))
          (assoc-in [:nodes new-node-id :child-ids] (vec greater-child-ids))))
    index))

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

(defn split-child [index parent-id old-child-id]
  (let [{:keys [lesser-values greater-values median-value]} (split-sorted-set (get-in index [:nodes old-child-id :values]))
        new-child-id (:next-node-id index)]
    (-> index
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

(defn split-root [index]
  (let [new-root-id (:next-node-id index)]
    (-> index
        (update :next-node-id inc)
        (assoc :root-id new-root-id)
        (assoc-in [:nodes new-root-id] {:child-ids [(:root-id index)]
                                        :values (sorted-set)})
        (split-child new-root-id (:root-id index)))))

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

(defn node [index node-id]
  (get-in index [:nodes node-id]))

(defn add [index value]
  (loop [index (if ((:full? index) (node index
                                         (:root-id index)))
                 (split-root index)
                 index)
         node-id (:root-id index)]
    (let [the-node (node index
                         node-id)]
      (if (leaf-node? the-node)
        (update-in index
                   [:nodes node-id :values]
                   conj value)
        (if-let [the-child-id (child-id the-node
                                        value)]
          (let [index (if ((:full? index) (node index
                                                the-child-id))
                        (split-child index
                                     node-id
                                     the-child-id)
                        index)]
            (if-let [the-child-id (child-id (node index
                                                  node-id)
                                            value)]
              (recur index
                     the-child-id)
              index))
          index)))))


(deftest test-add
  (let [full? (fn [node]
                (= 5 (count (:values node))))]
    (testing "root is full"
      (is (= {0 {:values #{1 2}},
              1 {:values #{3}, :child-ids [0 2]},
              2 {:values #{4 5 6}}}
             (:nodes (add {:nodes {0 {:values (sorted-set 1 2 3 4 5)}}
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

(defn parent-id [cursor]
  (last (drop-last cursor)))

(defn next-cursor [index cursor]
  (loop [cursor cursor]
    (if-let [parent (node index
                          (parent-id cursor))]
      (if-let [next-node-id-downwards (get (:child-ids parent)
                                           (inc (find-index (:child-ids parent)
                                                            (last cursor))))]
        (loop [cursor (conj (vec (drop-last cursor))
                            next-node-id-downwards)]
          (let [next-node-downwards (node index
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

(defn splitter-after-cursor [index cursor]
  (loop [cursor cursor]
    (if-let [parent (node index
                          (last (drop-last cursor)))]
      (if (empty? (children-after parent
                                  (last cursor)))
        (recur (drop-last cursor))
        (splitter-after-child parent
                              (last cursor)))
      nil)))


(deftest test-splitter-after-cursor
  (let [index {:nodes {0 {:values (sorted-set 0)}
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
           (splitter-after-cursor index
                                  [5 1 2])))

    (is (= nil
           (splitter-after-cursor index
                                  [5 6 4])))

    (is (= 5
           (splitter-after-cursor index
                                  [5 6 3]))))


  (let [index {:nodes
               {0 {:values (sorted-set 1 2)},
                1 {:values (sorted-set 3)
                   :child-ids [0 2]},
                2 {:values (sorted-set 4 5 6)}}
               :root-id 1}]

    (is (= 3
           (splitter-after-cursor index
                                  [1 0])))

    (is (= nil
           (splitter-after-cursor index
                                  [1 2])))))

(defn append-if-not-null [collection value]
  (if value
    (concat collection
            [value])
    collection))

(defn sequence-for-cursor [index cursor]
  (append-if-not-null (seq (:values (node index
                                          (last cursor))))
                      (splitter-after-cursor index
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

(defn first-cursor [index]
  (loop [cursor [(:root-id index)]
         node-id (:root-id index)]
    (let [the-node (node index
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


(defn replace-node-id [index parent-id old-child-id new-child-id]
  (if parent-id
    (update-in index
               [:nodes parent-id :child-ids]
               (partial replace
                        {old-child-id new-child-id}))
    (assoc index :root-id new-child-id)))

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

(defn cursors [cursor index]
  (if-let [the-next-cursor (next-cursor index
                                        cursor)]
    (lazy-seq (cons cursor
                    (cursors (next-cursor index
                                          cursor)
                             index)))
    [cursor]))

(deftest test-cursors
  (is (= '([1 0]
           [1 2])
         (let [index {:nodes {0 {:values (sorted-set 1 2)},
                              1 {:values (sorted-set 3), :child-ids [0 2]},
                              2 {:values (sorted-set 4 5 6)}}
                      :root-id 1}]
           (cursors (first-cursor index)
                    index)))))

(defn unload-cursor [index cursor]
  (prn "unload-cursor" cursor)
  (loop [cursor cursor
         index index]
    (let [node-id-to-be-unloded (last cursor)
          parent-id (last (drop-last cursor))
          bytes (node-to-bytes (node index
                                     node-id-to-be-unloded))
          the-storage-key (storage-key bytes)
          index (-> index
                    (replace-node-id parent-id
                                     node-id-to-be-unloded
                                     the-storage-key)
                    (update :nodes dissoc node-id-to-be-unloded)
                    (update :storage
                            put-to-storage
                            the-storage-key
                            bytes))]
      (if (and parent-id
               (every? (complement loaded?)
                       (:child-ids (node index
                                         parent-id))))
        (recur (drop-last cursor)
               index)
        index))))

(deftest test-unload-cursor
  (is (= {1 {:values #{3},
             :child-ids ["B58E78A458A49C835829351A3853B584CA01124A1A96EB782BA23513124F01A7"
                         2]},
          2 {:values #{4 5 6}}}
         (:nodes (unload-cursor {:nodes {0 {:values (sorted-set 1 2)},
                                         1 {:values (sorted-set 3), :child-ids [0 2]},
                                         2 {:values (sorted-set 4 5 6)}}
                                 :root-id 1
                                 :storage {}}
                                [1 0]))))

  (is (= {:nodes {},
          :root-id "7761BC24541DD827301A1AFE64FB273ED86C1B7F14A474192575A4A3428C0732"}
         (-> (unload-cursor {:nodes
                             {1 {:values #{3},
                                 :child-ids ["B58E78A458A49C835829351A3853B584CA01124A1A96EB782BA23513124F01A7"
                                             2]},
                              2 {:values #{4 5 6}}},
                             :storage {}
                             :root-id 1}
                            [1 2])
             (select-keys [:nodes :root-id]))))


  )

(defn unload-index [index]
  (loop [index index
         cursors (cursors (first-cursor index)
                          index)]
    (if-let [cursor (first cursors)]
      (let [index (unload-cursor index
                                 cursor)]
        (recur index
               (rest cursors)))
      index)))


(deftest test-unload-index
  (unload-index {:nodes {0 {:values (sorted-set 0)}
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
                 :root-id 5
                 :storage {}}))

(defn get-node-content [storage storage-key]
  (bytes-to-node (get-from-storage storage
                                   storage-key)))

(defn set-node-content [index parent-id storage-key node-content]
  (-> index
      (assoc-in [:nodes (:next-node-id index)]
                node-content)
      (replace-node-id parent-id
                       storage-key
                       (:next-node-id index))
      (update :next-node-id
              inc)))

(deftest test-set-node-content
  (let [full? (full-after-maximum-number-of-values 3)
        index (unload-index (reduce add
                                    (create full?)
                                    (range 10)))]
    (is (= {8 {:child-ids
               ["43B0199869DFD7D8B392D4F217CFB9E57D0CB52ABB4246EB60304E81AA7999B7"
                "E33374D7B0AEF7964CBA2A2A4B48BF1DFFB7B3F2F38782959C5D3C1D3EA8D444"],
               :values #{3}}}
           (:nodes (set-node-content index
                                     nil
                                     (:root-id index)
                                     (get-node-content (:storage index)
                                                       (:root-id index))))))

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
                                      :storage (:storage index)}
                                     8
                                     "43B0199869DFD7D8B392D4F217CFB9E57D0CB52ABB4246EB60304E81AA7999B7"
                                     (get-node-content (:storage index)
                                                       "43B0199869DFD7D8B392D4F217CFB9E57D0CB52ABB4246EB60304E81AA7999B7")))))))


(defn load-node [index-atom parent-id storage-key]
  (swap! index-atom
         set-node-content
         parent-id
         storage-key
         (get-node-content (:storage @index-atom)
                           storage-key)))



(defn cursor-and-index-for-value [index-atom value]
  (loop [index @index-atom
         cursor [(:root-id index)]
         node-id (:root-id index)]
    (if (storage-key? node-id)
      (do (load-node index-atom
                     (parent-id cursor)
                     node-id)
          (let [index @index-atom]
            (recur index
                   [(:root-id index)]
                   (:root-id index))))
      (let [the-node (node index
                           node-id)]
        (if (leaf-node? the-node)
          {:index index
           :cursor cursor}
          (if-let [the-child-id (child-id the-node
                                          value)]
            (recur index
                   (conj cursor the-child-id)
                   the-child-id)
            {:index index
             :cursor cursor}))))))


(deftest test-cursor-and-index-for-value
  (let [index-atom (atom {:nodes
                          {0 {:values (sorted-set 1 2)},
                           1 {:values (sorted-set 3)
                              :child-ids [0 2]},
                           2 {:values (sorted-set 4 5 6)}}
                          :root-id 1
                          :storage {}
                          :next-node-id 3})]
    (swap! index-atom
           unload-index)

    (is (= {:nodes {3 {:values #{3},
                       :child-ids [4
                                   "6638B45DAD7C0BDDD3BBA79F164FCC125102B1B638B090910E958DE120A8EA6A"]},
                    4 {:values #{1 2}}},
            :root-id 3,
            :next-node-id 5}
           (dissoc (:index (cursor-and-index-for-value index-atom
                                                       1))
                   :storage)))

    (is (= [3 4]
           (:cursor (cursor-and-index-for-value index-atom
                                                1))))

    (is (= [3]
           (:cursor (cursor-and-index-for-value index-atom
                                                3))))))

(defn index-and-node-id-after-splitter [index-atom splitter]
  (let [{:keys [cursor index]} (cursor-and-index-for-value index-atom
                                                           splitter)

        node-with-the-splitter (node index
                                     (last cursor))
        node-id-after-splitter (get (:child-ids node-with-the-splitter)
                                    (inc (find-index (:values node-with-the-splitter)
                                                     splitter)))]
    
    {:node-id-after-splitter node-id-after-splitter
     :index index
     :cursor (conj cursor
                   node-id-after-splitter)}))

(defn sequence-after-splitter [index-atom splitter]

  (let [{:keys [node-id-after-splitter index cursor]} (index-and-node-id-after-splitter index-atom
                                                                                        splitter)]

    (loop [index index
           cursor cursor
           node-id node-id-after-splitter]
      (if (storage-key? node-id)
        (do (load-node index-atom
                       (parent-id cursor)
                       node-id)
            (let [{:keys [node-id-after-splitter index cursor]} (index-and-node-id-after-splitter index-atom
                                                                                                  splitter)]
              (recur index
                     cursor
                     node-id-after-splitter)))
        (let [the-node (node index
                             node-id)]
          (if (leaf-node? the-node)
            (append-if-not-null (:values the-node)
                                (splitter-after-cursor index
                                                       cursor))
            (let [the-child-id (first (:child-ids the-node))]
              (recur index
                     (conj cursor the-child-id)
                     the-child-id))))))))

(deftest test-sequence-after-splitter
  (let [index-atom (atom {:nodes {0 {:values (sorted-set 0)}
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
                          :root-id 5
                          :storage {}})]
    (swap! index-atom
           unload-index)

    (are [splitter sequence]
        (= sequence
           (sequence-after-splitter index-atom
                                    splitter))

      3   [4 5]
      8   nil)))


(defn sequence-for-value [index-atom value]
  (let [{:keys [cursor index]} (cursor-and-index-for-value index-atom
                                                           value)]
    (let [the-node (node index
                         (last cursor))]
      (if (leaf-node? the-node)
        (append-if-not-null (subseq (:values the-node)
                                    >=
                                    value)
                            (splitter-after-cursor index
                                                   cursor))
        [value]))))

(deftest test-sequence-for-value
  (let [index-atom (atom {:nodes
                          {0 {:values (sorted-set 1 2)},
                           1 {:values (sorted-set 3)
                              :child-ids [0 2]},
                           2 {:values (sorted-set 4 5 6)}}
                          :root-id 1
                          :storage {}
                          :next-node-id 3})]
    (swap! index-atom
           unload-index)

    (are [value sequence]
        (= sequence
           (sequence-for-value index-atom
                               value))

      0   [1 2 3]
      1   [1 2 3]
      3   [3]
      -10 [1 2 3]
      5   [5 6]
      50  nil)))

(defn lazy-sequence [index-atom sequence]
  (if-let [sequence (if (first (rest sequence))
                      sequence
                      (if-let [value (first sequence)]
                        (cons value
                              (sequence-after-splitter index-atom
                                                       (first sequence)))
                        nil))]
    (lazy-seq (cons (first sequence)
                    (lazy-sequence index-atom
                                   (rest sequence))))
    nil))

(defn inclusive-subsequence [index-atom value]
  (lazy-sequence index-atom
                 (sequence-for-value index-atom
                                     value)))

(deftest test-inclusive-subsequence
  (let [index-atom (atom {:nodes
                          {0 {:values (sorted-set 1 2)},
                           1 {:values (sorted-set 3)
                              :child-ids [0 2]},
                           2 {:values (sorted-set 4 5 6)}}
                          :root-id 1
                          :next-node-id 3
                          :storage {}})]

    (swap! index-atom
           unload-index)
    
    (is (= [1 2 3 4 5 6]
           (inclusive-subsequence index-atom
                                  0)))

    (is (= [2 3 4 5 6]
           (inclusive-subsequence index-atom
                                  2)))

    (is (= [3 4 5 6]
           (inclusive-subsequence index-atom
                                  3)))

    (is (= [4 5 6]
           (inclusive-subsequence index-atom
                                  4)))

    (is (= nil
           (inclusive-subsequence index-atom
                                  7)))

    (let [values (range 200)]
      (is (= values
             (inclusive-subsequence (atom (reduce add
                                                  (create (full-after-maximum-number-of-values 3))
                                                  values))
                                    (first values)))))))

(deftest test-index
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
        (unload-index (reduce add
                              (create full?)
                              (range 10))))))






#_(defn start []
    (let [{:keys [index storage]} (unload-index (reduce add
                                                        (create (full-after-maximum-number-of-values 3))
                                                        (range 30))
                                                {}
                                                assoc)
          root-node-key (:storage-key (node index
                                            (:root-id index)))]
      #_(load-index storage
                    root-node-key)
      index
      #_(String. (get storage
                      (:storage-key (node index
                                          (:root-id index))))
                 "UTF-8")))
