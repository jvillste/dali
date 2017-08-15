(ns argumentica.db
  (:require [datascript.core :as d]
            [datascript.db :as db]
            [clojure.uuid :as uuid]
            [loom.alg :as alg]
            [loom.graph :as graph]
            [loom.io :as loom-io]
            [clojure.set :as set])
  (:import [java.security MessageDigest]
           [java.util Base64]
           [com.google.common.io BaseEncoding]
           [java.util UUID])
  (:use clojure.test))

(comment 
  (-> (d/empty-db)
      (d/conn-from-db)
      (d/transact! [{:db/id 1, :age 19}
                    {:db/id 2, :age "a"}
                    {:db/id 3, :age "b"}
                    {:db/id 4, :age 2}])))


#_(deftest test-entity


    #_(let [db (-> (d/empty-db)
                   (d/db-with [{:db/id 1, :age 19}
                               {:db/id 2, :age "a"}
                               {:db/id 3, :age "b"}
                               {:db/id 4, :age 2}]))]
        (is (= nil
               (d/datoms db :eavt)))
        (is (= (:age (d/entity db 1)) 19))
        (is (= (:age (d/entity db 2)) "foo"))))

#_(let [db (:db-after (d/with (d/empty-db)
                              [{:db/id 1, :name "Foo"}] :haa))]

    (d/datoms db :eavt))


#_(def compare-transaction-index [a b]
    (if (= :max a)
      ))

(defn uuid []
  (.toString (UUID/randomUUID)))

#_ (uuid)

(defn base-64-encode [bytes]
  (.encode (BaseEncoding/base64)
           bytes))

(defn base-16-encode [bytes]
  (.encode (BaseEncoding/base16)
           bytes))

(defn sha-256 [message]
  (.digest (MessageDigest/getInstance "SHA-256")
           (.getBytes message "UTF-8")))

(defn sha-1 [message]
  (.digest (MessageDigest/getInstance "SHA-1")
           (.getBytes message "UTF-8")))


(defn transaction-hash [transaction]
  (base-16-encode (sha-256 (pr-str (select-keys transaction [:statements :parents])))))

(defn eatvc-statement-value [statement]
  (nth statement 4))

(defn eatvc-statement-vector-to-map [statement]
  {:entity (nth statement 0)
   :attribute (nth statement 1)
   :transaction (nth statement 2)
   :command (nth statement 3)
   :value (nth statement 4)})

(defn reduce-values [values statement-map]
  (case (:command statement-map)
    :add (conj values (:value statement-map))
    :retract (disj values (:value statement-map))
    :set  #{(:value statement-map)}
    values))

(defn eatvc-statements
  ([eatvc e]
   (eatvc-statements eatvc e nil 0))
  
  ([eatvc e a]
   (eatvc-statements eatvc e a 0))

  ([eatvc e a t]
   (take-while (fn [statement]
                 (and (= (first statement)
                         e)
                      (if a
                        (= (second statement)
                           a)
                        true)))
               (subseq eatvc >= [e a t nil nil])))

  ([eatvc e a t-from t-to]
   (take-while (fn [statement]
                 (<= (nth statement 2)
                     t-to))
               (eatvc-statements eatvc e a t-from))))

(deftest eatvc-statements-test
  (is (= '([2 :friend 1 :add "2 frend 1"] [2 :friend 2 :add "2 frend 2"])
         (eatvc-statements  (sorted-set [1 :friend 1 :add "1 frend 1"]
                                        [2 :friend 1 :add "2 frend 1"]
                                        [2 :friend 2 :add "2 frend 2"]
                                        [2 :name 2 :add "2 frend 2"]
                                        [3 :friend 2 :add "2 frend 2"])
                            2
                            :friend)))
  
  (is (= '([2 :friend 1 :add "2 frend 1"]
           [2 :friend 2 :add "2 frend 2"]
           [2 :name 2 :add "2 frend 2"])
         (eatvc-statements (sorted-set [1 :friend 1 :add "1 frend 1"]
                                       [2 :friend 1 :add "2 frend 1"]
                                       [2 :friend 2 :add "2 frend 2"]
                                       [2 :name 2 :add "2 frend 2"]
                                       [3 :friend 2 :add "2 frend 2"])
                           2)))

  (is (= '([2 :friend 2 :add "2 frend 2"])
         (eatvc-statements (sorted-set [1 :friend 1 :add "1 frend 1"]
                                       [2 :friend 1 :add "2 frend 1"]
                                       [2 :friend 2 :add "2 frend 2"]
                                       [2 :name 2 :add "2 frend 2"]
                                       [3 :friend 2 :add "2 frend 2"])
                           2
                           :friend
                           2)))

  (is (= '([1 :friend 2 :add "frend 2"]
           [1 :friend 3 :add "frend 3"])
         (eatvc-statements (sorted-set [1 :friend 1 :add "frend 1"]
                                       [1 :friend 2 :add "frend 2"]
                                       [1 :friend 3 :add "frend 3"]
                                       [1 :friend 4 :add "frend 4"])
                           1
                           :friend
                           2
                           3))))

(defn get-eatvc-values [eatvc e a]
  (reduce reduce-values #{}
          (map eatvc-statement-vector-to-map
               (eatvc-statements eatvc e a))))

(deftest get-eatvc-values-test
  (is (= #{"2 frend 2"
           "2 frend 1"}
         (get-eatvc-values (sorted-set [1 :friend 1 :add "1 frend 1"]
                                       [2 :friend 1 :add "2 frend 1"]
                                       [2 :friend 2 :add "2 frend 2"]
                                       [3 :friend 2 :add "2 frend 2"])
                           2
                           :friend))
      "get multivalue")

  
  (is (= #{"1 frend 2"}
         (get-eatvc-values (sorted-set [1 :friend 1 :add "1 frend 1"]
                                       [1 :friend 2 :add  "1 frend 2"]
                                       [1 :friend 3 :retract "1 frend 1"])
                           1
                           :friend))
      "retract")

  (is (= #{"1 frend 1"}
         (get-eatvc-values (sorted-set [1 :friend 1 :add "1 frend 1"]
                                       [1 :friend 2 :add "1 frend 2"]
                                       [1 :friend 3 :set "1 frend 1"])
                           1
                           :friend))
      "set"))

(defn create []
  {:eatvcs {}
   :transactions {}
   :transaction-children {}
   :next-branch-number 0
   :branches {}})


(defn add-transaction-number-to-eavc [transaction-number [e a v c]]
  [e
   a
   transaction-number
   v
   c])

(defn transaction [parents & statements]
  {:parents parents
   :statements statements})

(defn add-child [child db parent]
  (update-in db [:transaction-children parent] (fnil conj #{}) child))

(deftest add-child-test
  (is (= {:transaction-children {:parent #{:child}}}
         (add-child :child
                    {:transaction-children {}}
                    :parent))))

(defn update-values [m f & args]
  (reduce (fn [r [k v]]
            (assoc r k (apply f v args)))
          {}
          m))

(defn update-keys [m f & args]
  (into {} 
        (for [[k v] m] 
          [(apply f k args) v])))

(defn transaction-map-to-graph [transaction-map]
  (update-values transaction-map
                 :parents))

#_(deftest test-transaction-map-to-graph
    (is (= {1 nil, 2 [1], 3 [1], 4 [2 3]}
           (transaction-map-to-graph {1 {}
                                      2 {:parents [1]}
                                      3 {:parents [1]}
                                      4 {:parents [2 3]}}))))

(defn transactions-to-transaction-map [transactions]
  (reduce (fn [transaction-map transaction]
            (assoc transaction-map
                   (:id transaction)
                   (dissoc transaction :id)))
          {}
          transactions))

(deftest test-transactions-to-transaction-map
  (is (= {2 {:parents [1]}
          3 {:parents [1]}
          4 {:parents [2 3]}}
         (transactions-to-transaction-map [{:parents [1]
                                            :id 2}
                                           {:parents [1]
                                            :id 3}
                                           {:parents [2 3]
                                            :id 4}]))))



(defn transactions-to-graph-and-hashes [transactions]
  (let [transaction-map (transactions-to-transaction-map transactions)
        transaction-ids (map :id transactions)]
    (loop [transaction-ids transaction-ids
           hashes {}
           graph {}]
      (if-let [transaction-id (first transaction-ids)]
        (let [transaction (update (get transaction-map
                                       transaction-id)
                                  :parents
                                  (partial map hashes))
              hash (transaction-hash transaction)]
          (recur (rest transaction-ids)
                 (assoc hashes
                        transaction-id
                        hash)
                 (assoc graph
                        hash
                        transaction)))
        {:graph graph
         :hashes hashes}))))


(defn temporal-ids-to-hashes [transactions]
  (let [{:keys [graph hashes]} (transactions-to-graph-and-hashes transactions)]
    (map graph (map hashes (map :id transactions)))))

(deftest test-temporal-ids-to-hashes
  (is (= '({:statements [[1 :friend :add "1 frend 1"]], :parents ()}
           {:parents
            ("46964D358C3EA11FAF5E3CB8FBBBABBEF86ADBFCB01103FBF7DAAE40C542E1D0"),
            :statements [[1 :friend :add "1 frend 2"]]}
           {:parents
            ("46964D358C3EA11FAF5E3CB8FBBBABBEF86ADBFCB01103FBF7DAAE40C542E1D0"),
            :statements [[1 :friend :add "1 frend 3"]]}
           {:parents
            ("3265B41A369902AD6FB1E0861AE026F0370EB52D354753944A4942FEAA8BF3E9"),
            :statements [[1 :friend :add "1 frend 4"]]}
           {:parents
            ("46964D358C3EA11FAF5E3CB8FBBBABBEF86ADBFCB01103FBF7DAAE40C542E1D0"
             "3265B41A369902AD6FB1E0861AE026F0370EB52D354753944A4942FEAA8BF3E9")})
         (temporal-ids-to-hashes [{:statements [[1 :friend :add "1 frend 1"]]
                                   :id 1}
                                  {:parents [1]
                                   :statements [[1 :friend :add "1 frend 2"]]
                                   :id 2}
                                  {:parents [1]
                                   :statements [[1 :friend :add "1 frend 3"]]
                                   :id 3}
                                  {:parents [3]
                                   :statements [[1 :friend :add "1 frend 4"]]
                                   :id 4}
                                  {:parents [1 3]
                                   :id 5}]))))

(defn transaction-number [transaction-hash db]
  (get-in db [:branches
              (get-in db [:transactions
                          transaction-hash
                          :branch-number])
              :transaction-numbers
              transaction-hash]))



(comment
  (transactions-to-graph-and-hashes [{:statements [[1 :friend :add "1 frend 1"]]
                                      :id 1}
                                     {:parents [1]
                                      :statements [[1 :friend :add "1 frend 2"]]
                                      :id 2}
                                     {:parents [1]
                                      :statements [[1 :friend :add "1 frend 3"]]
                                      :id 3}
                                     {:parents [3]
                                      :statements [[1 :friend :add "1 frend 4"]]
                                      :id 4}
                                     {:parents [1 3]
                                      :id 5}]))

#_(defn transactions-to-transaction-map)


(defn new-branch? [db transaction]
  (or (empty? (:parents transaction))
      (< 0 (count (get-in db [:transaction-children (first (:parents transaction))])))))

(deftest test-new-branch?
  (is (= true
         (new-branch? {:transaction-children {:parent #{:child1 :child2}}}
                      (transaction [:parent]
                                   [1 :friend :add "1 frend 1"]
                                   [2 :friend :add "2 frend 1"]))))

  (is (= false
         (new-branch? {:transaction-children {:parent #{}}}
                      (transaction [:parent]
                                   [1 :friend :add "1 frend 1"]
                                   [2 :friend :add "2 frend 1"]))))

  (is (= true
         (new-branch? {:transaction-children {:parent #{:child1 :child2}}}
                      (transaction []
                                   [1 :friend :add "1 frend 1"]
                                   [2 :friend :add "2 frend 1"])))))

#_(defn first-common-parents-for-parents [db parents]
    (map (fn [[parent-1 parent-2]]
           #_(prn [parent-1 parent-2])
           (first-common-parent db
                                parent-1
                                parent-2)) 
         (partition 2 1 parents)))

(defn new-forks [db transaction]
  (loop [parents (:parents transaction)
         forks []]
    (if-let [parent (first parents)]
      (recur (rest parents)
             (if (= 1 (count (get-in db [:transaction-children parent])))
               (conj forks parent)
               forks))
      forks)))

(defn transact [db transaction]
  (doseq [parent-hash (:parents transaction)]
    (assert (get-in db [:transactions parent-hash])
            (str "unknown parent " parent-hash)))
  
  (let [hash (transaction-hash transaction)
        
        is-new-branch (new-branch? db
                                   transaction)

        new-branch (when is-new-branch
                     (let [new-branch {:number (:next-branch-number db)
                                       :next-transaction-number 1
                                       :transaction-numbers {hash 0}
                                       :merges []}]
                       (if-let [first-parent-transaction-hash  (first (:parents transaction))]
                         (assoc new-branch
                                :parent-transaction-number (get-in db [:branches (:branch-number ((:transactions db) first-parent-transaction-hash)) :transaction-numbers first-parent-transaction-hash])
                                :parent-branch-number (get-in db [:transactions first-parent-transaction-hash :branch-number]))
                         new-branch)))
        
        
        branch-number (if is-new-branch
                        (:next-branch-number db)
                        (:branch-number ((:transactions db) (first (:parents transaction)))))

        transaction-number (if is-new-branch
                             0
                             (:next-transaction-number (get (:branches db) branch-number)))
        
        transaction-metadata (assoc (select-keys transaction [:parents])
                                    :branch-number branch-number
                                    :transaction-number transaction-number
                                    ;; :first-common-parents  (first-common-parents-for-parents db (:parents transaction))
                                    )]
    (-> db
        (update-in [:eatvcs branch-number]  (fnil (fn [eatvc]
                                                    (apply conj
                                                           eatvc
                                                           (map (partial add-transaction-number-to-eavc
                                                                         transaction-number)
                                                                (:statements transaction))))
                                                  (sorted-set)))
        (cond-> is-new-branch
          (-> (update :next-branch-number inc)
              (assoc-in [:branches (:number new-branch)] new-branch)))
        
        (cond-> (not is-new-branch)
          (-> (update-in [:branches branch-number :next-transaction-number] inc)
              (assoc-in [:branches branch-number :transaction-numbers hash] transaction-number)))

        (cond-> (< 1 (count (:parents transaction)))
          (-> (update-in [:branches branch-number :merges] conj transaction-number)))

        (update :transactions assoc hash transaction-metadata)
        
        (cond-> (first (:parents transaction))
          (update-in [:transaction-children (first (:parents transaction))] (fnil conj #{}) hash)))))



(defn add-other-branch-node-parents [branch-graph db]
  (loop [branch-graph branch-graph
         transaction-hashes (keys (:transactions db))]
    (if-let [transaction-hash (first transaction-hashes)]
      (recur (reduce (fn [branch-graph parent-transaction-hash]
                       (update branch-graph
                               {:transaction-number (transaction-number transaction-hash db)
                                :branch-number (get-in db [:transactions transaction-hash :branch-number])}
                               conj
                               {:transaction-number (transaction-number parent-transaction-hash db)
                                :branch-number (get-in db [:transactions parent-transaction-hash :branch-number])}))
                     branch-graph
                     (rest (get-in db [:transactions transaction-hash :parents])))
             (rest transaction-hashes))
      branch-graph)))

(defn add-transaction-hash [db branch-node]
  (assoc branch-node
         :transaction-hash ((set/map-invert (get-in db [:branches (:branch-number branch-node) :transaction-numbers]))
                            (:transaction-number branch-node))))

(defn add-temporal-id [temporal-ids branch-node]
  (assoc branch-node
         :transaction-temporal-id (temporal-ids (:transaction-hash branch-node))))

(defn branch-node-label [{:keys [branch-number transaction-number transaction-temporal-id]}]
  (str  branch-number ":" transaction-number ":" transaction-temporal-id))

(defn branch-node-to-label [db temporal-ids branch-node]
  (->> branch-node
       (add-transaction-hash db)
       (add-temporal-id temporal-ids)
       (branch-node-label)))


(defn branch-nodes [branch-number last-transaction-number]
  (map (partial assoc {:branch-number branch-number} :transaction-number)
       (reverse (range (inc last-transaction-number)))))

(deftest test-branch-nodes
  (is (= '({:branch-number 3, :transaction-number 4}
           {:branch-number 3, :transaction-number 3}
           {:branch-number 3, :transaction-number 2}
           {:branch-number 3, :transaction-number 1}
           {:branch-number 3, :transaction-number 0})
         (branch-nodes 3 4))))

(defn branches-to-graph [branches]
  (loop [branch-numbers (keys branches)
         graph {}]
    (if-let [branch-number (first branch-numbers)]
      (let [branch (branches branch-number)]
        (recur (rest branch-numbers)
               (reduce (fn [graph branch-node]
                         (assoc graph branch-node
                                (if (and (= 0 (:transaction-number branch-node))
                                         (:parent-branch-number branch))
                                  [{:branch-number (:parent-branch-number branch)
                                    :transaction-number (:parent-transaction-number branch)}]
                                  (if (< 0 (:transaction-number branch-node))
                                    [{:branch-number branch-number
                                      :transaction-number (dec (:transaction-number branch-node))}]
                                    []))))
                       graph
                       (branch-nodes branch-number
                                     (dec (get-in branches [branch-number :next-transaction-number]))))))
      graph)))

(defn view-branches [transactions]
  (let [{:keys [hashes]} (transactions-to-graph-and-hashes transactions)
        temporal-ids (set/map-invert hashes)
        db (reduce transact
                   (create)
                   (temporal-ids-to-hashes transactions))
        branch-node-to-label (partial branch-node-to-label db temporal-ids)]
    (loom-io/view (graph/digraph (-> (branches-to-graph (:branches db))
                                     (add-other-branch-node-parents db)
                                     (update-values (partial map branch-node-to-label))
                                     (update-keys branch-node-to-label))))))


(defn create-test-transactions [& graph]
  (map (fn [[id parents]]
         {:id id
          :statements [[1 :friend :add id]]
          :parents parents})
       (partition 2 graph)))

(defn view-transaction-graph [transactions]
  (loom-io/view (graph/digraph (transaction-map-to-graph (transactions-to-transaction-map transactions)))))



(defn parent-nodes [db branch-number transaction-number]
  (loop [branch-number branch-number
         parent-nodes (branch-nodes branch-number transaction-number)]
    (if-let [parent-branch-number (get-in db [:branches branch-number :parent-branch-number])]
      (recur parent-branch-number
             (concat parent-nodes (branch-nodes parent-branch-number
                                                (get-in db [:branches branch-number :parent-transaction-number]))))
      parent-nodes)))

(deftest test-parent-nodes
  (let [transactions (create-test-transactions 1 []
                                               2 [1]
                                               3 [1]
                                               4 [3]
                                               5 [3]
                                               6 [2 5]
                                               7 [5])
        db (reduce transact
                   (create)
                   (temporal-ids-to-hashes transactions))]
    (view-branches transactions)
    (is (= '({:branch-number 2, :transaction-number 1}
             {:branch-number 2, :transaction-number 0}
             {:branch-number 1, :transaction-number 0}
             {:branch-number 0, :transaction-number 0})
           (parent-nodes db 2 1)))

    (is (= '({:branch-number 0, :transaction-number 2}
             {:branch-number 0, :transaction-number 1}
             {:branch-number 0, :transaction-number 0})
           (parent-nodes db 0 2)))))

(defn parts-first-transaction-number [merges last-transaction-number]
  (or (first (drop-while (fn [merge-transaction-number]
                           (> merge-transaction-number
                              last-transaction-number))
                         merges))
      0))

(defn part-contains? [part branch-number transaction-number]
  (and (= branch-number
          (:branch-number part))
       (<= (:first-transaction-number part)
           transaction-number)
       (>= (:last-transaction-number part)
           transaction-number)))

(deftest test-part-contains?
  (let [part {:branch-number 1
              :first-transaction-number 0
              :last-transaction-number 2}]
    (is (= true (part-contains? part 1 1)))
    (is (= false (part-contains? part 0 1)))
    (is (= false (part-contains? part 1 3)))
    (is (= true (part-contains? part 1 2)))))


(defn parent-parts [db transaction-hash parts]
  (let [transaction (get-in db [:transactions transaction-hash])
        first-parent-parts-first-transaction-number (parts-first-transaction-number (get-in db [:branches
                                                                                                (:branch-number transaction)
                                                                                                :merges])
                                                                                    (:transaction-number transaction))]
    (loop [parts (concat parts
                         [{:last-transaction-number (:transaction-number transaction)
                           :first-transaction-number first-parent-parts-first-transaction-number
                           :branch-number (:branch-number transaction)}]
                         (parent-parts db)
                         )
           parents (:parents transaction)]
      (if-let [parent (first parents)]
        (let [parent-transaction (get-in db [:transactions parent])
              first-transaction-number ]
          (recur ))
        
        
        (recur (concat parts
                       (parent-parts db
                                     parent
                                     )))
        ))
    )
  
  
  (loop [branch-number branch-number
         parent-nodes (branch-nodes branch-number transaction-number)]
    (if-let [parent-branch-number (get-in db [:branches branch-number :parent-branch-number])]
      (recur parent-branch-number
             (concat parent-nodes (branch-nodes parent-branch-number
                                                (get-in db [:branches branch-number :parent-transaction-number]))))
      parent-nodes)))


#_(defn first-common [xs ys]
    (loop [seen-xs (hash-set)
           seen-ys (hash-set)
           xs xs
           ys ys]
      (if-let [x (first xs)]
        (let [seen-xs (conj seen-xs x)]
          (if-let [y (first ys)]
            (let [seen-ys (conj seen-ys y)]
              (if (or (contains? seen-xs y) 
                      (contains? seen-ys x))
                x
                (recur seen-xs
                       seen-ys
                       (rest xs)
                       (rest ys))))
            (if (contains? seen-ys x)
              x
              (recur seen-xs
                     seen-ys
                     (rest xs)
                     []))))
        (if-let [y (first ys)]
          (if (contains? seen-xs y) 
            y
            (recur seen-xs
                   seen-ys
                   []
                   (rest ys)))
          nil))))

#_(deftest test-first-common
    (is (= 1
           (first-common [0 1] [3 1])))

    (is (= 1
           (first-common [0 2 4 5 1] [3 1])))

    (is (= 1
           (first-common [3 1] [0 2 4 5 1]))))



#_(defn first-common-parent [db hash-1 hash-2]
    (let [transaction-1 (get-in db [:transactions hash-1])
          transaction-2 (get-in db [:transactions hash-2])]
      #_(:branch-number transaction-1)

      #_(prn (parent-nodes db
                           (:branch-number transaction-1)
                           (:transaction-number transaction-1)))


      #_(prn (parent-nodes db
                           (:branch-number transaction-2)
                           (:transaction-number transaction-2)))
      
      (first-common (parent-nodes db
                                  (:branch-number transaction-1)
                                  (:transaction-number transaction-1))
                    (parent-nodes db
                                  (:branch-number transaction-2)
                                  (:transaction-number transaction-2)))))




#_(deftest test-first-common-parent
    (let [transactions (create-test-transactions 1 []
                                                 2 [1]
                                                 3 [1]
                                                 4 [3]
                                                 5 [3]
                                                 6 [2 5])
          {:keys [hashes]} (transactions-to-graph-and-hashes transactions)
          db (reduce transact
                     (create)
                     (temporal-ids-to-hashes transactions))]
      #_(view-branches transactions)

      #_(is (= {:branch-number 0, :transaction-number 0}
               (first-common-parent db
                                    (hashes 2)
                                    (hashes 3))))

      #_(is (= {:branch-number 0, :transaction-number 0}
               (first-common-parent db
                                    (hashes 2)
                                    (hashes 5))))

      (is (= {:branch-number 1, :transaction-number 0}
             (first-common-parent db
                                  (hashes 4)
                                  (hashes 6))))))





(deftest test-branches-to-graph
  (is (= {{:branch-number 2, :transaction-number 0}
          [{:branch-number 1, :transaction-number 1}],
          {:branch-number 2, :transaction-number 3}
          [{:branch-number 2, :transaction-number 2}],
          {:branch-number 2, :transaction-number 2}
          [{:branch-number 2, :transaction-number 1}],
          {:branch-number 0, :transaction-number 0} [],
          {:branch-number 1, :transaction-number 4}
          [{:branch-number 1, :transaction-number 3}],
          {:branch-number 0, :transaction-number 2}
          [{:branch-number 0, :transaction-number 1}],
          {:branch-number 1, :transaction-number 2}
          [{:branch-number 1, :transaction-number 1}],
          {:branch-number 2, :transaction-number 1}
          [{:branch-number 2, :transaction-number 0}],
          {:branch-number 1, :transaction-number 3}
          [{:branch-number 1, :transaction-number 2}],
          {:branch-number 1, :transaction-number 0}
          [{:branch-number 0, :transaction-number 2}],
          {:branch-number 1, :transaction-number 1}
          [{:branch-number 1, :transaction-number 0}],
          {:branch-number 0, :transaction-number 4}
          [{:branch-number 0, :transaction-number 3}],
          {:branch-number 0, :transaction-number 3}
          [{:branch-number 0, :transaction-number 2}],
          {:branch-number 2, :transaction-number 4}
          [{:branch-number 2, :transaction-number 3}],
          {:branch-number 0, :transaction-number 1}
          [{:branch-number 0, :transaction-number 0}]}
         (branches-to-graph {0 {:next-transaction-number 5},

                             1 {:next-transaction-number 5
                                :parent-transaction-number 2
                                :parent-branch-number 0}
                             
                             2 {:next-transaction-number 5
                                :parent-transaction-number 1
                                :parent-branch-number 1}}))))













(comment
  (view-branches (create-test-transactions 1 []
                                           2 [1]
                                           3 [1]
                                           4 [3]
                                           5 [3]
                                           6 [2 5])))




(defn get-statements [db leaf-transaction e a]
  (loop [statements []
         branch (get-in db [:branches (get-in db [:transactions leaf-transaction :branch-number])])
         last-transaction-number (get-in branch [:transaction-numbers leaf-transaction])]

    (let [statements (concat (eatvc-statements (get-in db [:eatvcs (:number branch)])
                                               e
                                               a
                                               0
                                               last-transaction-number)
                             statements)]
      (if-let [parent-branch-number  (:parent-branch-number branch)]
        (recur statements
               (get-in db [:branches parent-branch-number])
               (:parent-transaction-number branch))
        statements))))


(deftest test-get-statements
  (let [transactions (create-test-transactions 1 []
                                               2 [1]
                                               3 [1]
                                               4 [2 3])
        {:keys [hashes]} (transactions-to-graph-and-hashes transactions)
        
        db (reduce transact (create)
                   (temporal-ids-to-hashes transactions))]

    #_(view-branches transactions)
    
    (is (= '([1 :friend 0 :add 1]
             [1 :friend 1 :add 2])
           (get-statements db
                           (hashes 2)
                           1 :friend)))

    (is (= '([1 :friend 0 :add 1]
             [1 :friend 0 :add 3])
           (get-statements db
                           (hashes 3)
                           1
                           :friend)))

    (is (= '([1 :friend 0 :add 1]
             [1 :friend 1 :add 2]
             [1 :friend 2 :add 4])
           (get-statements db
                           (hashes 4)
                           1
                           :friend)))))

(defn get-value [db leaf-transaction e a]
  (reduce reduce-values #{}
          (map eatvc-statement-vector-to-map
               (get-statements db leaf-transaction e a))))

(deftest test-get-value
  (let [transaction-1 (transaction []
                                   [1 :friend :add "1 frend 1"]
                                   [2 :friend :add "2 frend 1"])
        transaction-2 (transaction [(transaction-hash transaction-1)]
                                   [1 :friend :set "1 frend 2"])
        transaction-3 (transaction [(transaction-hash transaction-1)]
                                   [1 :friend :set "1 frend 3"])
        db (-> (create)
               (transact transaction-1)
               (transact transaction-2)
               (transact transaction-3)
               #_(transact (transaction [(transaction-hash transaction-2)
                                         (transaction-hash transaction-3)])))]
    
    (is (= #{"1 frend 2"}
           (get-value db
                      (transaction-hash transaction-2)
                      1
                      :friend)))

    (is (= #{"1 frend 3"}
           (get-value db
                      (transaction-hash transaction-3)
                      1
                      :friend)))))

(defn start []
  (let  [transaction-1 (transaction []
                                    [1 :friend :add "1 frend 1"]
                                    [2 :friend :add "2 frend 1"])
         transaction-2 (transaction [(transaction-hash transaction-1)]
                                    [1 :friend :set "1 frend 2"])
         transaction-3 (transaction [(transaction-hash transaction-1)]
                                    [1 :friend :set "1 frend 3"])]
    (-> (create)
        (transact transaction-1)
        (transact transaction-2)
        (transact transaction-3)
        (transact (transaction [(transaction-hash transaction-2)
                                (transaction-hash transaction-3)]))
        #_(get-value 1 :friend))))

(start)
