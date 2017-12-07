(ns argumentica.db
  (:require [datascript.core :as d]
            [datascript.db :as db]
            [clojure.uuid :as uuid]
            [loom.alg :as alg]
            loom.graph
            argumentica.graph
            [loom.io :as loom-io]
            [clojure.set :as set]
            (argumentica [index :as index]
                         [encode :as encode]
                         [comparator :as comparator]
                         [cryptography :as cryptography]))
  (:import [java.security MessageDigest]
           [java.util UUID])
  (:use clojure.test))


(comment 
  (-> (d/empty-db)
      (d/conn-from-db)
      (d/transact! [{:db/id 1, :age 19}
                    {:db/id 2, :age "a"}
                    {:db/id 3, :age "b"}
                    {:db/id 4, :age 2}])))

(defn uuid []
  (.toString (UUID/randomUUID)))



(defn transaction-hash [transaction]
  (encode/base-16-encode (cryptography/sha-256 (.getBytes (pr-str (select-keys transaction [:statements :parents]))
                                                          "UTF-8"))))

(defn entity [statement]
  (get statement 0))

(defn attribute [statement]
  (get statement 1))

(defn transaction-number [statement]
  (get statement 2))

(defn command [statement]
  (get statement 3))

(defn value [statement]
  (get statement 4))

(defn accumulate-values [values statement]
  (case (command statement)
    :add (conj values (value statement))
    :retract (disj values (value statement))
    :set  #{(value statement)}
    values))



(defn eatcv-statements
  ([eatcv entity-id]
   (eatcv-statements eatcv entity-id nil 0))
  
  ([eatcv entity-id a]
   (eatcv-statements eatcv entity-id a 0))

  ([eatcv entity-id a t-from]
   (take-while (fn [statement]
                 (and (= (first statement)
                         entity-id)
                      (if a
                        (= (second statement)
                           a)
                        true)))
               (index/inclusive-subsequence eatcv
                                            [entity-id a t-from nil nil])))

  ([eatcv entity-id a t-from t-to]
   (take-while (fn [statement]
                 (<= (nth statement 2)
                     t-to))
               (eatcv-statements eatcv entity-id a t-from))))

(deftest eatcv-statements-test
  (is (= '([2 :friend 1 :add "2 frend 1"] [2 :friend 2 :add "2 frend 2"])
         (eatcv-statements  (sorted-set [1 :friend 1 :add "1 frend 1"]
                                        [2 :friend 1 :add "2 frend 1"]
                                        [2 :friend 2 :add "2 frend 2"]
                                        [2 :name 2 :add "2 frend 2"]
                                        [3 :friend 2 :add "2 frend 2"])
                            2
                            :friend)))
  
  (is (= '([2 :friend 1 :add "2 frend 1"]
           [2 :friend 2 :add "2 frend 2"]
           [2 :name 2 :add "2 frend 2"])
         (eatcv-statements (sorted-set [1 :friend 1 :add "1 frend 1"]
                                       [2 :friend 1 :add "2 frend 1"]
                                       [2 :friend 2 :add "2 frend 2"]
                                       [2 :name 2 :add "2 frend 2"]
                                       [3 :friend 2 :add "2 frend 2"])
                           2)))

  (is (= '([2 :friend 2 :add "2 frend 2"])
         (eatcv-statements (sorted-set [1 :friend 1 :add "1 frend 1"]
                                       [2 :friend 1 :add "2 frend 1"]
                                       [2 :friend 2 :add "2 frend 2"]
                                       [2 :name 2 :add "2 frend 2"]
                                       [3 :friend 2 :add "2 frend 2"])
                           2
                           :friend
                           2)))

  (is (= '([1 :friend 2 :add "frend 2"]
           [1 :friend 3 :add "frend 3"])
         (eatcv-statements (sorted-set [1 :friend 1 :add "frend 1"]
                                       [1 :friend 2 :add "frend 2"]
                                       [1 :friend 3 :add "frend 3"]
                                       [1 :friend 4 :add "frend 4"])
                           1
                           :friend
                           2
                           3))))



(defn get-eatcv-values [eatcv entity-id a]
  (reduce accumulate-values #{}
          (eatcv-statements eatcv entity-id a)))

(deftest get-eatcv-values-test
  (is (= #{"2 frend 2"
           "2 frend 1"}
         (get-eatcv-values (sorted-set [1 :friend 1 :add "1 frend 1"]
                                       [2 :friend 1 :add "2 frend 1"]
                                       [2 :friend 2 :add "2 frend 2"]
                                       [3 :friend 2 :add "2 frend 2"])
                           2
                           :friend))
      "get multivalue")

  
  (is (= #{"1 frend 2"}
         (get-eatcv-values (sorted-set [1 :friend 1 :add "1 frend 1"]
                                       [1 :friend 2 :add  "1 frend 2"]
                                       [1 :friend 3 :retract "1 frend 1"])
                           1
                           :friend))
      "retract")

  (is (= #{"1 frend 1"}
         (get-eatcv-values (sorted-set [1 :friend 1 :add "1 frend 1"]
                                       [1 :friend 2 :add "1 frend 2"]
                                       [1 :friend 3 :set "1 frend 1"])
                           1
                           :friend))
      "set"))

(defn create
  ([]
   (create sorted-set))
  
  ([create-index]
   {:eatcvs {}
    :create-index create-index
    :transactions {}
    :next-branch-number 0
    :branches {}}))

(defn add-transaction-number-to-eacv [transaction-number [entity-id a c v]]
  [entity-id
   a
   transaction-number
   c
   v])

(comment
  (apply str (repeat (/ 256 8) "x"))
  (apply str (repeat (/ 128 8) "x")))

(defn create-transaction [parents & statements]
  {:parents parents
   :statements statements})


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


(defn new-branch? [db transaction]
  (or (empty? (:parents transaction))
      (< 0 (count (get-in db [:transactions
                              (first (:parents transaction))
                              :children])))))

(deftest test-new-branch?
  (is (= true
         (new-branch? {:transactions {:parent {:children #{:child1 :child2}}}}
                      (create-transaction [:parent]
                                          [1 :friend :add "1 frend 1"]
                                          [2 :friend :add "2 frend 1"]))))

  (is (= false
         (new-branch? {:transactions {:parent {:children #{}}}}
                      (create-transaction [:parent]
                                          [1 :friend :add "1 frend 1"]
                                          [2 :friend :add "2 frend 1"]))))

  (is (= true
         (new-branch? {:transactions {:parent {:children #{:child1 :child2}}}}
                      (create-transaction []
                                          [1 :friend :add "1 frend 1"]
                                          [2 :friend :add "2 frend 1"])))))


(defn new-forks [db transaction]
  (loop [parents (:parents transaction)
         forks []]
    (if-let [parent (first parents)]
      (recur (rest parents)
             (if (= 1 (count (get-in db [:tansactions parent :children])))
               (conj forks parent)
               forks))
      forks)))


(defn add-child [db parent child]
  (update-in db
             [:transactions
              parent
              :children]
             (fnil conj #{}) child))

(deftest add-child-test
  (is (= {:transactions {:parent {:children #{:child}}}}
         (add-child {:transactions {}}
                    :parent
                    :child))))


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
                                       :merges #{}}]
                       (if-let [first-parent-transaction-hash  (first (:parents transaction))]
                         (assoc new-branch
                                :parent-transaction-number (get-in db
                                                                   [:transactions
                                                                    first-parent-transaction-hash
                                                                    :transaction-number])
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
        (update-in [:eatcvs branch-number] (fnil (fn [eatcv]
                                                   (apply index/add-to-index
                                                          eatcv
                                                          (map (partial add-transaction-number-to-eacv
                                                                        transaction-number)
                                                               (:statements transaction))))
                                                 (sorted-set-by comparator/cc-cmp)))
        (cond-> is-new-branch
          (-> (update :next-branch-number inc)
              (assoc-in [:branches (:number new-branch)] new-branch)))
        
        (cond-> (not is-new-branch)
          (-> (update-in [:branches branch-number :next-transaction-number] inc)))

        (cond-> (< 1 (count (:parents transaction)))
          (-> (update-in [:branches branch-number :merges] conj transaction-number)))

        (update :transactions assoc hash transaction-metadata)

        #_(update :transaction-log (fnil conj []) transaction)
        
        (cond-> (first (:parents transaction))
          (add-child (first (:parents transaction))
                     hash)))))

(defn transaction-number-for-hash [transaction-hash db]
  (get-in db [:transactions
              transaction-hash
              :transaction-number]))

(defn add-other-branch-node-parents [branch-graph db]
  (loop [branch-graph branch-graph
         transaction-hashes (keys (:transactions db))]
    (if-let [transaction-hash (first transaction-hashes)]
      (recur (reduce (fn [branch-graph parent-transaction-hash]
                       (update branch-graph
                               {:transaction-number (transaction-number-for-hash transaction-hash db)
                                :branch-number (get-in db [:transactions transaction-hash :branch-number])}
                               conj
                               {:transaction-number (transaction-number-for-hash parent-transaction-hash db)
                                :branch-number (get-in db [:transactions parent-transaction-hash :branch-number])}))
                     branch-graph
                     (rest (get-in db [:transactions transaction-hash :parents])))
             (rest transaction-hashes))
      branch-graph)))


(defn add-transaction-hash [db branch-node]
  (assoc branch-node
         :transaction-hash ((set/map-invert (update-values (:transactions db)
                                                           :transaction-number))
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
    (loom-io/view (loom.graph/digraph (-> (branches-to-graph (:branches db))
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
  (loom-io/view (loom.graph/digraph (transaction-map-to-graph (transactions-to-transaction-map transactions)))))



#_(defn parent-nodes [db branch-number transaction-number]
  (loop [branch-number branch-number
         parent-nodes (branch-nodes branch-number transaction-number)]
    (if-let [parent-branch-number (get-in db [:branches branch-number :parent-branch-number])]
      (recur parent-branch-number
             (concat parent-nodes (branch-nodes parent-branch-number
                                                (get-in db [:branches branch-number :parent-transaction-number]))))
      parent-nodes)))

#_(deftest test-parent-nodes
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
    #_(view-branches transactions)
    (is (= '({:branch-number 2, :transaction-number 1}
             {:branch-number 2, :transaction-number 0}
             {:branch-number 1, :transaction-number 0}
             {:branch-number 0, :transaction-number 0})
           (parent-nodes db 2 1)))

    (is (= '({:branch-number 0, :transaction-number 2}
             {:branch-number 0, :transaction-number 1}
             {:branch-number 0, :transaction-number 0})
           (parent-nodes db 0 2)))))

(defn create-test-db [visualize & transaction-graph-edges]
  (let [transactions (apply create-test-transactions
                            transaction-graph-edges)
        {:keys [hashes]} (transactions-to-graph-and-hashes transactions)
        
        db (reduce transact (create)
                   (temporal-ids-to-hashes transactions))]

    (if visualize
      (view-branches transactions))

    {:db db
     :hashes hashes
     :temporal-ids (set/map-invert hashes)}))

(defn parts-first-transaction-number [merges last-transaction-number]
  (or (first (drop-while (fn [merge-transaction-number]
                           (> merge-transaction-number
                              last-transaction-number))
                         merges))
      0))

(defn parts-first-transaction-hash [db last-transaction-hash]
  (let [parts-branch-number (get-in db [:transactions last-transaction-hash :branch-number])]
    (loop [transaction-hash last-transaction-hash]
      (let [{:keys [parents]} (get-in db [:transactions transaction-hash])]
        (if (and (= 1 (count parents))
                 (= (get-in db [:transactions (first parents) :branch-number])
                    parts-branch-number))
          (recur (first parents))
          transaction-hash)))))

(deftest test-parts-first-transaction-hash
  (let [{:keys [db hashes temporal-ids]} (create-test-db #_true false
                                                         1 []
                                                         2 [1]
                                                         3 [1]
                                                         4 [2 3]
                                                         5 []
                                                         6 [4 5]
                                                         7 [4])]
    (are [parts-first-temporal-id parts-last-temporal-id]
        (= parts-first-temporal-id
           (temporal-ids (parts-first-transaction-hash db
                                                       (hashes parts-last-temporal-id))))
      3 3
      1 2
      7 7
      4 4
      6 6)))

(defn part-contains? [part branch-number transaction-number]
  (and (= branch-number
          (:branch-number part))
       (<= (:first-transaction-number part)
           transaction-number)
       (>= (:last-transaction-number part)
           transaction-number)))


(defn get-transaction [db transaction-hash]
  (if-let [transaction (get-in db [:transactions transaction-hash])]
    transaction
    (loop [parents (:parents db)]
      (if-let [parent (first parents)]
        (if-let  [transaction (get-in db [:transactions transaction-hash])]
          transaction
          (recur (rest parents)))
        nil))))

(defn parent-transactions [db last-transaction-hash]
  (argumentica.graph/sort-topologically last-transaction-hash
                                        (fn [transaction-hash]
                                          (:parents (get-transaction db transaction-hash))
                                          #_(get-in db [:transactions transaction-hash :parents]))
                                        identity))

(deftest test-parent-transactions
  (let [{:keys [db hashes temporal-ids]} (create-test-db #_true false
                                                         1 []
                                                         2 [1]
                                                         3 [1]
                                                         4 [2 3]
                                                         5 []
                                                         6 [4 5])]

    (is (= '(1 2 3 4)
           (map temporal-ids (parent-transactions db
                                                  (hashes 4)))))

    (is (= '(1 2 3 4 5 6)
           (map temporal-ids (parent-transactions db
                                                  (hashes 6)))))))

(defn partition-transaction-hashes-into-parts [db transaction-hashes-in-topological-order]
  (->> (partition-by (fn [transaction-hash]
                       (get-in db [:transactions transaction-hash :branch-number]))
                     transaction-hashes-in-topological-order)
       (map (fn [transaction-hashes]
              {:branch-number (get-in db [:transactions (first transaction-hashes) :branch-number])
               :first-transaction-number (get-in db [:transactions (first transaction-hashes) :transaction-number])
               :last-transaction-number (get-in db [:transactions (last transaction-hashes) :transaction-number])}))))

(defn part-label [{:keys [first-transaction-number last-transaction-number branch-number]}]
  (str branch-number
       ":"
       first-transaction-number
       "-"
       last-transaction-number))

(deftest test-partition-transaction-hashes-in-to-parts
  (let [{:keys [db hashes]} (create-test-db #_true false
                                            1 []
                                            2 [1]
                                            3 [1]
                                            4 [2 3]
                                            5 []
                                            6 [4 5])]

    (is (= '("0:0-1" "1:0-0" "0:2-2")
           (map part-label
                (partition-transaction-hashes-into-parts db
                                                         (parent-transactions db (hashes 4))))))
    
    (is (= '("0:0-1" "1:0-0" "0:2-2" "2:0-0" "0:3-3")
           (map part-label
                (partition-transaction-hashes-into-parts db
                                                         (parent-transactions db (hashes 6)))))))

  (let [{:keys [db hashes]} (create-test-db  #_true false
                                             1 []
                                             2 [1]
                                             3 [1]
                                             4 [3 2])]

    (is (= '("0:0-0" "1:0-0" "0:1-1" "1:1-1")
           (map part-label
                (partition-transaction-hashes-into-parts db
                                                         (parent-transactions db (hashes 4))))))))


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

(defn parent-parts [db last-transaction-hash]
  (partition-transaction-hashes-into-parts db
                                           (parent-transactions db
                                                                last-transaction-hash)))

(deftest test-parent-parts
  (let [{:keys [db hashes]} (create-test-db  false #_true
                                             1 []
                                             2 [1]
                                             3 [1]
                                             4 [2 3]
                                             5 []
                                             6 [5 4]
                                             7 [4])]
    (is (= '({:branch-number 0,
              :first-transaction-number 0,
              :last-transaction-number 1}
             {:branch-number 1,
              :first-transaction-number 0,
              :last-transaction-number 0}
             {:branch-number 0,
              :first-transaction-number 2,
              :last-transaction-number 3})
           (parent-parts db
                         (hashes 7))))))

(defn get-statements [db last-transaction entity-id a]
  (loop [statements []
         parts (parent-parts db last-transaction)]
    (if-let [part (first parts)]
      (recur (concat statements
                     (eatcv-statements (get-in db [:eatcvs (:branch-number part)])
                                       entity-id
                                       a
                                       (:first-transaction-number part)
                                       (:last-transaction-number part)))
             (rest parts))
      statements)))


(deftest test-get-statements
  (let [{:keys [db hashes]} (create-test-db  false #_true
                                             1 []
                                             2 [1]
                                             3 [1]
                                             4 [2 3]
                                             5 []
                                             6 [5 4]
                                             7 [4])]
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
             [1 :friend 0 :add 3]
             [1 :friend 2 :add 4])
           (get-statements db
                           (hashes 4)
                           1
                           :friend)))

    (is (= '([1 :friend 0 :add 5]
             [1 :friend 0 :add 1]
             [1 :friend 1 :add 2]
             [1 :friend 0 :add 3]
             [1 :friend 2 :add 4]
             [1 :friend 1 :add 6])
           (get-statements db
                           (hashes 6)
                           1
                           :friend)))))

(defn with-parents [db & parents]
  (assoc db :parents parents))



(defn get-value [db transaction-hash entity-id a]
  (reduce accumulate-values #{}
          (get-statements db transaction-hash entity-id a)))

(defn get-value-from-eatcv [eatcv entity-id a]
  (reduce accumulate-values #{}
          (eatcv-statements eatcv entity-id a)))

(deftest test-get-value
  (let [transaction-1 (create-transaction []
                                          [1 :friend :add "1 frend 1"]
                                          [2 :friend :add "2 frend 1"])
        transaction-2 (create-transaction [(transaction-hash transaction-1)]
                                          [1 :friend :set "1 frend 2"])
        transaction-3 (create-transaction [(transaction-hash transaction-1)]
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
                      :friend))))

  (let [{:keys [db hashes]} (create-test-db  false #_true
                                             1 []
                                             2 [1]
                                             3 [1]
                                             4 [2 3]
                                             5 []
                                             6 [5 4]
                                             7 [4])]
    (is (= #{1 2 3 4 5 6}
           (get-value db
                      (hashes 6)
                      1
                      :friend)))

    (is (= #{1 2 3 4 5 6}
           (get-value db
                      (hashes 6)
                      1
                      :friend)))))

(defn get-reference [db reference]
  (get-in db [:references reference]))

(defn transaction-over [db parent-references statements]
  (doseq [reference-to-be-merged (rest parent-references)]
    (assert (not= nil
                  (get-reference db
                                 reference-to-be-merged))
            (str "unknown reference to be merged:" reference-to-be-merged)))
  
  (apply create-transaction
         (->> (map (partial get-reference db)
                   parent-references)
              (filter (complement nil?)))
         statements))

(defn transact-over [db first-parent-reference transaction]
  (-> db
      (transact transaction)
      (assoc-in [:references first-parent-reference]
                (transaction-hash transaction))))

(defn transact-statements-over [db parent-references & statements]
  (transact-over db
                 (first parent-references)
                 (transaction-over db
                                   parent-references
                                   statements)))

(deftest test-transact-statements-over
  (let [db (-> (create)
               (transact-statements-over [:master] [[1] :friend :add "friend 1"])
               (transact-statements-over [:master] [[1] :friend :add "friend 2"]))]
    
    (is (= #{"friend 2" "friend 1"}
           (get-value db
                      (get-reference db
                                     :master)
                      [1]
                      :friend)))
    
    (let [db (-> db
                 (transact-statements-over [:master] [[1] :friend :retract "friend 1"]))]
      
      (is (= #{"friend 2"}
             (get-value db
                        (get-reference db
                                       :master)
                        [1]
                        :friend)))
      (let [db (-> db
                   (transact-statements-over [:master] [[1] :friend :set "only friend"]))]
        
        (is (= #{"only friend"}
               (get-value db
                          (get-reference db
                                         :master)
                          [1]
                          :friend)))))))

(defn add-reference [db reference reference-or-transaction-hash]
  (if-let [old-reference (get-reference db
                                        reference-or-transaction-hash)]
    (assoc-in db
              [:references reference]
              old-reference)
    (if (get-in db [:transactions reference-or-transaction-hash])
      (assoc-in db
                [:references reference]
                reference-or-transaction-hash)
      (throw (Exception. (str "unknown reference or transaction " reference-or-transaction-hash))))))




(defn eatcv-statements-for-transaction-range [eatcv from-transaction-index to-transaction-index]
  (set/select (fn [statement]
                (and (<= from-transaction-index
                         (transaction-number statement))
                     (>= to-transaction-index
                         (transaction-number statement))))
              eatcv))

(defn squash-statements [statements]
  (sort (reduce (fn [result-statements statement]
                  (case (command statement)
                    :add (conj (set/select (fn [result-statement]
                                             (not (and (= (entity statement)
                                                          (entity result-statement))
                                                       (= (attribute statement)
                                                          (attribute result-statement))
                                                       (= (value statement)
                                                          (value result-statement))
                                                       (= :retract
                                                          (command result-statement)))))
                                           result-statements)
                               statement)
                    :retract (let [removed-statements (set/select (fn [result-statement]
                                                                    (and (= (entity statement)
                                                                            (entity result-statement))
                                                                         (= (attribute statement)
                                                                            (attribute result-statement))
                                                                         (= (value statement)
                                                                            (value result-statement))))
                                                                  result-statements)]
                               
                               (if (empty? removed-statements)
                                 (conj result-statements
                                       statement)
                                 (set/difference result-statements
                                                 removed-statements)))
                    :set  (conj (set/select (fn [result-statement]
                                              (not (and (= (entity statement)
                                                           (entity result-statement))
                                                        (= (attribute statement)
                                                           (attribute result-statement)))))
                                            result-statements)
                                statement)))
                #{}
                statements)))


(deftest test-squash-statements
  (is (= [[1 :friend 1 :add 1]]
         (squash-statements [[1 :friend 1 :add 1]])))

  (is (= []
         (squash-statements [[1 :friend 1 :add 1]
                             [1 :friend 2 :retract 1]])))

  (is (= [[1 :friend 2 :add 1]]
         (squash-statements [[1 :friend 1 :retract 1]
                             [1 :friend 2 :add 1]])))

  (is (= []
         (squash-statements [[1 :friend 1 :set 1]
                             [1 :friend 2 :retract 1]])))

  (is (= [[1 :friend 1 :retract 1]]
         (squash-statements [[1 :friend 1 :retract 1]])))


  (is (= [[1 :friend 4 :set 2]]
         (squash-statements [[1 :friend 1 :retract 1]
                             [1 :friend 2 :add 1]
                             [1 :friend 3 :add 2]
                             [1 :friend 4 :set 2]])))

  (is (= [[1 :friend 2 :add 1]]
         (squash-statements [[1 :friend 1 :retract 1]
                             [1 :friend 2 :add 1]
                             [1 :friend 3 :add 2]
                             [1 :friend 1 :retract 2]]))))

(defn statements-for-transaction-hashes [db transaction-hashes]
  (loop [statements []
         parts (partition-transaction-hashes-into-parts db
                                                        transaction-hashes)]
    (if-let [part (first parts)]
      (recur (concat statements
                     (eatcv-statements-for-transaction-range (get-in db [:eatcvs (:branch-number part)])
                                                             (:first-transaction-number part)
                                                             (:last-transaction-number part)))
             (rest parts))
      statements)))

(defn property-conflicts [our-statements their-statements]
  (let [properties (fn [statements]
                     (into #{}
                           (map (fn [statement]
                                  [(entity statement)
                                   (attribute statement)])
                                statements)))]
    (set/intersection (properties our-statements)
                      (properties their-statements))))

(defn novel-transaction-hashes [db first-common-transaction-hash our-latest-transaction-hash their-latest-transaction-hash]
  (let [common-transactions-set (into #{}
                                      (argumentica.graph/sort-topologically first-common-transaction-hash
                                                                            (fn [transaction-hash]
                                                                              (get-in db [:transactions transaction-hash :parents]))
                                                                            identity))
        non-common-parent-hashes (fn [transaction-hash]
                                   (filter (complement common-transactions-set)
                                           (get-in db [:transactions transaction-hash :parents])))]
    {:their-novel-transaction-hashes (argumentica.graph/sort-topologically their-latest-transaction-hash
                                                                           non-common-parent-hashes
                                                                           identity)
     :our-novel-transaction-hashes (argumentica.graph/sort-topologically our-latest-transaction-hash
                                                                         non-common-parent-hashes
                                                                         identity)}))

(comment
  (let [metadata (meta #'println)]
    (get (:ns metadata)
         (:nmae metadata))))


(deftype Entity [db entity-id transaction-hash]
  Object
  (toString [this]   (pr-str entity-id))
  (hashCode [this]   (hash this)) ; db?

  clojure.lang.Seqable
  (seq [this]           (seq []))

  clojure.lang.Associative
  (equiv [this other-object] (= this other-object))
  (containsKey [this attribute] (get-value db transaction-hash entity-id attribute))
  (entryAt [this a]     (some->> (get-value db transaction-hash entity-id a) (clojure.lang.MapEntry. a)))

  (empty [this]         (throw (UnsupportedOperationException.)))
  (assoc [this k v]     (throw (UnsupportedOperationException.)))
  (cons  [this [k v]]   (throw (UnsupportedOperationException.)))
  (count [this]         (throw (UnsupportedOperationException.)))

  clojure.lang.ILookup
  (valAt [this attribute] (get-value db
                                     transaction-hash
                                     entity-id
                                     attribute))
  (valAt [this attribute not-found] (or (get-value db
                                                   transaction-hash
                                                   entity-id
                                                   attribute)
                                        not-found))

  clojure.lang.IFn
  (invoke [this attribute] (get-value db
                                      transaction-hash
                                      entity-id
                                      attribute))
  (invoke [this attribute not-found] (or (get-value db
                                                    transaction-hash
                                                    entity-id
                                                    attribute)
                                         not-found)))

(defn add-statements [btree value]
  )

(defn start []

  (:db (create-test-db false #_true
                       1 []
                       2 [1]
                       3 [1]
                       4 [2 3]
                       5 []
                       6 [4 5]
                       7 [4]
                       )))

#_(start)
