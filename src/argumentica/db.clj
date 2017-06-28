(ns argumentica.db
  (:require [datascript.core :as d]
            [datascript.db :as db]
            [clojure.uuid :as uuid]
            [loom.alg :as alg]
            [loom.graph :as graph]
            [loom.io :as loom-io])
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
  (base-16-encode (sha-256 (pr-str transaction))))

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
   :next-trunk-number 0
   :trunks {}})


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
            ("DA954F2FF5E79F674DBC08FD5F1A103E8B86E6F6B2C9F3D3CE9CA3E186735200"),
            :statements [[1 :friend :add "1 frend 4"]]}
           {:parents
            ("46964D358C3EA11FAF5E3CB8FBBBABBEF86ADBFCB01103FBF7DAAE40C542E1D0"
             "DA954F2FF5E79F674DBC08FD5F1A103E8B86E6F6B2C9F3D3CE9CA3E186735200")})
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

#_(defn transactions-to-transaction-map)


(defn new-trunk? [db transaction]
  (or (empty? (:parents transaction))
      (< 0 (count (get-in db [:transaction-children (first (:parents transaction))])))))

(deftest test-new-trunk?
  (is (= true
         (new-trunk? {:transaction-children {:parent #{:child1 :child2}}}
                     (transaction [:parent]
                                  [1 :friend :add "1 frend 1"]
                                  [2 :friend :add "2 frend 1"]))))

  (is (= false
         (new-trunk? {:transaction-children {:parent #{}}}
                     (transaction [:parent]
                                  [1 :friend :add "1 frend 1"]
                                  [2 :friend :add "2 frend 1"]))))

  (is (= true
         (new-trunk? {:transaction-children {:parent #{:child1 :child2}}}
                     (transaction []
                                  [1 :friend :add "1 frend 1"]
                                  [2 :friend :add "2 frend 1"])))))

(defn transact [db transaction]
  (doseq [parent-hash (:parents transaction)]
    (assert (get-in db [:transactions parent-hash])
            (str "unknown parent " parent-hash)))
  
  (let [hash (transaction-hash transaction)
        
        is-new-trunk (new-trunk? db
                                 transaction)

        new-trunk (when is-new-trunk
                    (let [new-trunk {:number (:next-trunk-number db)
                                     :next-transaction-number 1
                                     :transaction-numbers {hash 0}}]
                      (if-let [first-parent-transaction-hash  (first (:parents transaction))]
                        (assoc new-trunk
                               :parent-transaction-number (get-in db [:trunks (:trunk-number ((:transactions db) first-parent-transaction-hash)) :transaction-numbers first-parent-transaction-hash])
                               :parent-trunk-number (get-in db [:transactions first-parent-transaction-hash :trunk-number]))
                        new-trunk)))
        
        
        trunk-number (if is-new-trunk
                       (:next-trunk-number db)
                       (:trunk-number ((:transactions db) (first (:parents transaction)))))

        transaction-number (if is-new-trunk
                             0
                             (:next-transaction-number (get (:trunks db) trunk-number)))
        
        transaction-metadata (assoc (select-keys transaction [:parents])
                                    :trunk-number trunk-number
                                    :transaction-number transaction-number)]
    (-> db
        (update-in [:eatvcs trunk-number]  (fnil (fn [eatvc]
                                                   (apply conj
                                                          eatvc
                                                          (map (partial add-transaction-number-to-eavc
                                                                        transaction-number)
                                                               (:statements transaction))))
                                                 (sorted-set)))
        (cond-> is-new-trunk
          (-> (update :next-trunk-number inc)
              (assoc-in [:trunks (:number new-trunk)] new-trunk)))
        
        (cond-> (not is-new-trunk)
          (-> (update-in [:trunks trunk-number :next-transaction-number] inc)
              (assoc-in [:trunks trunk-number :transaction-numbers hash] transaction-number)))
        
        (update :transactions assoc hash transaction-metadata)
        
        (cond-> (first (:parents transaction))
          (update-in [:transaction-children (first (:parents transaction))] (fnil conj #{}) hash)))))

(defn trunk-parents [db trunk-number]
  (loop [trunk-number trunk-number
         parent-trunk-numbers []]
    (if-let [parent-trunk-number (get-in db [:trunks trunk-number :parent-trunk-number])]
      (recur parent-trunk-number
             (conj parent-trunk-numbers {:trunk-number parent-trunk-number
                                         :transaction-number (get-in db [:trunks trunk-number :parent-transaction-number])}))
      parent-trunk-numbers)))

(defn view-transaction-graph [transactions]
  (loom-io/view (graph/digraph (transaction-map-to-graph (transactions-to-transaction-map transactions)))))

(deftest test-trunk-parents
  (is (= [{:trunk-number 1, :transaction-number 0}
          {:trunk-number 0, :transaction-number 0}]
         (trunk-parents (reduce transact
                                (create)
                                (temporal-ids-to-hashes [{:id 1}
                                                         {:id 2
                                                          :parents [1]}
                                                         {:id 3
                                                          :parents [1]}
                                                         {:id 4
                                                          :parents [3]}
                                                         {:id 5
                                                          :parents [3]}]))
                        2))))

(defn first-common-anchestor [db hash-1 hash-2]
  (let [transaction-1 (get-in db [:transactions hash-1])
        transaction-2 (get-in db [:transactions hash-1])]
    #_(:trunk-number transaction-1)
    [(trunk-parents db (:trunk-number transaction-1))
     (trunk-parents db (:trunk-number transaction-2))] ))

(deftest test-first-common-anchestor
  (let [transactions [{:id 1}
                      {:id 2
                       :parents [1]}
                      {:id 3
                       :parents [1]}
                      {:id 4
                       :parents [3]}
                      {:id 5
                       :parents [3]}]
        {:keys [hashes]} (transactions-to-graph-and-hashes transactions)
        db (reduce transact
                   (create)
                   (temporal-ids-to-hashes transactions))]
    #_(view-transaction-graph transactions)

    #_(is (= nil
             (first-common-anchestor db (hashes 5) (hashes 2))))))

(defn trunk-nodes [trunk-number next-transaction-number]
  (map (partial assoc {:trunk-number trunk-number} :transaction-number)
       (reverse (range next-transaction-number))))

(deftest test-trunk-nodes
  (is (= '({:trunk-number 3, :transaction-number 4}
           {:trunk-number 3, :transaction-number 3}
           {:trunk-number 3, :transaction-number 2}
           {:trunk-number 3, :transaction-number 1}
           {:trunk-number 3, :transaction-number 0})
         (trunk-nodes 3 5))))

(defn trunk-node-label [{:keys [trunk-number transaction-number ]}]
  (str  trunk-number ":" transaction-number))

(defn trunks-to-graph [trunks]
  (loop [trunk-numbers (keys trunks)
         graph {}]
    (if-let [trunk-number (first trunk-numbers)]
      (let [trunk (trunks trunk-number)]
        (recur (rest trunk-numbers)
               (reduce (fn [graph trunk-node]
                         (prn trunk trunk-node)
                         (assoc graph (trunk-node-label trunk-node)
                                (if (and (= 0 (:transaction-number trunk-node))
                                         (:parent-trunk-number trunk))
                                  [(trunk-node-label {:trunk-number (:parent-trunk-number trunk)
                                                      :transaction-number (:parent-transaction-number trunk)})]
                                  (if (< 0 (:transaction-number trunk-node))
                                    [(trunk-node-label {:trunk-number trunk-number
                                                        :transaction-number (dec (:transaction-number trunk-node))})]
                                    []))))
                       graph
                       (trunk-nodes trunk-number
                                    (get-in trunks [trunk-number :next-transaction-number])))))
      graph)))

(defn view-trunks [trunks]
  (loom-io/view (graph/digraph (trunks-to-graph trunks))))

(comment
  (view-trunks (:trunks (reduce transact
                                (create)
                                (temporal-ids-to-hashes [{:id 1}
                                                         {:id 2
                                                          :parents [1]}
                                                         {:id 3
                                                          :parents [1]}
                                                         {:id 4
                                                          :parents [3]}
                                                         {:id 5
                                                          :parents [3]}]))))
  )

(deftest test-trunks-to-graph
  (is (= {"1:4" ["1:3"],
          "0:3" ["0:2"],
          "2:3" ["2:2"],
          "0:2" ["0:1"],
          "2:1" ["2:0"],
          "1:3" ["1:2"],
          "2:0" ["1:1"],
          "0:1" ["0:0"],
          "0:0" [],
          "2:4" ["2:3"],
          "2:2" ["2:1"],
          "1:2" ["1:1"],
          "1:1" ["1:0"],
          "1:0" ["0:2"],
          "0:4" ["0:3"]}
         (trunks-to-graph {0 {:next-transaction-number 5},

                           1 {:next-transaction-number 5
                              :parent-transaction-number 2
                              :parent-trunk-number 0}
                           
                           2 {:next-transaction-number 5
                              :parent-transaction-number 1
                              :parent-trunk-number 1}}))))




(defn get-statements [db leaf-transaction e a]
  (loop [statements []
         trunk (get-in db [:trunks (get-in db [:transactions leaf-transaction :trunk-number])])
         last-transaction-number (get-in trunk [:transaction-numbers leaf-transaction])]

    (let [statements (concat (eatvc-statements (get-in db [:eatvcs (:number trunk)])
                                               e
                                               a
                                               0
                                               last-transaction-number)
                             statements)]
      (if-let [parent-trunk-number  (:parent-trunk-number trunk)]
        (recur statements
               (get-in db [:trunks parent-trunk-number])
               (:parent-transaction-number trunk))
        statements))))


(deftest test-get-statements
  (let [transaction-1 (transaction []
                                   [1 :friend :add "1 frend 1"]
                                   [2 :friend :add "2 frend 1"])
        transaction-2 (transaction [(transaction-hash transaction-1)]
                                   [1 :friend :set "1 frend 2"])
        transaction-3 (transaction [(transaction-hash transaction-1)]
                                   [1 :friend :set "1 frend 3"])
        transaction-4 (transaction [(transaction-hash transaction-2)
                                    (transaction-hash transaction-3)])
        db (-> (create)
               (transact transaction-1)
               (transact transaction-2)
               (transact transaction-3)
               (transact transaction-4))]
    
    (is (= '([1 :friend 0 :add "1 frend 1"]
             [1 :friend 1 :set "1 frend 2"])
           (get-statements db
                           (transaction-hash transaction-2)
                           1 :friend)))

    (is (= '([1 :friend 0 :add "1 frend 1"]
             [1 :friend 0 :set "1 frend 3"])
           (get-statements db
                           (transaction-hash transaction-3)
                           1
                           :friend)))

    #_(is (= '([1 :friend 0 :add "1 frend 1"]
               [1 :friend 0 :set "1 frend 3"])
             (get-statements db
                             (transaction-hash transaction-4)
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







