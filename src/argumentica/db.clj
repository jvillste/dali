(ns argumentica.db
  (:require [datascript.core :as d]
            [datascript.db :as db]
            [clojure.uuid :as uuid])
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
  (base-64-encode (sha-256 (pr-str transaction))))

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

(defn eatvc-statements [eatvc e a]
  (take-while (fn [statement]
                (and (= (first statement)
                        e)
                     (if a
                       (= (second statement)
                          a)
                       true)))
              (subseq eatvc >= [e a 0 nil nil])))

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
                           2
                           nil))))

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
                    {:parent (if (not (empty? (:parents transaction)))
                               (:trunk-number ((:transactions db) (first (:parents transaction))))
                               nil)
                     :parent-transaciton-number (if-let [parent-transaction  (first (:parents transaction))]
                                                  (get-in db [:trunks (:trunk-number ((:transactions db) parent-transaction)) :transaction-numbers parent-transaction])
                                                  nil)
                     :number (:next-trunk-number db)
                     :next-transaction-number 1
                     :transaction-numbers {hash 0}})
        
        
        trunk-number (if is-new-trunk
                       (:next-trunk-number db)
                       (:trunk-number ((:transactions db) (first (:parents transaction)))))

        transaction-number (if is-new-trunk
                             0
                             (:next-transaction-number (get (:trunks db) trunk-number)))
        
        transaction-metadata (assoc (select-keys transaction [:parents])
                                    :trunk-number trunk-number)]
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

(defn get-value [db e a]
  (get-eatvc-values (:eatvc db)
                    e
                    a))

(comment
  
  )

(defn start []
  (let [transaction-1 (transaction []
                                   [1 :friend :add "1 frend 1"]
                                   [2 :friend :add "2 frend 1"])]
    (-> (create)
        (transact transaction-1)
        (transact (transaction [(transaction-hash transaction-1)]
                               [1 :friend :set "1 frend 2"]))
        (transact (transaction [(transaction-hash transaction-1)]
                               [1 :friend :set "1 frend 3"]))
        #_(get-value 1 :friend))))







