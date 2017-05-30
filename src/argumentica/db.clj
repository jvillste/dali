(ns argumentica.db
  (:require [datascript.core :as d]
            [datascript.db :as db]
            [clojure.uuid :as uuid])
  (:import [java.security MessageDigest]
           [java.util Base64]
           [com.google.common.io BaseEncoding]
           [java.util UUID])
  (:use clojure.test))

(deftest test-entity
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


(defn transaction-id [transaction]
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
  {:eatvc (sorted-set)
   :transactions []
   :next-transaction-number 0})


(defn transact [db & statements]
  (-> db
      (update :eatvc (fn [eatvc]
                       (apply conj
                              eatvc
                              (map (fn [[e a v c]]
                                     [e
                                      a
                                      (:next-transaction-number db)
                                      v
                                      c])
                                   statements))))
      (update :transactions conj statements)
      (update :next-transaction-number inc)))

(defn get [db e a]
  (get-eatvc-values (:eatvc db)
                    e
                    a))

(comment
  (-> (create)
      (transact [1 :friend :add "1 frend 1"]
                [2 :friend :add "2 frend 1"])
      (transact [1 :friend :set "1 frend 2"])
      (get 1 :friend))
  
  (let [eatv (sorted-set [1 :name 1 "11"]
                         [1 :name 2 "12"]
                         [2 :name 2 "21"]
                         [2 :name 3 "22"])]
    (rsubseq eatv <= [2 :name Long/MAX_VALUE nil]))
  )








