(ns argumentica.db.common
  (:require [clojure.string :as string]
            (argumentica [transaction-log :as transaction-log]
                         [sorted-map-transaction-log :as sorted-map-transaction-log]
                         [index :as index]))
  (:use clojure.test))

(defn eatcv-attribute [statement]
  (get statement 1))

(defn eatcv-transaction-number [statement]
  (get statement 2))

(defn eatcv-command [statement]
  (get statement 3))

(defn eatcv-value [statement]
  (get statement 4))

(defn eatcv-to-eatcv-datoms [e a t c v]
  [[e a t c v]])

(defn eatcv-to-avtec-datoms [e a t c v]
  [[a v t e c]])


(defn set-statement [entity attribute value]
  [entity attribute :set value])

(defn map-to-transaction [transaction-number entity-id eatcv-to-datoms a-map]
  (reduce (fn [transaction [key value]]
            (apply conj
                   transaction
                   (eatcv-to-datoms entity-id
                                    key
                                    transaction-number
                                    :set
                                    value)))
          []
          a-map))

(deftest test-map-to-transaction
  (is (= [[2 :name 1 :set "Foo"]
          [2 :age 1 :set 20]]
         (map-to-transaction 1
                             2
                             eatcv-to-eatcv-datoms
                             {:name "Foo"
                              :age 20}))))

(defn create [& {:keys [indexes
                        transaction-log]
                 :or {indexes {}
                      transaction-log (sorted-map-transaction-log/create)}}]
  {:next-transaction-number (if-let [last-transaction-number (transaction-log/last-transaction-number transaction-log)]
                              (inc last-transaction-number)
                              0)
   :indexes indexes
   :transaction-log transaction-log})

(defn add-log-entry [db eacv-statements]
  (transaction-log/add! (:transaction-log db)
                        (:next-transaction-number db)
                        eacv-statements)
  (update db
          :next-transaction-number
          inc))

(defn update-index [index transaction-log]

  (let [new-transactions (transaction-log/subseq transaction-log
                                                 (if-let [last-transaction-number (:last-transaction-number index)]
                                                   (inc last-transaction-number)
                                                   0))]
    (doseq [[t statements] new-transactions]
      (doseq [[e a c v] statements]
        (doseq [datom ((:eatcv-to-datoms index)
                       e
                       a
                       (- t)
                       c
                       v)]
          (index/add! (:index index)
                      datom))))
    
    (assoc index
           :last-transaction-number
           (:transaction-number (last new-transactions)))))

(defn apply-to-indexes [db function & arguments]
  (update db :indexes
          (fn [indexes]
            (reduce (fn [indexes index-key]
                      (apply update
                             indexes
                             index-key
                             function
                             arguments))
                    indexes
                    (keys indexes)))))

(defn update-indexes [db]
  (apply-to-indexes db
                    update-index
                    (:transaction-log db)))


(defn transact [db statements]
  (-> db
      (add-log-entry statements)
      (update-indexes)))


(defn set [db entity attribute value]
  (transact db
            [[entity attribute :set value]]))

(defn entities-by-string-value [avtec attribute pattern latest-transaction-number]
  (map (fn [datom]
         (nth datom
              3))
       (take-while (fn [[a v t e c]]
                     (and (= a attribute)
                          (string/starts-with? v pattern)
                          (<= t latest-transaction-number)))
                   (index/inclusive-subsequence avtec
                                                [attribute pattern nil nil nil]))))

(defn accumulate-values [values statement]
  (case (eatcv-command statement)
    :add (conj values (eatcv-value statement))
    :retract (disj values (eatcv-value statement))
    :set  #{(eatcv-value statement)}
    values))

(defn values-from-eatcv-statements [statements]
  (reduce accumulate-values
          #{}
          statements))

(defn take-while-and-n-more [pred n coll]
    (let [[head tail] (split-with pred coll)]
      (concat head (take n tail))))

(defn value-from-eatcv-statements-in-reverse [statements-in-reverse]
  (let [first-statement (first statements-in-reverse)]
    (if (= (eatcv-command first-statement)
           :set)
      (eatcv-value first-statement)
      nil)))

(deftest test-value-from-eatcv-statements-in-reverse
  (is (= "foo"
         (value-from-eatcv-statements-in-reverse [[:x :name 1 :set "foo"]])))

  (is (= "bar"
         (value-from-eatcv-statements-in-reverse [[:x :name 1 :set "bar"]
                                                  [:x :name 0 :set "foo"]]))))


(defn eat-matches [entity-id attribute transaction-comparator latest-transaction-number]
  (fn [[e a t c v]]
    (and (= e entity-id)
         (= a attribute)
         (transaction-comparator t latest-transaction-number))))

(defn eat-datoms-from-eatcv [eatcv entity-id attribute latest-transaction-number]
  (take-while (eat-matches entity-id
                           attribute
                           <=
                           latest-transaction-number) 
              (index/inclusive-subsequence eatcv
                                           [entity-id attribute 0 nil nil])))

(defn eat-datoms [db entity-id attribute latest-transaction-number]
  (eat-datoms-from-eatcv (-> db :indexes :eatcv :index)
                         entity-id
                         attribute
                         latest-transaction-number))

(defn eat-datoms-in-reverse-from-eatcv [eatcv entity-id attribute latest-transaction-number]
  (take-while (eat-matches entity-id
                           attribute
                           >=
                           latest-transaction-number) 
              (index/inclusive-reverse-subsequence eatcv
                                                   [entity-id attribute latest-transaction-number nil nil])))

(defn eat-datoms-in-reverse [db entity-id attribute latest-transaction-number]
  (eat-datoms-in-reverse-from-eatcv (-> db :indexes :eatcv :index)
                                    entity-id
                                    attribute
                                    latest-transaction-number))

(defn value-from-eatcv [eatcv entity-id attribute latest-transaction-number]
  (last (first (eat-datoms-from-eatcv eatcv
                                      entity-id
                                      attribute
                                      latest-transaction-number))))

(defn last-transaction-number [db]
  (transaction-log/last-transaction-number (:transaction-log db)))

(defn value
  ([db entity-id attribute]
   (value db
          entity-id
          attribute
          (last-transaction-number db)))
  
  ([db entity-id attribute transaction-number]
   (value-from-eatcv (-> db :indexes :eatcv :index)
                     entity-id
                     attribute
                     transaction-number)))

(deftype Entity [indexes entity-id transaction-number]
  Object
  (toString [this]   (pr-str entity-id))
  (hashCode [this]   (hash this))

  clojure.lang.Seqable
  (seq [this] (seq []))

  clojure.lang.Associative
  (equiv [this other-object] (= this other-object))
  (containsKey [this attribute] (value (-> indexes :eatcv :index)
                                       entity-id
                                       attribute
                                       transaction-number))
  (entryAt [this attribute]     (some->> (value (-> indexes :eatcv :index)
                                                entity-id
                                                attribute
                                                transaction-number)
                                         (clojure.lang.MapEntry. attribute)))

  (empty [this]         (throw (UnsupportedOperationException.)))
  (assoc [this k v]     (throw (UnsupportedOperationException.)))
  (cons  [this [k v]]   (throw (UnsupportedOperationException.)))
  (count [this]         (throw (UnsupportedOperationException.)))

  clojure.lang.ILookup
  (valAt [this attribute] (value (-> indexes :eatcv :index)
                                 entity-id
                                 attribute
                                 transaction-number))
  (valAt [this attribute not-found] (or (value (-> indexes :eatcv :index)
                                               entity-id
                                               attribute
                                               transaction-number)
                                        not-found))

  clojure.lang.IFn
  (invoke [this attribute] (value (-> indexes :eatcv :index)
                                  entity-id
                                  attribute
                                  transaction-number))
  (invoke [this attribute not-found] (or (value (-> indexes :eatcv :index)
                                                entity-id
                                                attribute
                                                transaction-number)
                                         not-found)))
