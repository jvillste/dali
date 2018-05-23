(ns argumentica.db.common
  (:require [clojure.string :as string]
            [clojure.set :as set]
            [flatland.useful.map :as map]
            (argumentica [transaction-log :as transaction-log]
                         [sorted-map-transaction-log :as sorted-map-transaction-log]
                         [index :as index])
            [argumentica.db.db]
            [argumentica.comparator :as comparator]
            [argumentica.db.db :as db])
  (:use clojure.test)
  (:import clojure.lang.MapEntry))

(defn eatcv-entity [statement]
  (get statement 0))

(defn eatcv-attribute [statement]
  (get statement 1))

(defn eatcv-transaction-number [statement]
  (get statement 2))

(defn eatcv-command [statement]
  (get statement 3))

(defn eatcv-value [statement]
  (get statement 4))


(defn statement-entity [statement]
  (get statement 0))

(defn statement-attribute [statement]
  (get statement 1))

(defn statement-command [statement]
  (get statement 2))

(defn statement-value [statement]
  (get statement 3))


(defn avtec-attribute [statement]
  (get statement 0))

(defn avtec-value [statement]
  (get statement 1))

(defn avtec-transaction-number [statement]
  (get statement 2))

(defn avtec-entity [statement]
  (get statement 3))

(defn avtec-command [statement]
  (get statement 4))


(defn eatcv-to-eatcv-datoms [_db e a t c v]
  [[e a t c v]])

(defn eatcv-to-avtec-datoms [_db e a t c v]
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

(defn update-index [index db transaction-log]
  (let [new-transactions (transaction-log/subseq transaction-log
                                                 (if-let [last-indexed-transaction-number (:last-indexed-transaction-number index)]
                                                   (inc last-indexed-transaction-number)
                                                   0))]
    (doseq [[t statements] new-transactions]
      (assert (every? (fn [statement]
                        (= 4 (count statement)))
                      statements)
              "Statement must have four values")

      (doseq [[e a c v] statements]
        (doseq [datom ((:eatcv-to-datoms index)
                       db
                       e
                       a
                       t
                       c
                       v)]
          (index/add! (:index index)
                      datom))))

    (assoc index
           :last-indexed-transaction-number
           (first (last new-transactions)))))

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
                    db
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


#_(defn datoms [db]
  (index/inclusive-subsequence (-> db :indexes :eatcv :index)
                               [nil nil nil nil nil]))

(defn pattern-matches? [pattern datom]
  (every? (fn [[pattern-value datom-value]]
            (if pattern-value
              (= pattern-value
                 datom-value)
              true))
          (map vector pattern datom)))

(defn datoms [db index-key pattern]
  (take-while (partial pattern-matches? pattern)
              (db/inclusive-subsequence db index-key pattern)))

(defn eat-matches [entity-id attribute transaction-comparator latest-transaction-number]
  (fn [[e a t c v]]
    (and (= e entity-id)
         (= a attribute)
         (if latest-transaction-number
           (transaction-comparator t latest-transaction-number)
           true))))

(defn eat-datoms-from-eatcv [eatcv entity-id attribute latest-transaction-number]
  (take-while (eat-matches entity-id
                           attribute
                           <=
                           latest-transaction-number)
              (index/inclusive-subsequence eatcv
                                           [entity-id attribute 0 nil nil])))


(defn avt-matches [attribute value-predicate transaction-comparator latest-transaction-number]
  (fn [[a v t e c]]
    (and (= a attribute)
         (value-predicate v)
         (if latest-transaction-number
           (transaction-comparator t latest-transaction-number)
           true))))



(defn accumulate-entities [entities avtec-datom]
  (case (avtec-command avtec-datom)
    :add (conj entities (avtec-entity avtec-datom))
    :retract (disj entities (avtec-entity avtec-datom))
    :set  (conj entities (avtec-entity avtec-datom))
    entities))

(defn entities-from-avtec-datoms [datoms]
  (reduce accumulate-entities
          #{}
          datoms))



(defn avtec-datoms-from-avtec [avtec attribute value value-predicate latest-transaction-number]
  (take-while (avt-matches attribute
                           value-predicate
                           <=
                           latest-transaction-number)
              (index/inclusive-subsequence avtec
                                           [attribute value 0 nil nil])))

(defn entities [db attribute value]
  (entities-from-avtec-datoms (avtec-datoms-from-avtec (-> db :indexes :avtec :index)
                                                       attribute
                                                       value
                                                       (fn [other-value]
                                                         (= other-value
                                                            value))
                                                       nil)))

(defn all-avtec-datoms-from-avtec-2 [avtec attribute value value-predicate latest-transaction-number]
  (take-while (avt-matches attribute
                           value-predicate
                           <=
                           latest-transaction-number)
              (index/inclusive-subsequence avtec
                                           [attribute value 0 nil nil])))

(defn entities-2 [avtec-index attribute value value-predicate]
  (entities-from-avtec-datoms (all-avtec-datoms-from-avtec-2 avtec-index
                                                             attribute
                                                             value
                                                             value-predicate
                                                             nil)))

(defn eat-datoms-in-reverse-from-eatcv [eatcv entity-id attribute latest-transaction-number]
  (take-while (eat-matches entity-id
                           attribute
                           >=
                           latest-transaction-number)
              (index/inclusive-reverse-subsequence eatcv
                                                   [entity-id attribute latest-transaction-number nil nil])))


(defn eat-datoms [db entity-id attribute latest-transaction-number reverse?]
  (let [eat-datoms-from-eatcv (if reverse?
                                eat-datoms-in-reverse-from-eatcv
                                eat-datoms-from-eatcv)]
    (eat-datoms-from-eatcv (-> db :indexes :eatcv :index)
                           entity-id
                           attribute
                           latest-transaction-number)))

(defn values-from-eatcv [eatcv entity-id attribute latest-transaction-number]
  (values-from-eatcv-statements (eat-datoms-from-eatcv eatcv
                                                       entity-id
                                                       attribute
                                                       latest-transaction-number)))


(defn last-transaction-number [db]
  (if-let [transaction-log (:transaction-log db)]
    (transaction-log/last-transaction-number transaction-log)
    nil))

(defn values
  ([db entity-id attribute]
   (values db
           entity-id
           attribute
           (last-transaction-number db)))

  ([db entity-id attribute transaction-number]
   (values-from-eatcv (-> db :indexes :eatcv :index)
                      entity-id
                      attribute
                      transaction-number)))

(defn value
  ([db entity-id attribute]
   (first (values db
                  entity-id
                  attribute)))

  ([db entity-id attribute transaction-number]
   (first (values db
                  entity-id
                  attribute
                  transaction-number))))



(defn datom-to-eacv-statemnt [[e a t c v]]
  [e a c v])

(defn squash-statements [statements]
  (sort comparator/cc-cmp (reduce (fn [result-statements statement]
                                    (case (statement-command statement)
                                      :add (conj (set/select (fn [result-statement]
                                                               (not (and (= (statement-entity statement)
                                                                            (statement-entity result-statement))
                                                                         (= (statement-attribute statement)
                                                                            (statement-attribute result-statement))
                                                                         (= (statement-value statement)
                                                                            (statement-value result-statement))
                                                                         (= :retract
                                                                            (statement-command result-statement)))))
                                                             result-statements)
                                                 statement)
                                      :retract (let [removed-statements (set/select (fn [result-statement]
                                                                                      (and (= (statement-entity statement)
                                                                                              (statement-entity result-statement))
                                                                                           (= (statement-attribute statement)
                                                                                              (statement-attribute result-statement))
                                                                                           (= (statement-value statement)
                                                                                              (statement-value result-statement))))
                                                                                    result-statements)]

                                                 (if (empty? removed-statements)
                                                   (conj result-statements
                                                         statement)
                                                   (set/difference result-statements
                                                                   removed-statements)))
                                      :set  (conj (set/select (fn [result-statement]
                                                                (not (and (= (statement-entity statement)
                                                                             (statement-entity result-statement))
                                                                          (= (statement-attribute statement)
                                                                             (statement-attribute result-statement)))))
                                                              result-statements)
                                                  statement)))
                                  #{}
                                  statements)))


(deftest test-squash-statements
  (is (= [[1 :friend :add 1]]
         (squash-statements [[1 :friend :add 1]])))

  (is (= []
         (squash-statements [[1 :friend :add 1]
                         [1 :friend :retract 1]])))

  (is (= [[1 :friend :add 1]]
         (squash-statements [[1 :friend :retract 1]
                         [1 :friend :add 1]])))

  (is (= []
         (squash-statements [[1 :friend :set 1]
                         [1 :friend :retract 1]])))

  (is (= [[1 :friend :retract 1]]
         (squash-statements [[1 :friend :retract 1]])))


  (is (= [[1 :friend :set 2]]
         (squash-statements [[1 :friend :retract 1]
                         [1 :friend :add 1]
                         [1 :friend :add 2]
                         [1 :friend :set 2]])))

  (is (= [[1 :friend :add 1]]
         (squash-statements [[1 :friend :retract 1]
                         [1 :friend :add 1]
                         [1 :friend :add 2]
                         [1 :friend :retract 2]])))

  (is (= '([1 :friend :add 1]
           [2 :friend :add 1])
         (squash-statements [[1 :friend :add 1]
                         [2 :friend :add 1]]))))


(defn squash-transactions [transactions]
  (squash-statements (apply concat
                            transactions)))

(deftest test-squash-transactions
  (is (= '([1 :friend :set 2]
           [2 :friend :add 1])
         (squash-transactions [[[1 :friend :add 1]
                                [2 :friend :add 1]]

                               [[1 :friend :set 2]]]))))


#_(deftype Entity [indexes entity-id transaction-number]
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
(declare ->Entity)

(defn entity-value-from-values [db schema attribute values]
  (cond (and (-> schema attribute :multivalued?)
             (-> schema attribute :reference?))
        (into #{} (map (fn [value]
                        (->Entity db schema value))
                      values))

        (and (not (-> schema attribute :multivalued?))
             (-> schema attribute :reference?))
        (->Entity db schema (first values))

        (and (-> schema attribute :multivalued?)
             (not (-> schema attribute :reference?)))
        values

        (and (not (-> schema attribute :multivalued?))
             (not (-> schema attribute :reference?)))
        (first values)))

(deftest test-entity-value-from-values
  (is (= "Foo"
       (entity-value-from-values nil {} :name ["Foo"]))))

(defn entity-value [db schema entity-id attribute]
  (if (= attribute :entity/id)
    entity-id
    (entity-value-from-values db
                              schema
                              attribute
                              (values db
                                      entity-id
                                      attribute))))

(defn entity-datoms-from-eatcv [eatcv entity-id]
  (take-while (fn [[e a t c v]]
                (= e entity-id))
              (index/inclusive-subsequence eatcv
                                           [entity-id nil nil nil nil])))

(defn entity-to-sec [db schema entity-id]
  (->> (entity-datoms-from-eatcv (-> db :indexes :eatcv :index)
                                 entity-id)
       (map second)
       (into #{})
       (#(conj % :entity/id))
       (map (fn [attribute]
              [attribute (entity-value db schema entity-id attribute)]))
       (filter (fn [[attribute value]]
                 (not (nil? value))))
       (map (fn [[attribute value]]
              (MapEntry. attribute value)))))

(deftype Entity [db schema entity-id]
  Object
  (toString [this] "Entity")
  (hashCode [this] (hash entity-id))

  clojure.lang.Seqable
  (seq [this] (entity-to-sec db schema entity-id))

  clojure.lang.Associative
  (equiv [this other-object] (= entity-id (:entity/id other-object)))
  (containsKey [this attribute] (entity-value db schema entity-id attribute))
  (entryAt [this attribute]     (some->> (entity-value db schema entity-id attribute)
                                         (clojure.lang.MapEntry. attribute)))

  (empty [this]         (throw (UnsupportedOperationException.)))
  (assoc [this k v]     (throw (UnsupportedOperationException.)))
  (cons  [this [k v]]   (throw (UnsupportedOperationException.)))
  (count [this]         (throw (UnsupportedOperationException.)))

  clojure.lang.ILookup
  (valAt [this attribute] (entity-value db schema entity-id attribute))
  (valAt [this attribute not-found] (or (entity-value db schema entity-id attribute)
                                        not-found))

  clojure.lang.IFn
  (invoke [this attribute] (entity-value db schema entity-id attribute))
  (invoke [this attribute not-found] (or (entity-value db schema entity-id attribute)
                                         not-found)))

(defn entity? [value]
  (= Entity (class value)))

(deftest test-entity?
  (is (entity? (Entity. nil nil nil)))
  (is (not (entity? 1)))
  (is (not (entity? "Foo"))))

(defn entity-to-map [entity]
  (map/map-vals entity
                (fn [value]
                  (if (entity? value)
                    (:entity/id value)
                    (if (and (set? value)
                             (entity? (first value)))
                      (into #{} (map :entity/id value))
                      value)))))

(deftest test-entity-to-map
  (is (= {:name "Foo", :entity 1, :entities #{3 2}}
         (entity-to-map {:name "Foo"
                         :entity (->Entity nil nil 1)
                         :entities #{(->Entity nil nil 2)
                                     (->Entity nil nil 3)}}))))

(defmethod print-method Entity [entity ^java.io.Writer writer]
  (.write writer  (pr-str (entity-to-map entity))))


(defn index-definition-to-indexes [index-definition create-index]
  (into {}
        (map (fn [[key eatcv-to-datoms]]
               [key
                {:eatcv-to-datoms eatcv-to-datoms
                 :index (create-index (name key))}])
             index-definition)))

(deftest test-index-definition-to-indexes
  (is (= {:eatcv
          {:eatcv-to-datoms :eatcv-to-eatcv-datoms,
           :index {:index-name "eatcv"}}}
         (index-definition-to-indexes {:eatcv :eatcv-to-eatcv-datoms}
                                      (fn [index-name] {:index-name index-name})))))

(defn db-from-index-definition [index-definition create-index transaction-log]
  (update-indexes (create :indexes (index-definition-to-indexes index-definition
                                                                create-index)
                          :transaction-log transaction-log)))

(defrecord LocalDb [indexes transaction-log]
  db/DB
  (transact [this statements]
    (transact this statements))

  (inclusive-subsequence [this index-key first-datom]
    (index/inclusive-subsequence (-> this :indexes index-key :index)
                                 first-datom)))
