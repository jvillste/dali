(ns argumentica.db.common
  (:require [clojure.string :as string]
            [clojure.set :as set]
            [clojure.pprint :as pprint]
            [flatland.useful.map :as map]
            (argumentica [transaction-log :as transaction-log]
                         [sorted-map-transaction-log :as sorted-map-transaction-log]
                         [index :as index])
            [argumentica.db.db]
            [argumentica.comparator :as comparator]
            [argumentica.db.db :as db]
            [argumentica.util :as util]
            [clojure.math.combinatorics :as combinatorics]
            [argumentica.log :as log]
            [schema.core :as schema]
            [medley.core :as medley])
  (:use clojure.test)
  (:import clojure.lang.MapEntry
           java.util.UUID))

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

(defn statement-operator [statement]
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


(defn eacv-entity [statement]
  (get statement 0))

(defn eacv-attribute [statement]
  (get statement 1))

(defn eacv-command [statement]
  (get statement 2))

(defn eacv-value [statement]
  (get statement 3))


(defn eatcv-to-eatcv-datoms [_indexes e a t c v]
  [[e a t c v]])

(defn eatcv-to-avtec-datoms [_indexes e a t c v]
  [[a v t e c]])

(defn eatcv-to-avetc-datoms [_indexes e a t c v]
  [[a v e t c]])

(def eatcv-index-definition {:key :eatcv
                             :eatcv-to-datoms eatcv-to-eatcv-datoms
                             :datom-transaction-number-index 2})

(def avtec-index-definition {:key :avtec
                             :eatcv-to-datoms eatcv-to-avtec-datoms
                             :datom-transaction-number-index 2})

(def eav-index-definition {:key :eav
                           :eatcv-to-datoms (fn [_indexes e a t c v] [[e a v t c]])})

(def avetc-index-definition {:key :avetc
                             :eatcv-to-datoms eatcv-to-avetc-datoms})

(def base-index-definitions [eatcv-index-definition
                             avtec-index-definition])

(defn set-statement [entity attribute value]
  [entity attribute :set value])

(defn create [& {:keys [indexes
                        transaction-log]
                 :or {indexes {}
                      transaction-log (sorted-map-transaction-log/create)}}]
  {#_:next-transaction-number #_(if-let [last-transaction-number (transaction-log/last-transaction-number transaction-log)]
                                  (inc last-transaction-number)
                                  0)
   :indexes indexes
   :transaction-log transaction-log})

(defn add-log-entry [db eacv-statements]
  (transaction-log/add! (:transaction-log db)
                        eacv-statements)
  db)

(defn add-transaction-to-index! [index indexes transaction-number statements]
  (assert (every? (fn [statement]
                    (= 4 (count statement)))
                  statements)
          "Statement must have four values")

  (if-let [statements-to-datoms (:statements-to-datoms index)]
    (doseq [datom (statements-to-datoms indexes
                                        transaction-number
                                        statements)]
      (index/add! (:index index)
                  datom))
    (doseq [[e a c v] statements]
      (doseq [datom ((:eatcv-to-datoms index)
                     indexes
                     e
                     a
                     transaction-number
                     c
                     v)]
        (index/add! (:index index)
                    datom)))))

(defn add-transaction-to-index [index indexes transaction-number statements]
  (if (or (nil? (:last-indexed-transaction-number index))
          (< (:last-indexed-transaction-number index)
             transaction-number))
    (do (add-transaction-to-index! index indexes transaction-number statements)
        (assoc index
               :last-indexed-transaction-number
               transaction-number))
    index))

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

(defn first-unindexed-transacion-number-for-index [index]
  (if-let [last-indexed-transaction-number (:last-indexed-transaction-number index)]
    (inc last-indexed-transaction-number)
    0))

(defn first-unindexed-transacion-number-for-index-map [index-map]
  (->> (vals index-map)
       (map first-unindexed-transacion-number-for-index)
       (apply min)))

(defn first-unindexed-transacion-number [db]
  (first-unindexed-transacion-number-for-index-map (:indexes db)))

#_(defn update-index [index transaction-log]
    (reduce (fn [index [transaction-number statements]]
              (add-transaction-to-index index
                                        nil
                                        transaction-number
                                        statements))
            index
            (transaction-log/subseq transaction-log
                                    (first-unindexed-transacion-number-for-index index))))

(defn accumulate-values [values statement]
  (case (eatcv-command statement)
    :add (conj values (eatcv-value statement))
    :remove (disj values (eatcv-value statement))
    values))

(defn values-from-eatcv-datoms [statements]
  (reduce accumulate-values
          #{}
          statements))

(defn deref [db]
  (assoc db
         :last-transaction-number (transaction-log/last-transaction-number (:transaction-log db))))


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


(def datoms-starting-from-index-options {(schema/optional-key :reverse?) schema/Bool})

(util/defno datoms-starting-from-index [index pattern {:keys [reverse?] :or {reverse? false}} :- datoms-starting-from-index-options]
  (or (if reverse?
        (index/inclusive-reverse-subsequence (:index index)
                                             (util/pad (count (first (index/inclusive-subsequence (:index index) nil)))
                                                       pattern
                                                       ::comparator/max))
        (index/inclusive-subsequence (:index index)
                                     pattern))
      []))

(defn datoms-from [db index-key pattern]
  (datoms-starting-from-index (get-in db [:indexes index-key])
                              pattern))

(defn proposition [datom]
  (vec (drop-last 2 datom)))

(defn command [datom]
  (last datom))

(defn- accumulate-propositions [propositions datom]
  (case (command datom)
    :add (conj propositions (proposition datom))
    :remove (disj propositions (proposition datom))
    propositions))

(defn transaction-number [datom]
  (first (take-last 2 datom)))

(defn- take-until-transaction-number [last-transaction-numer datoms]
  (take-while (fn [datom]
                (if last-transaction-numer
                  (>= last-transaction-numer
                      (transaction-number datom))
                  true))
              datoms))

(defn reduce-propositions [datoms]
  (sort (reduce accumulate-propositions
                #{}
                datoms)))

(util/defno datoms-by-proporistion [index pattern last-transaction-number options :- datoms-starting-from-index-options]
  (->> (datoms-starting-from-index index
                                   pattern
                                   options)
       (partition-by proposition)
       (map (partial take-until-transaction-number
                     last-transaction-number))))

(defn datoms-from-index
  ([index pattern]
   (datoms-from-index index pattern nil))

  ([index pattern last-transaction-number]
   (apply concat
          (datoms-by-proporistion index
                                  pattern
                                  last-transaction-number))))

(defn take-while-pattern-matches [pattern propositions]
  (let [pattern-length (count pattern)]
    (take-while (fn [proposition]
                  (= pattern
                     (take pattern-length proposition)))
                propositions)))

(defn matching-datoms-from-index
  ([index pattern]
   (matching-datoms-from-index index pattern nil))

  ([index pattern last-transaction-number]
   (take-while-pattern-matches pattern
                               (datoms-from-index index
                                                  pattern
                                                  last-transaction-number))))

(defn index [db index-key]
  (if-let [index (get-in db [:indexes index-key])]
    index
    (throw (Exception. (str "Unknown index-key: "
                            index-key)))))

(util/defno propositions-from-index [index pattern last-transaction-number options :- datoms-starting-from-index-options]
  (mapcat reduce-propositions
          (datoms-by-proporistion index
                                  pattern
                                  last-transaction-number
                                  options)))

(def propositions-options (merge datoms-starting-from-index-options
                                 {(schema/optional-key :take-while-pattern-matches?) schema/Bool}))

(util/defno propositions [db index-key pattern options :- propositions-options]
  (let [propositions (propositions-from-index (index db index-key)
                                              pattern
                                              (:last-transaction-number db)
                                              options)]
    (if (or (:take-while-pattern-matches? options)
            true)
      (take-while-pattern-matches pattern
                                  propositions)
      propositions)))


(defn datoms [db index-key pattern]
  (matching-datoms-from-index (get-in db [:indexes index-key])
                              pattern
                              (:last-transaction-number db)))


(defn values-from-eatcv
  ([eatcv entity-id attribute]
   (values-from-eatcv-datoms (matching-datoms-from-index eatcv
                                                         [entity-id attribute])))

  ([eatcv entity-id attribute last-transaction-number]
   (values-from-eatcv-datoms (matching-datoms-from-index eatcv
                                                         [entity-id attribute]
                                                         last-transaction-number))))

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
    :remove (disj entities (avtec-entity avtec-datom))
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
                                           [attribute value 0 ::comparator/min ::comparator/min])))

(defn entities [db attribute value]
  (entities-from-avtec-datoms (avtec-datoms-from-avtec (-> db :indexes :avtec :index)
                                                       attribute
                                                       value
                                                       (fn [other-value]
                                                         (= other-value
                                                            value))
                                                       (transaction-log/last-transaction-number (:transaction-log db)))))

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

(defn last-transaction-number [db]
  (if-let [transaction-log (:transaction-log db)]
    (transaction-log/last-transaction-number transaction-log)
    nil))

(defn values-from-eatcv
  ([eatcv-index entity-id attribute]
   (values-from-eatcv-datoms (matching-datoms-from-index eatcv-index
                                                         [entity-id attribute])))

  ([eatcv-index entity-id attribute last-transaction-number]
   (values-from-eatcv-datoms (matching-datoms-from-index eatcv-index
                                                         [entity-id attribute]
                                                         last-transaction-number))))

(defn value-from-eatcv
  ([eatcv-index entity-id attribute]
   (first (values-from-eatcv eatcv-index entity-id attribute)))

  ([eatcv-index entity-id attribute last-transaction-number]
   (first (values-from-eatcv eatcv-index entity-id attribute last-transaction-number))))



(defn datom-to-eacv-statemnt [[e a t c v]]
  [e a c v])

(defn squash-statements [statements]
  (reduce (fn [result-statements statement]
            (case (statement-operator statement)
              :add (conj (set/select (fn [result-statement]
                                       (not (and (= (statement-entity statement)
                                                    (statement-entity result-statement))
                                                 (= (statement-attribute statement)
                                                    (statement-attribute result-statement))
                                                 (= (statement-value statement)
                                                    (statement-value result-statement))
                                                 (= :remove
                                                    (statement-operator result-statement)))))
                                     result-statements)
                         statement)
              :remove (let [removed-statements (set/select (fn [result-statement]
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
          statements))


(deftest test-squash-statements
  (is (= #{[1 :friend :add 1]}
         (squash-statements [[1 :friend :add 1]])))

  (is (= #{}
         (squash-statements [[1 :friend :add 1]
                             [1 :friend :remove 1]])))

  (is (= #{[1 :friend :add 1]}
         (squash-statements [[1 :friend :remove 1]
                             [1 :friend :add 1]])))

  (is (= #{}
         (squash-statements [[1 :friend :set 1]
                             [1 :friend :remove 1]])))

  (is (= #{[1 :friend :remove 1]}
         (squash-statements [[1 :friend :remove 1]])))


  (is (= #{[1 :friend :set 2]}
         (squash-statements [[1 :friend :remove 1]
                             [1 :friend :add 1]
                             [1 :friend :add 2]
                             [1 :friend :set 2]])))

  (is (= #{[1 :friend :add 1]}
         (squash-statements [[1 :friend :remove 1]
                             [1 :friend :add 1]
                             [1 :friend :add 2]
                             [1 :friend :remove 2]])))

  (is (= #{[1 :friend :add 1]
           [2 :friend :add 1]}
         (squash-statements [[1 :friend :add 1]
                             [2 :friend :add 1]]))))


(defn squash-transactions [transactions]
  (squash-statements (apply concat
                            transactions)))

(deftest test-squash-transactions
  (is (= #{[1 :friend :set 2]
           [2 :friend :add 1]}
         (squash-transactions [[[1 :friend :add 1]
                                [2 :friend :add 1]]

                               [[1 :friend :set 2]]]))))

(defn squash-transaction-log [transaction-log last-transaction-number]
  (squash-statements (mapcat second (take-while (fn [[transaction-number statements_]]
                                                  (<= transaction-number
                                                      last-transaction-number))
                                                (transaction-log/subseq transaction-log
                                                                        0)))))
(defn values-from-eav
  ([eav-index entity-id attribute]
   (values-from-eav eav-index entity-id attribute nil))

  ([eav-index entity-id attribute last-transaction-number]
   (map last
        (take-while-pattern-matches [entity-id attribute]
                                    (propositions-from-index eav-index
                                                             [entity-id attribute]
                                                             last-transaction-number)))))

(defn values [db entity-id attribute]
  (values-from-eav (index db :eav)
                   entity-id
                   attribute
                   (:last-transaction-number db)))

(defn value [db entity-id attribute]
  (first (values db
                 entity-id
                 attribute)))

(defn- remove-statements [eav-index entity attribute]
  (for [value (values-from-eav eav-index entity attribute)]
    [entity attribute :remove value]))

(defn expand-set-statements [eav-index statements]
  (into #{}
        (mapcat (fn [[e a c v]]
                  (if (= :set c)
                    (do (assert (not (nil? eav-index))
                                "EAV index is needed for using 'set' operations in statements")
                        (vec (concat [[e a :add v]]
                                     (remove-statements eav-index
                                                        e
                                                        a))))
                    [[e a c v]]))
                statements)))

(defn add-transactions-to-indexes [indexes transactions]
  (reduce (fn [indexes [transaction-number statements]]
            (map/map-vals indexes
                          (fn [index]
                            (add-transaction-to-index index
                                                      indexes
                                                      transaction-number
                                                      statements))))
          indexes
          transactions))

(defn add-transaction-to-indexes! [indexes transaction-number statements]
  (doseq [index (vals indexes)]
    (add-transaction-to-index! index
                               indexes
                               transaction-number
                               statements)))

(defn update-indexes! [indexes transaction-log]
  (add-transactions-to-indexes indexes
                               (transaction-log/subseq transaction-log
                                                       (first-unindexed-transacion-number-for-index-map indexes))))

(defn update-indexes-2! [db]
  (doseq [[transaction-number statements] (transaction-log/subseq (:transaction-log db)
                                                                  (first-unindexed-transacion-number db))]
    (doseq [index (vals (:indexes db))]
      (add-transaction-to-index! index
                                 (:indexes db)
                                 transaction-number
                                 statements)))
  db)

(defn update-indexes [db]
  (update db :indexes update-indexes! (:transaction-log db)))

(defn transact [db statements]
  (let [statements (expand-set-statements (get-in db [:indexes :eav])
                                          statements)]
    (-> db
        (add-log-entry statements)
        (update-indexes))))

(defn transact! [db statements]
  (let [statements (expand-set-statements (get-in db [:indexes :eatcv])
                                          statements)
        transaction-number (transaction-log/add! (:transaction-log db)
                                                 statements)]
    (add-transaction-to-indexes! (:indexes db)
                                 transaction-number
                                 statements)))

(defn set [db entity attribute value]
  (transact! db
             [[entity attribute :set value]]))

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
  (if (= attribute :dali/id)
    entity-id
    (entity-value-from-values db
                              schema
                              attribute
                              (values db
                                      entity-id
                                      attribute))))

(defn entity-datoms-from-eav [eatcv entity-id]
  (take-while (fn [[e a t c v]]
                (= e entity-id))
              (index/inclusive-subsequence eatcv
                                           [entity-id nil nil nil nil])))

(defn entity-attributes [db entity-id]
  (->> (propositions db
                     :eav
                     [entity-id])
       (map second)
       (into #{})
       (#(conj % :dali/id))))

(defn entity-to-sec [db schema entity-id]
  (->> (entity-attributes db entity-id)
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
  (equiv [this other-object] (= entity-id (:dali/id other-object)))
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
                    (:dali/id value)
                    (if (and (set? value)
                             (entity? (first value)))
                      (into #{} (map :dali/id value))
                      value)))))

(deftest test-entity-to-map
  (is (= {:name "Foo", :entity 1, :entities #{3 2}}
         (entity-to-map {:name "Foo"
                         :entity (->Entity nil nil 1)
                         :entities #{(->Entity nil nil 2)
                                     (->Entity nil nil 3)}}))))

(defmethod print-method Entity [entity ^java.io.Writer writer]
  (.write writer  (pr-str (entity-to-map entity))))

(defn index-to-index-definition [index]
  (select-keys index
               [:eatcv-to-datoms
                :datom-transaction-number-index
                :key]))

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

(defn index-definitions-to-indexes [create-index index-definitions]
  (assert (sequential? index-definitions))

  (into {}
        (map (fn [index-definition]
               [(:key index-definition)
                (assoc index-definition
                       :index
                       (create-index (:key index-definition)))])
             index-definitions)))

(deftest test-index-definitions-to-indexes
  (is (= {:eatcv
          {:eatcv-to-datoms :eatcv-to-eatcv-datoms,
           :key :eatcv,
           :index {:index-key :eatcv}}}
         (index-definitions-to-indexes (fn [index-key] {:index-key index-key})
                                       [{:eatcv-to-datoms :eatcv-to-eatcv-datoms :key :eatcv}]))))

(defn db-from-index-definition [index-definition create-index transaction-log]
  (update-indexes (create :indexes (index-definition-to-indexes index-definition
                                                                create-index)
                          :transaction-log transaction-log)))



(defrecord LocalDb [indexes transaction-log]
  db/WriteableDB
  (transact [this statements]
    (transact! this statements))

  db/ReadableDB
  (inclusive-subsequence [this index-key first-datom]
    (index/inclusive-subsequence (-> this :indexes index-key :index)
                                 first-datom))
  clojure.lang.IDeref
  (deref [this] (deref this)))

(defmethod print-method LocalDb [local-db ^java.io.Writer writer]
  (print-dup (into {} local-db)
             writer))

(defmethod clojure.pprint/simple-dispatch LocalDb [local-db]
  (pprint/pprint (into {} local-db)))

(defn db-from-index-definitions [index-definitions create-index transaction-log]
  (map->LocalDb (create :indexes (index-definitions-to-indexes create-index
                                                               index-definitions)
                        :transaction-log transaction-log)))


(deftype EmptyDb []
  db/WriteableDB
  (transact [this statements]
    this)

  db/ReadableDB
  (inclusive-subsequence [this index-key first-datom]
    []))

(defn tokenize [string]
  (->> (string/split string #" ")
       (map string/lower-case)))

(defn eatcv-to-full-text-avtec [tokenize indexes e a t c v]
  (if (string? v)
    (let [old-tokens (clojure.core/set (mapcat tokenize (values-from-eav (:eav indexes)
                                                                         e
                                                                         a
                                                                         (dec t))))]
      (case c

        :remove
        (for [token (set/difference old-tokens
                                    (clojure.core/set (mapcat tokenize
                                                              (values-from-eav (:eav indexes)
                                                                               e
                                                                               a
                                                                               t))))]
          [a token e t :remove])

        :add
        (for [token (set/difference (clojure.core/set (tokenize v))
                                    old-tokens)]
          [a token e t :add])))
    []))

(def full-text-index-definition {:key :full-text
                                 :eatcv-to-datoms (partial eatcv-to-full-text-avtec tokenize)
                                 :datom-transaction-number-index 2})

#_(defn- old-tokens [tokenize indexes entity attribute transaction-number]
    (clojure.core/set (mapcat tokenize (values-from-eav (:eav indexes)
                                                        entity
                                                        attribute
                                                        (dec transaction-number)))))

#_(defn- removed-tokens* [old-tokens tokenize indexes entity attribute transaction-number]
    (set/difference old-tokens
                    (clojure.core/set (mapcat tokenize
                                              (values-from-eav (:eav indexes)
                                                               entity
                                                               attribute
                                                               transaction-number)))))

#_(defn removed-tokens [tokenize indexes transaction-number [entity attribute operator value]]
    (removed-tokens* (old-tokens tokenize indexes entity attribute transaction-number)
                     tokenize indexes entity attribute transaction-number))

#_(defn- added-tokens [tokenize indexes transaction-number [entity attribute operator value]]
    (set/difference (clojure.core/set (tokenize value))
                    (old-tokens tokenize indexes entity attribute transaction-number)))

#_(defn token-operations [tokenize indexes transaction-number [entity attribute operator value]]
    (let [old-tokens (old-tokens tokenize indexes entity attribute transaction-number)]
      (case operator

        :remove
        (for [token (removed-tokens old-tokens tokenize indexes entity attribute transaction-number)]
          {:token token
           :operator :remove})

        :add
        (for [token (added-tokens tokenize value old-tokens)]
          {:token token
           :operator :add}))))

#_(defn statements-to-token-datoms [tokenize propositions indexes transaction-number statements]
    (for [[e a o v :as statement] statements
          operation (token-operations tokenize
                                      indexes
                                      transaction-number
                                      statement)
          proposition (propositions indexes
                                    operation
                                    transaction-number
                                    statement)]
      (concat proposition
              [transaction-number
               (statement-operator statement)])))

#_(defn token-index-definition [key propositions]
    {:key key
     :statements-to-datoms (partial statements-to-token-datoms
                                    tokenize
                                    propositions)})

(defn attributes-from-column-definition [column-definition]
  (if (keyword? column-definition)
    [column-definition]
    (:attributes column-definition)))

(defn value-function-from-column-definition [column-definition]
  (if-let [value-function (:value-function column-definition)]
    value-function
    (fn [value]
      [value])))

(defn composite-index-definition [key column-definitions]
  {:key key
   :statements-to-datoms (let [attributes (mapcat attributes-from-column-definition
                                                  column-definitions)
                               attributes-set (clojure.core/set attributes)]
                           (fn [indexes transaction-number statements]
                             (apply concat
                                    (for [affected-entity-id (->> statements
                                                                  (filter (fn [statement]
                                                                            (contains? attributes-set
                                                                                       (eacv-attribute statement))))
                                                                  (map eacv-entity)
                                                                  (dedupe))]

                                      (let [values (fn [transaction-number]
                                                     (for [column-definition column-definitions]
                                                       (apply concat
                                                              (for [attribute (attributes-from-column-definition column-definition)]
                                                                (mapcat (value-function-from-column-definition column-definition)
                                                                        (values-from-eav (:eav indexes)
                                                                                         affected-entity-id
                                                                                         attribute
                                                                                         transaction-number))))))
                                            old-combinations (clojure.core/set (apply combinatorics/cartesian-product (values (dec transaction-number))))
                                            new-combinations (clojure.core/set (apply combinatorics/cartesian-product (values transaction-number)))]

                                        (concat (for [combination (set/difference old-combinations
                                                                                  new-combinations)]
                                                  (vec (concat combination
                                                               [affected-entity-id
                                                                transaction-number
                                                                :remove])))

                                                (for [combination (set/difference new-combinations
                                                                                  old-combinations)]
                                                  (vec (concat combination
                                                               [affected-entity-id
                                                                transaction-number
                                                                :add])))))))))})

(defn enumeration-count [index value]
  (let [[value-from-index _transaction-number count-from-index] (first (datoms-starting-from-index index
                                                                                                   [value]
                                                                                                   {:reverse? true}))]
    (if (= value-from-index value)
      count-from-index
      0)))

(defn- grouped-statements-to-enumeration-datom [index transaction-number statements-with-same-value-and-operator]
  (let [value (statement-value (first statements-with-same-value-and-operator))]
    [value
     transaction-number
     ((if (= :add (statement-operator (first statements-with-same-value-and-operator)))
        + -)
      (enumeration-count index
                         value)
      (count statements-with-same-value-and-operator))]))

(defn enumeration-index-definition [index-key selected-attribute]
  {:key index-key
   :statements-to-datoms (fn [indexes transaction-number statements]
                           (->> statements
                                (filter (fn [statement]
                                          (= selected-attribute
                                             (statement-attribute statement))))
                                (sort-by (juxt statement-operator
                                               statement-value))
                                (partition-by (juxt statement-operator
                                                    statement-value))
                                (map (partial grouped-statements-to-enumeration-datom
                                              (get indexes index-key)
                                              transaction-number))))})
(defn new-id []
  (UUID/randomUUID))

(defn add-id [a-map]
  (assoc a-map
         :dali/id (new-id)))

(defn ^:dynamic create-id-generator []
  (fn [] (new-id)))

(defn create-test-id-generator []
  (let [ids-atom (atom (range))]
    (fn [] (let [id (first @ids-atom)]
             (swap! ids-atom rest)
             id))))

(defn- values-from-enumeration-index* [index last-transaction-number value]
  (let [latest-datom (medley/find-first (fn [datom]
                                          (or (nil? last-transaction-number)
                                              (<= (second datom)
                                                  last-transaction-number)))
                                        (take-while-pattern-matches [value]
                                                                    (datoms-starting-from-index index [value] {:reverse? true})))
        value-exists-after-last-transaction? (and latest-datom
                          (< 0 (nth latest-datom
                                    2)))
        next-value (first (first (datoms-starting-from-index index [value ::comparator/max])))]

    (cond
      (and value-exists-after-last-transaction?
           next-value)
      (lazy-seq (cons value
                      (values-from-enumeration-index* index
                                                      last-transaction-number
                                                      next-value)))
      value-exists-after-last-transaction?
      [value]

      :default
      [])))

(defn values-from-enumeration-index [index last-transaction-number]
  (if-let [first-value (first (first (datoms-starting-from-index index [])))]
    (values-from-enumeration-index* index
                                    last-transaction-number
                                    first-value)
    []))


(defn values-from-enumeration [db index-key]
  (values-from-enumeration-index (index db index-key)
                                 (:last-transaction-number db)))
