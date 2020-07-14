(ns argumentica.db.common-test
  (:require [argumentica.db.common :as common]
            (argumentica [hash-map-storage :as hash-map-storage]
                         [sorted-set-db :as sorted-set-db]
                         [btree :as btree]
                         [index :as index]
                         [comparator :as comparator]
                         [sorted-map-transaction-log :as sorted-map-transaction-log]
                         [btree-collection :as btree-collection])

            [argumentica.btree-index :as btree-index]
            [argumentica.sorted-set-index :as sorted-set-index]
            [medley.core :as medley]
            [argumentica.db.query :as query]
            [argumentica.util :as util]
            schema.test)
  (:use clojure.test))

(use-fixtures :once schema.test/validate-schemas)

(defn create-eav-db [& transactions]
  (reduce common/transact
          (common/db-from-index-definitions [common/eav-index-definition]
                                            (fn [index-key] (btree-collection/create-in-memory {:node-size 3}))
                                            (sorted-map-transaction-log/create))
          transactions))

(deftest test-transact
  (is (= '([1 :friend 2 0 :add]
           [1 :friend 2 1 :remove]
           [1 :friend 3 1 :add]
           [2 :friend 1 0 :add])
         (let [db (create-eav-db #{[1 :friend :set 2]
                                   [2 :friend :set 1]}
                                 #{[1 :friend :set 3]})]
           (util/inclusive-subsequence (-> db :indexes :eav :collection)
                                       [1 :friend nil nil nil])))))

(deftest test-datoms-from-index
  (let [db (-> (create-eav-db #{[:entity-1 :attribute-1 :set :value-1]
                                [:entity-1 :attribute-2 :set :value-2]}
                              #{[:entity-1 :attribute-1 :set :value-3]}))]

    (is (= '([:entity-1 :attribute-1 :value-1 0 :add]
             [:entity-1 :attribute-1 :value-1 1 :remove]
             [:entity-1 :attribute-1 :value-3 1 :add]
             [:entity-1 :attribute-2 :value-2 0 :add])
           (common/datoms-from-index (-> db :indexes :eav)
                                     [])))

    (is (= '([:entity-1 :attribute-1 :value-1 0 :add]
             [:entity-1 :attribute-2 :value-2 0 :add])
           (common/datoms-from-index (-> db :indexes :eav)
                                     []
                                     0)))

    (is (= '([:entity-1 :attribute-1 :value-1 0 :add]
             [:entity-1 :attribute-2 :value-2 0 :add])
           (common/datoms-from-index (-> db :indexes :eav)
                                     [:entity-1 :attribute-1]
                                     0)))

    (is (= '([:entity-1 :attribute-2 :value-2 0 :add])
           (common/datoms-from-index (-> db :indexes :eav)
                                     [:entity-1 :attribute-2]
                                     0)))))




(deftest test-matching-datoms-from-index
  (let [db (-> (create-eav-db #{[:entity-1 :attribute-1 :set :value-1]
                                [:entity-1 :attribute-2 :set :value-2]}
                              #{[:entity-1 :attribute-1 :set :value-3]}))]

    (is (= '([:entity-1 :attribute-1 :value-1 0 :add]
             [:entity-1 :attribute-1 :value-1 1 :remove]
             [:entity-1 :attribute-1 :value-3 1 :add]
             [:entity-1 :attribute-2 :value-2 0 :add])
           (common/matching-datoms-from-index (-> db :indexes :eav)
                                              [])))

    (is (= '([:entity-1 :attribute-1 :value-1 0 :add]
             [:entity-1 :attribute-2 :value-2 0 :add])
           (common/matching-datoms-from-index (-> db :indexes :eav)
                                              []
                                              0)))

    (is (= '([:entity-1 :attribute-1 :value-1 0 :add])
           (common/matching-datoms-from-index (-> db :indexes :eav)
                                              [:entity-1 :attribute-1]
                                              0)))

    (is (= '([:entity-1 :attribute-1 :value-1 0 :add]
             [:entity-1 :attribute-1 :value-1 1 :remove]
             [:entity-1 :attribute-1 :value-3 1 :add])
           (common/matching-datoms-from-index (-> db :indexes :eav)
                                              [:entity-1 :attribute-1]
                                              1)))

    (is (= '([:entity-1 :attribute-2 :value-2 0 :add])
           (common/matching-datoms-from-index (-> db :indexes :eav)
                                              [:entity-1 :attribute-2]
                                              0)))

    (is (= '([:entity-1 :attribute-2 :value-2 0 :add])
           (common/matching-datoms-from-index (-> db :indexes :eav)
                                              [:entity-1 :attribute-2]
                                              1)))))

(deftest test-values-from-eav
  (let [db (-> (create-eav-db #{[1 :friend :set 2]
                                [2 :friend :set 1]}
                              #{[1 :friend :set 3]}))]

    (is (= '(2)
           (common/values-from-eav (-> db :indexes :eav)
                                   1
                                   :friend
                                   0)))

    (is (= '(3)
           (common/values-from-eav (-> db :indexes :eav)
                                   1
                                   :friend
                                   1))))

  (let [db (-> (create-eav-db #{[:entity-1 :attribute-1 :set :value-1]
                                [:entity-1 :attribute-2 :set :value-2]}))]

    (is (= '(:value-1)
           (common/values-from-eav (-> db :indexes :eav)
                                   :entity-1
                                   :attribute-1)))

    (is (= '(:value-2)
           (common/values-from-eav (-> db :indexes :eav)
                                   :entity-1
                                   :attribute-2)))))

(defn create-in-memory-db [index-definitions]
  (common/db-from-index-definitions index-definitions
                                    (fn [index-key]
                                      (btree-collection/create-in-memory {:node-size 3}))
                                    (sorted-map-transaction-log/create)))

(defn create-db-with-composite-index [& composite-index-definition-arguments]
  (create-in-memory-db [common/eav-index-definition
                        (apply common/composite-index-definition composite-index-definition-arguments)]))

(defn datoms-from-composite-index [& transactions]
  (-> (reduce common/transact
              (create-db-with-composite-index :composite [:attribute-1 :attribute-2])
              transactions)
      (common/datoms-from :composite [])))

(deftest test-composite-index
  (is (= []
         (datoms-from-composite-index #{[:entity-1 :attribute-1 :add :value-1]})))

  (is (= []
         (datoms-from-composite-index #{[:entity-1 :attribute-2 :add :value-1]})))

  (is (= '([:value-1 :value-2 :entity-1 0 :add])
         (datoms-from-composite-index #{[:entity-1 :attribute-1 :add :value-1]
                                        [:entity-1 :attribute-2 :add :value-2]})))

  (is (= '([:value-1 :value-2 :entity-1 1 :add])
         (datoms-from-composite-index #{[:entity-1 :attribute-1 :add :value-1]}
                                      #{[:entity-1 :attribute-2 :add :value-2]})))

  (is (= '([:value-1 :value-2 :entity-1 0 :add]
           [:value-1 :value-2 :entity-1 1 :remove])
         (datoms-from-composite-index #{[:entity-1 :attribute-1 :add :value-1]
                                        [:entity-1 :attribute-2 :add :value-2]}
                                      #{[:entity-1 :attribute-2 :remove :value-2]}))))

(defn propositions-from-composite-index [last-transaction-number pattern & transactions]
  (let [db (reduce common/transact
                   (create-db-with-composite-index :composite [:attribute-1 :attribute-2])
                   transactions)]
    (common/propositions-from-index (common/index db :composite)
                                    pattern
                                    last-transaction-number)))

(deftest test-propositions-from-index
  (is (= '()
         (propositions-from-composite-index nil [] #{})))

  (is (= '([:value-1 :value-2 :entity-1])
         (propositions-from-composite-index nil []
                                            #{[:entity-1 :attribute-1 :add :value-1]
                                              [:entity-1 :attribute-2 :add :value-2]})))

  (is (= '([:value-3 :value-2 :entity-1])
         (propositions-from-composite-index nil []
                                            #{[:entity-1 :attribute-1 :add :value-1]
                                              [:entity-1 :attribute-2 :add :value-2]}
                                            #{[:entity-1 :attribute-1 :set :value-3]})))

  (is (= '([:value-1 :value-2 :entity-1])
         (propositions-from-composite-index 0 []
                                            #{[:entity-1 :attribute-1 :add :value-1]
                                              [:entity-1 :attribute-2 :add :value-2]}
                                            #{[:entity-1 :attribute-1 :set :value-3]})))

  (testing "pattern"
    (is (= '([:value-1 :value-2 :entity-1]
             [:value-2 :value-2 :entity-1])
           (propositions-from-composite-index nil []
                                              #{[:entity-1 :attribute-1 :add :value-1]
                                                [:entity-1 :attribute-1 :add :value-2]
                                                [:entity-1 :attribute-2 :add :value-2]})))

    (is (= '([:value-2 :value-2 :entity-1])
           (propositions-from-composite-index nil [:value-2]
                                              #{[:entity-1 :attribute-1 :add :value-1]
                                                [:entity-1 :attribute-1 :add :value-2]
                                                [:entity-1 :attribute-2 :add :value-2]})))))

(defn datoms-from-composite-index-with-value-function [column-definitions & transactions]
  (-> (reduce common/transact
              (create-db-with-composite-index :composite
                                              column-definitions)
              transactions)
      (common/datoms-from :composite [])))

(def composite-index-with-one-attribute-and-value-function [:attribute-1
                                                            {:attributes [:attribute-2]
                                                             :value-function seq}])

(deftest test-value-function
  (is (= '([:value-1 \f :entity-1 0 :add]
           [:value-1 \o :entity-1 0 :add])
         (datoms-from-composite-index-with-value-function composite-index-with-one-attribute-and-value-function
                                                          #{[:entity-1 :attribute-1 :add :value-1]
                                                            [:entity-1 :attribute-2 :add "foo"]})))

  (is (= '([:value-1 \f :entity-1 0 :add]
           [:value-1 \f :entity-1 1 :remove]
           [:value-1 \o :entity-1 0 :add]
           [:value-1 \o :entity-1 1 :remove])
         (datoms-from-composite-index-with-value-function composite-index-with-one-attribute-and-value-function
                                                          #{[:entity-1 :attribute-1 :add :value-1]
                                                            [:entity-1 :attribute-2 :add "foo"]}
                                                          #{[:entity-1 :attribute-1 :remove :value-1]})))

  (is (= '([:value-1 \f :entity-1 0 :add]
           [:value-1 \o :entity-1 0 :add]
           [:value-1 \z :entity-1 1 :add])
         (datoms-from-composite-index-with-value-function composite-index-with-one-attribute-and-value-function
                                                          #{[:entity-1 :attribute-1 :add :value-1]
                                                            [:entity-1 :attribute-2 :add "foo"]}
                                                          #{[:entity-1 :attribute-2 :add "fooz"]})))

  (is (= '([:value-1 \a :entity-1 1 :add]
           [:value-1 \b :entity-1 1 :add]
           [:value-1 \f :entity-1 0 :add]
           [:value-1 \f :entity-1 1 :remove]
           [:value-1 \o :entity-1 0 :add]
           [:value-1 \o :entity-1 1 :remove]
           [:value-1 \r :entity-1 1 :add])
         (datoms-from-composite-index-with-value-function composite-index-with-one-attribute-and-value-function
                                                          #{[:entity-1 :attribute-1 :add :value-1]
                                                            [:entity-1 :attribute-2 :add "foo"]}
                                                          #{[:entity-1 :attribute-2 :set "bar"]}))))

(def composite-index-with-two-attribute-column [{:attributes [:attribute-1
                                                              :attribute-2]
                                                 :value-function seq}])
(deftest test-two-attributes-in-one-column
  (is (= '([\a :entity-1 0 :add]
           [\b :entity-1 0 :add]
           [\f :entity-1 0 :add]
           [\o :entity-1 0 :add]
           [\r :entity-1 0 :add])
         (datoms-from-composite-index-with-value-function composite-index-with-two-attribute-column
                                                          #{[:entity-1 :attribute-1 :add "foo"]
                                                            [:entity-1 :attribute-2 :add "bar"]})))

  (is (= '([\f :entity-1 0 :add]
           [\o :entity-1 0 :add])
         (datoms-from-composite-index-with-value-function composite-index-with-two-attribute-column
                                                          #{[:entity-1 :attribute-1 :add "foo"]}))))



(defn datoms-from-enumeration-index [& transactions]
  (-> (reduce common/transact
              (create-in-memory-db [(common/enumeration-index-definition :enumeration
                                                                         :attribute-1)])
              transactions)
      (common/datoms-from :enumeration [])))

(deftest test-enumeration-index
  (is (= '([:value-1 0 1])
         (datoms-from-enumeration-index #{[:entity-1 :attribute-1 :add :value-1]})))

  (is (= '([:value-1 0 2])
         (datoms-from-enumeration-index #{[:entity-1 :attribute-1 :add :value-1]
                                          [:entity-2 :attribute-1 :add :value-1]})))

  (is (= '([:value-1 0 2]
           [:value-2 0 1])
         (datoms-from-enumeration-index #{[:entity-2 :attribute-1 :add :value-2]
                                          [:entity-1 :attribute-1 :add :value-1]
                                          [:entity-2 :attribute-1 :add :value-1]})))

  (is (= '([:value-1 0 2]
           [:value-1 1 1]
           [:value-1 2 0])
         (datoms-from-enumeration-index #{[:entity-1 :attribute-1 :add :value-1]
                                          [:entity-2 :attribute-1 :add :value-1]}
                                        #{[:entity-2 :attribute-1 :remove :value-1]}
                                        #{[:entity-1 :attribute-1 :remove :value-1]}))))



(defn values-from-enumeration-index [transaction-number transactions]
  (common/values-from-enumeration-index (-> (reduce common/transact
                                                    (create-in-memory-db [(common/enumeration-index-definition :enumeration
                                                                                                               :attribute-1)])
                                                    transactions)
                                            :indexes
                                            :enumeration)
                                        transaction-number))

(deftest test-values-from-enumeration-index
  (is (= '(:value-1)
         (values-from-enumeration-index 0
                                        [#{[:entity-1 :attribute-1 :add :value-1]}])))

  (is (= '(:value-1)
         (values-from-enumeration-index nil
                                        [#{[:entity-1 :attribute-1 :add :value-1]}])))

  (is (= []
         (values-from-enumeration-index 1
                                        [#{[:entity-1 :attribute-1 :add :value-1]}
                                         #{[:entity-1 :attribute-1 :remove :value-1]}])))

  (is (= [:value-1]
         (values-from-enumeration-index 0
                                        [#{[:entity-1 :attribute-1 :add :value-1]}
                                         #{[:entity-1 :attribute-1 :remove :value-1]}])))

  (is (= '(:value-1 :value-2)
         (values-from-enumeration-index 0
                                        [#{[:entity-1 :attribute-1 :add :value-1]
                                           [:entity-2 :attribute-1 :add :value-1]
                                           [:entity-2 :attribute-1 :add :value-2]}])))

  (is (= []
         (values-from-enumeration-index 0
                                        [#{[:entity-1 :attribute-2 :add :value-1]}]))))

