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
            schema.test
            [argumentica.sorted-reducible :as sorted-reducible]
            [argumentica.db.query :as query])
  (:use clojure.test))

(use-fixtures :once schema.test/validate-schemas)

(defn create-eav-db [& transactions]
  (reduce common/transact
          (common/db-from-index-definitions [common/eav-index-definition]
                                            (fn [index-key] (btree-collection/create-in-memory {:node-size 3}))
                                            (sorted-map-transaction-log/create))
          transactions))

(deftest test-transact
  (is (= [[1 :friend 2 0 :add]
          [1 :friend 2 1 :remove]
          [1 :friend 3 1 :add]
          [2 :friend 1 0 :add]]
         (let [db (create-eav-db #{[1 :friend :set 2]
                                   [2 :friend :set 1]}
                                 #{[1 :friend :set 3]})]
           (into [] (sorted-reducible/subreducible (-> db :indexes :eav :collection)))))))

(def test-datoms (sorted-set-by comparator/compare-datoms
                                [:entity-1 :attribute-1 :value-1 0 :add]
                                [:entity-1 :attribute-1 :value-1 1 :remove]
                                [:entity-1 :attribute-1 :value-3 1 :add]
                                [:entity-1 :attribute-2 :value-2 0 :add]
                                [:entity-1 :attribute-3 :value-4 0 :add]))

(deftest test-transduce-datoms-by-proposition
  (is (= [[[:entity-1 :attribute-1 :value-1 0 :add]
           [:entity-1 :attribute-1 :value-1 1 :remove]]
          [[:entity-1 :attribute-1 :value-3 1 :add]]
          [[:entity-1 :attribute-2 :value-2 0 :add]]
          [[:entity-1 :attribute-3 :value-4 0 :add]]]
         (common/transduce-datoms-by-proposition test-datoms
                                                 [:entity-1]
                                                 {:reducer conj})))

  (is (= [[[:entity-1 :attribute-1 :value-1 0 :add]]
          [[:entity-1 :attribute-2 :value-2 0 :add]]
          [[:entity-1 :attribute-3 :value-4 0 :add]]]
         (common/transduce-datoms-by-proposition test-datoms
                                                 [:entity-1]
                                                 {:reducer conj
                                                  :last-transaction-number 0})))

  (is (= [[[:entity-1 :attribute-2 :value-2 0 :add]]
          [[:entity-1 :attribute-3 :value-4 0 :add]]]
         (common/transduce-datoms-by-proposition test-datoms
                                                 [:entity-1 :attribute-2]
                                                 {:reducer conj})))

  (is (= [[[:entity-1 :attribute-1 :value-1 0 :add]]]
         (common/transduce-datoms-by-proposition test-datoms
                                                 [:entity-1]
                                                 {:reducer conj
                                                  :transducer (take 1)
                                                  :last-transaction-number 0}))))
(deftest test-transduce-propositions
  (is (= [[:entity-1 :attribute-1 :value-3]]
         (common/transduce-propositions test-datoms
                                        [:entity-1 :attribute-1]
                                        {:reducer conj})))

  (is (= [[:entity-1 :attribute-1 :value-3]
          [:entity-1 :attribute-2 :value-2]
          [:entity-1 :attribute-3 :value-4]]
         (common/transduce-propositions test-datoms
                                        [:entity-1 :attribute-1]
                                        {:reducer conj
                                         :take-while-pattern-matches? false})))

  (is (= [[:attribute-1 2 :entity-3]
          [:attribute-1 3 :entity-2]
          [:attribute-1 4 :entity-4]]
         (common/transduce-propositions (sorted-set-by comparator/compare-datoms
                                                       [:attribute-1 1 :entity-1 0 :add]
                                                       [:attribute-1 2 :entity-3 0 :add]
                                                       [:attribute-1 3 :entity-2 0 :add]
                                                       [:attribute-1 4 :entity-4 0 :add])
                                        [:attribute-1 2]
                                        {:reducer conj
                                         :take-while-pattern-matches? false}))))

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

(def test-eav-index (-> (create-eav-db #{[1 :friend :set 2]
                                         [2 :friend :set 1]}
                                       #{[1 :friend :set 3]})
                        :indexes :eav))

(deftest test-adding-same-datom-multiple-times
  (is (= [[1 :friend 2 0 :add]]
         (into [] (common/datoms-from (create-eav-db #{[1 :friend :add 2]}
                                                     #{[1 :friend :add 2]})
                                      :eav
                                      [])))))
(deftest test-values-from-eav
  (let [db (-> (create-eav-db #{[1 :friend :set 2]
                                [2 :friend :set 1]}
                              #{[1 :friend :set 3]}))]

    (is (= '(2)
           (into [] (common/values-from-eav (-> db :indexes :eav)
                                            1
                                            :friend
                                            0))))

    (is (= '(3)
           (into [] (common/values-from-eav test-eav-index
                                            1
                                            :friend
                                            1)))))

  (let [db (-> (create-eav-db #{[:entity-1 :attribute-1 :set :value-1]
                                [:entity-1 :attribute-2 :set :value-2]}))]

    (is (= '(:value-1)
           (into [] (common/values-from-eav (-> db :indexes :eav)
                                            :entity-1
                                            :attribute-1))))

    (is (= '(:value-2)
           (into [] (common/values-from-eav (-> db :indexes :eav)
                                            :entity-1
                                            :attribute-2))))))

(deftest test-transduce-values-from-eav-collection
  (is (= [:value-3 :value-3-2]
         (common/transduce-values-from-eav-collection (sorted-set-by comparator/compare-datoms
                                                                     [:entity-1 :attribute-1 :value-1 0 :add]
                                                                     [:entity-1 :attribute-1 :value-1 1 :remove]
                                                                     [:entity-1 :attribute-1 :value-3 1 :add]
                                                                     [:entity-1 :attribute-1 :value-3-2 1 :add]
                                                                     [:entity-1 :attribute-2 :value-2 0 :add]
                                                                     [:entity-1 :attribute-3 :value-4 0 :add])
                                                      :entity-1
                                                      :attribute-1
                                                      {:reducer conj})))

  (is (= [:value-1]
         (common/transduce-values-from-eav-collection (sorted-set-by comparator/compare-datoms
                                                                     [:entity-1 :attribute-1 :value-1 0 :add]
                                                                     [:entity-1 :attribute-1 :value-1 1 :remove]
                                                                     [:entity-1 :attribute-1 :value-3 1 :add]
                                                                     [:entity-1 :attribute-1 :value-3-2 1 :add]
                                                                     [:entity-1 :attribute-2 :value-2 0 :add]
                                                                     [:entity-1 :attribute-3 :value-4 0 :add])
                                                      :entity-1
                                                      :attribute-1
                                                      {:reducer conj
                                                       :last-transaction-number 0}))))

(defn create-in-memory-db [index-definitions]
  (common/db-from-index-definitions index-definitions
                                    (fn [index-key]
                                      (btree-collection/create-in-memory {:node-size 3}))
                                    (sorted-map-transaction-log/create)))

(defn create-db-with-composite-index [& composite-index-definition-arguments]
  (create-in-memory-db [common/eav-index-definition
                        (apply common/composite-index-definition composite-index-definition-arguments)]))

(defn datoms-from-composite-index [& transactions]
  (into [] (-> (reduce common/transact
                       (create-db-with-composite-index :composite [:attribute-1 :attribute-2])
                       transactions)
               (common/datoms-from :composite []))))

(deftest test-composite-index
  (is (= []
         (datoms-from-composite-index #{[:entity-1 :attribute-1 :add :value-1]})))

  (is (= []
         (datoms-from-composite-index #{[:entity-1 :attribute-2 :add :value-1]})))

  (is (= [[:value-1 :value-2 :entity-1 0 :add]]
         (datoms-from-composite-index #{[:entity-1 :attribute-1 :add :value-1]
                                        [:entity-1 :attribute-2 :add :value-2]})))

  (is (= [[:value-1 :value-2 :entity-1 0 :add]
          [:value-1 :value-3 :entity-1 0 :add]]
         (datoms-from-composite-index #{[:entity-1 :attribute-1 :add :value-1]
                                        [:entity-1 :attribute-2 :add :value-2]
                                        [:entity-1 :attribute-2 :add :value-3]})))

  (is (= [[:value-1 :value-2 :entity-1 1 :add]]
         (datoms-from-composite-index #{[:entity-1 :attribute-1 :add :value-1]}
                                      #{[:entity-1 :attribute-2 :add :value-2]})))

  (is (= [[:value-1 :value-2 :entity-1 0 :add]
          [:value-1 :value-2 :entity-1 1 :remove]]
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
  (into [] (-> (reduce common/transact
                       (create-db-with-composite-index :composite
                                                       column-definitions)
                       transactions)
               (common/datoms-from :composite []))))

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
  (into []
        (-> (reduce common/transact
                    (create-in-memory-db [(common/enumeration-index-definition :enumeration
                                                                               :attribute-1)])
                    transactions)
            :indexes
            :enumeration
            :collection
            (sorted-reducible/subreducible ::comparator/min))))

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

(defn datoms-from-rule-index [& transactions]
  (into [] (common/datoms-from (reduce common/transact
                                       (create-in-memory-db
                                        [common/eav-index-definition
                                         (common/rule-index-definition :rule
                                                                       {:head [:?category :?nutrient :?amount :?food]
                                                                        :body [[:eav
                                                                                [:?measurement :nutirent :?nutrient]
                                                                                [:?measurement :amount :?amount]
                                                                                [:?measurement :food :?food]
                                                                                [:?food :category :?category]]]})])
                                       transactions)
                               :rule
                               [])))

(deftest test-datoms-from-rule-index
  (is (= [] (datoms-from-rule-index #{[:entity-1 :attribute-1 :add :value-1]})))

  (is (= [[:pastry :nutrient-1 10 :food-1 0 :add]]
         (datoms-from-rule-index #{[:measurement-1 :nutirent :add :nutrient-1]
                                   [:measurement-1 :amount :add 10]
                                   [:measurement-1 :food :add :food-1]
                                   [:food-1 :category :add :pastry]})))

  (is (= [[:pastry :nutrient-1 10 :food-1 1 :add]]
         (datoms-from-rule-index #{[:measurement-1 :nutirent :add :nutrient-1]
                                   [:measurement-1 :amount :add 10]
                                   [:measurement-1 :food :add :food-1]}
                                 #{[:food-1 :category :add :pastry]}))))

(deftest test-proposition-collection
  (is (= '([1 :friend 2]
           [2 :friend 1])
         (util/inclusive-subsequence (common/->PropositionCollection (common/index (create-eav-db #{[1 :friend :set 2]
                                                                                                    [2 :friend :set 1]}
                                                                                                  #{[1 :friend :set 3]})
                                                                                   :eav)
                                                                     0)
                                     [1 :friend]))))

(defn run-rule-index-statements-to-datoms [transactions]
  (#'common/rule-index-statements-to-datoms {:head [:?category :?amount :?food]
                                             :body [[:eav
                                                     [:?measurement :amount :?amount]
                                                     [:?measurement :food :?food]
                                                     [:?food :category :?category]]]}
                                            (:indexes (apply create-eav-db transactions))
                                            (dec (count transactions))
                                            (last transactions)))

(deftest test-rule-index-statements-to-datoms
  (is (= '((:pastry 10 :food-1 0 :add))
         (run-rule-index-statements-to-datoms [#{[:measurement-1 :amount :add 10]
                                                 [:measurement-1 :food :add :food-1]
                                                 [:food-1 :category :add :pastry]}])))

  (is (= '((:pastry 10 :food-1 1 :add))
         (run-rule-index-statements-to-datoms [#{[:measurement-1 :amount :add 10]
                                                 [:measurement-1 :food :add :food-1]}
                                               #{[:food-1 :category :add :pastry]}])))

  (is (= '((:pastry 5 :food-1 1 :add))
         (run-rule-index-statements-to-datoms [#{[:measurement-1 :amount :add 10]
                                                 [:measurement-1 :food :add :food-1]
                                                 [:food-1 :category :add :pastry]}
                                               #{[:measurement-2 :amount :add 5]
                                                 [:measurement-2 :food :add :food-1]}])))

  (is (= '((:pastry 5 :food-1 1 :add)
           (:pastry 10 :food-1 1 :remove))
         (run-rule-index-statements-to-datoms [#{[:measurement-1 :amount :add 10]
                                                 [:measurement-1 :food :add :food-1]
                                                 [:food-1 :category :add :pastry]}
                                               #{[:measurement-1 :amount :remove 10]
                                                 [:measurement-1 :amount :add 5]}])))

  (is (= '((:beverage 5 :food-1 2 :add)
           (:beverage 10 :food-1 2 :add)
           (:pastry 10 :food-1 2 :remove)
           (:pastry 5 :food-1 2 :remove))
         (run-rule-index-statements-to-datoms [#{[:measurement-1 :amount :add 10]
                                                 [:measurement-1 :food :add :food-1]
                                                 [:food-1 :category :add :pastry]}
                                               #{[:measurement-2 :amount :add 5]
                                                 [:measurement-2 :food :add :food-1]}
                                               #{[:food-1 :category :add :beverage]
                                                 [:food-1 :category :remove :pastry]}]))))

(deftest test-dali
  (let [db (-> (common/db-from-index-definitions [{:key :eav
                                                   :statements-to-changes (fn [indexes_ transaction-number_ statements]
                                                                            (for [[e a o v] statements]
                                                                              [o e a v]))}
                                                  {:key :names
                                                   :statements-to-changes (fn [indexes_ transaction-number_ statements]
                                                                            (for [[e a o v] statements
                                                                                  :when (= a :name)]
                                                                              [o v e]))}]
                                                 (fn [index-key_] (btree-collection/create-in-memory))
                                                 (sorted-map-transaction-log/create))

               (common/transact [[:food-1 :name :add "potato"]
                                 [:food-2 :name :add "tomato"]
                                 [:nutrient-1 :name :add "salt"]])
               (common/transact [[:measurement-1 :nutrient :add :nutrient-1]
                                 [:measurement-1 :food :add :food-1]
                                 [:measurement-1 :value :add 10]])
               (common/transact [[:measurement-2 :nutrient :add :nutrient-1]
                                 [:measurement-2 :food :add :food-1]
                                 [:measurement-2 :value :add 11]]))]

    (testing "both indexes are filled with datoms resulting from the transactions"
      (is (= [["potato" :food-1 0 :add]
              ["salt" :nutrient-1 0 :add]
              ["tomato" :food-2 0 :add]]
             (into [] (common/datoms-from db :names nil))))

      (is (= [[:food-1 :name "potato" 0 :add]
              [:food-2 :name "tomato" 0 :add]
              [:measurement-1 :food :food-1 1 :add]
              [:measurement-1 :nutrient :nutrient-1 1 :add]
              [:measurement-1 :value 10 1 :add]
              [:measurement-2 :food :food-1 2 :add]
              [:measurement-2 :nutrient :nutrient-1 2 :add]
              [:measurement-2 :value 11 2 :add]
              [:nutrient-1 :name "salt" 0 :add]]
             (into [] (common/datoms-from db :eav nil)))))

    (testing "indexes are just sorted sets that can be reduced forwards or backwards starting from a given value"
      (is (= [["salt" :nutrient-1 0 :add]
              ["potato" :food-1 0 :add]]
             (into [] (sorted-reducible/subreducible (common/collection db :names)
                                                     ["salt" :comparator/max]
                                                     :backwards)))))

    (testing "To get the actual propositions we deref the database which means that we fix the latest transaction after which we want to run our query.
Transactions transacted to the database after the deref do not affect the database value obtained here."
      (is (= [[:measurement-1 :value 10]]
             (into [] (common/matching-propositions @db :eav [:measurement-1 :value]))))

      (is (= [[:measurement-1 :food :food-1]
              [:measurement-1 :nutrient :nutrient-1]
              [:measurement-1 :value 10]]
             (into [] (common/matching-propositions @db :eav [:measurement-1])))))

    (testing "We can run a logic programming style query over multiple indexes."
      (is (= [{:?food :food-1,
               :?nutrient :nutrient-1,
               :?measurement :measurement-1,
               :?value 10}
              {:?food :food-1,
               :?nutrient :nutrient-1,
               :?measurement :measurement-2,
               :?value 11}]
             (into [] (query/reducible-query nil
                                             [(common/collection db :names)
                                              ["potato" :?food]
                                              ["salt" :?nutrient]]
                                             [(common/collection db :eav)
                                              [:?measurement :food :?food]
                                              [:?measurement :nutrient :?nutrient]
                                              [:?measurement :value :?value]])))))

    (testing "The same query can be run to clojure's build in sorted sets with a special comparator."
      (is (= [{:?food :food-1,
               :?nutrient :nutrient-1,
               :?measurement :measurement-1,
               :?value 10}
              {:?food :food-1,
               :?nutrient :nutrient-1,
               :?measurement :measurement-2,
               :?value 11}]
             (into [] (query/reducible-query nil
                                             [(sorted-set-by comparator/compare-datoms
                                                             ["potato" :food-1 0 :add]
                                                             ["salt" :nutrient-1 0 :add]
                                                             ["tomato" :food-2 0 :add])
                                              ["potato" :?food]
                                              ["salt" :?nutrient]]
                                             [(sorted-set-by comparator/compare-datoms
                                                             [:food-1 :name "potato" 0 :add]
                                                             [:food-2 :name "tomato" 0 :add]
                                                             [:measurement-1 :food :food-1 1 :add]
                                                             [:measurement-1 :nutrient :nutrient-1 1 :add]
                                                             [:measurement-1 :value 10 1 :add]
                                                             [:measurement-2 :food :food-1 2 :add]
                                                             [:measurement-2 :nutrient :nutrient-1 2 :add]
                                                             [:measurement-2 :value 11 2 :add]
                                                             [:nutrient-1 :name "salt" 0 :add])
                                              [:?measurement :food :?food]
                                              [:?measurement :nutrient :?nutrient]
                                              [:?measurement :value :?value]])))))

    (testing "The Entity type implements clojure.lang.Associative for a node in the database graph so that the graph can be traversed like a hashmap structure.
 It needs a schema that tells what attributes are references to other entities."
      (is (= "salt"
             (-> (common/->Entity @db
                                  {:food {:reference? true}
                                   :nutrient {:reference? true}}
                                  :measurement-1)
                 :nutrient
                 :name))))))
