(ns argumentica.db.query-test
  (:require [argumentica.btree-collection :as btree-collection]
            [argumentica.db.query :as query]
            [argumentica.mutable-collection :as mutable-collection]
            [clojure.test :refer :all]
            [schema.core :as schema]
            schema.test))

(use-fixtures :once schema.test/validate-schemas)

(defn create-collection [& datoms]
  (let [collection (btree-collection/create-in-memory {:node-size 3})]
    (doseq [datom datoms]
      (mutable-collection/add! collection datom))
    collection))

(deftest test-datoms
  (let [collection (-> (create-collection [:entity-1 :attribute-1 :add 1]
                                          [:entity-1 :attribute-1 :add 2]
                                          [:entity-1 :attribute-1 :add 3]))]

    (is (= '([:entity-1 :attribute-1 :add 1]
             [:entity-1 :attribute-1 :add 2]
             [:entity-1 :attribute-1 :add 3])
           (query/datoms collection
                         [])))

    (is (= '([:entity-1 :attribute-1 :add 2]
             [:entity-1 :attribute-1 :add 3])
           (query/datoms collection
                         [:entity-1 :attribute-1 :add 2])))

    (is (= '([:entity-1 :attribute-1 :add 3]
             [:entity-1 :attribute-1 :add 2]
             [:entity-1 :attribute-1 :add 1])
           (query/datoms collection
                         []
                         {:reverse? true})))

    (is (= '([:entity-1 :attribute-1 :add 2]
             [:entity-1 :attribute-1 :add 1])
           (query/datoms collection
                         [:entity-1 :attribute-1 :add 2]
                         {:reverse? true})))

    (is (= '([:entity-1 :attribute-1 :add 2])
           (query/datoms collection
                         [:entity-1 nil nil 2]
                         {:reverse? true})))

    (is (= '([:entity-1 :attribute-1 :add 2])
           (query/datoms collection
                         [nil :attribute-1 nil 2])))))

(deftest test-substitutions-for-collection
  (is (= '({:b? 1}
           {:b? 2})
         (query/substitutions-for-collection (create-collection [:a 1]
                                                                [:a 2])
                                             [:a :b?]))))

(def test-query-collection (create-collection [:measurement-5 :nutirent :nutrient-1]
                                              [:measurement-1 :amount 10]
                                              [:measurement-1 :food :food-1]
                                              [:food-1 :category :pastry]

                                              [:measurement-2 :nutirent :nutrient-1]
                                              [:measurement-2 :amount 20]
                                              [:measurement-2 :food :food-2]
                                              [:food-2 :category :meat]))

(deftest test-query
  (is (= '({:measurement? :measurement-1,
            :amount? 10,
            :food? :food-1,
            :category? :pastry}

           {:measurement? :measurement-2,
            :amount? 20,
            :food? :food-2,
            :category? :meat})
         (query/query test-query-collection
                      [[:measurement? :amount :amount?]
                       [:measurement? :food :food?]
                       [:food? :category :category?]])))

  (is (= '({:category? :meat
            :measurement? :measurement-2
            :food? :food-2})
         (query/query test-query-collection
                      [[:measurement? :amount :amount?]
                       [:measurement? :food :food?]
                       [:food? :category :category?]]
                      {:substitution {:category? :meat}}))))

(deftest test-join
  (is (= '(#:v{:measurement :measurement-1, :food :food-1}
           #:v{:measurement :measurement-2, :food :food-2})
         (query/join [{:sorted-collection (create-collection [:measurement-1 :food :food-1]
                                                             [:measurement-2 :food :food-2])
                       :patterns [[:v/measurement :food :v/food]]}])))

  (is (= '(#:v{:measurement :measurement-2, :food :food-2})
         (query/join [{:sorted-collection (create-collection [:measurement-1 :food :food-1]
                                                             [:measurement-2 :food :food-2])
                       :patterns [[:v/measurement :food :v/food]]}
                      {:sorted-collection (create-collection [:measurement-1 :nutirent :nutrient-1]
                                                             [:measurement-2 :nutirent :nutrient-2])
                       :patterns [[:v/measurement :nutrient :nutrient-1]]}]))))
