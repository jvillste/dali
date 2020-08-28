(ns argumentica.db.query-test
  (:require [argumentica.comparator :as comparator]
            [argumentica.db.query :as query]
            [clojure.test :refer :all]
            schema.test))

(use-fixtures :once schema.test/validate-schemas)

(defn create-sorted-set [& datoms]
  (apply sorted-set-by comparator/compare-datoms datoms))

(deftest test-datoms
  (let [collection (-> (create-sorted-set [:entity-1 :attribute-1 :add 1]
                                          [:entity-1 :attribute-1 :add 2]
                                          [:entity-1 :attribute-1 :add 3]))]

    (is (= '([:entity-1 :attribute-1 :add 1]
             [:entity-1 :attribute-1 :add 2]
             [:entity-1 :attribute-1 :add 3])
           (query/filter-by-pattern collection
                                    [])))

    (is (= '([:entity-1 :attribute-1 :add 2]
             [:entity-1 :attribute-1 :add 3])
           (query/filter-by-pattern collection
                                    [:entity-1 :attribute-1 :add 2])))

    (is (= '([:entity-1 :attribute-1 :add 3]
             [:entity-1 :attribute-1 :add 2]
             [:entity-1 :attribute-1 :add 1])
           (query/filter-by-pattern collection
                                    []
                                    {:reverse? true})))

    (is (= '([:entity-1 :attribute-1 :add 2]
             [:entity-1 :attribute-1 :add 1])
           (query/filter-by-pattern collection
                                    [:entity-1 :attribute-1 :add 2]
                                    {:reverse? true})))

    (is (= '([:entity-1 :attribute-1 :add 2])
           (query/filter-by-pattern collection
                                    [:entity-1 nil nil 2]
                                    {:reverse? true})))

    (is (= '([:entity-1 :attribute-1 :add 2])
           (query/filter-by-pattern collection
                                    [nil :attribute-1 nil 2])))))

(deftest test-substitutions-for-collection
  (is (= '({:?b 1}
           {:?b 2})
         (query/substitutions-for-collection (create-sorted-set [:a 1]
                                                                [:a 2])
                                             [:a :?b]))))

(def test-query-collection (create-sorted-set [:measurement-5 :nutirent :nutrient-1]
                                              [:measurement-1 :amount 10]
                                              [:measurement-1 :food :food-1]
                                              [:food-1 :category :pastry]

                                              [:measurement-2 :nutirent :nutrient-1]
                                              [:measurement-2 :amount 20]
                                              [:measurement-2 :food :food-2]
                                              [:food-2 :category :meat]))

(deftest test-substitutions-for-patterns
  (is (= '({:?measurement :measurement-1,
            :?amount 10,
            :?food :food-1,
            :?category :pastry}

           {:?measurement :measurement-2,
            :?amount 20,
            :?food :food-2,
            :?category :meat})
         (query/substitutions-for-patterns test-query-collection
                                           [[:?measurement :amount :?amount]
                                            [:?measurement :food :?food]
                                            [:?food :category :?category]])))

  (is (= '({:?category :meat,
            :?measurement :measurement-2,
            :?amount 20,
            :?food :food-2})
         (query/substitutions-for-patterns test-query-collection
                                           [[:?measurement :amount :?amount]
                                            [:?measurement :food :?food]
                                            [:?food :category :?category]]
                                           {:substitution {:?category :meat}}))))

(deftest test-query
  (is (= '(#:v{:measurement :measurement-1, :food :food-1}
           #:v{:measurement :measurement-2, :food :food-2})
         (query/query [{:sorted-set (create-sorted-set [:measurement-1 :food :food-1]
                                                       [:measurement-2 :food :food-2])
                        :patterns [[:v/measurement :food :v/food]]}])))

  (is (= '({:?measurement :measurement-1, :?food :food-1})
         (query/query-2 [(create-sorted-set [:measurement-1 :food :food-1]
                                            [:measurement-2 :food :food-2])
                         [:?measurement :food :?food]]
                        [(create-sorted-set [:measurement-1 :nutrient :nutrient-1]
                                            [:measurement-2 :nutrient :nutrient-2])
                         [:?measurement :nutrient :nutrient-1]])))


  (is (= '({:?a 1} {:?a 2} {:?a 3})
         (query/query [{:sorted-set (create-sorted-set 1 2 3)
                        :patterns [:?a]}])))

  (is (=  '({:?a 1} {:?a 3})
          (query/query-2 [(create-sorted-set 1 2 3) :?a]
                         [(create-sorted-set 3 4 5 1) :?a])))

  (is (= '({:?x :a, :?y :c}
           {:?x :d, :?y :e})
         (query/query-2 [(create-sorted-set [:a :b :c]
                                            [:d :b :e])
                         [:?x :b :?y]])))

  (is (= '({:?x :a, :?y :c}
           {:?x :a, :?y :e})
         (query/query-2 [(create-sorted-set [:a :b :c]
                                            [:d :b :e]
                                            [:a :b :e])
                         [:?x :b :?y]
                         [:?x :b :c]])))
  
  (is (= [{:?x 4}]
         (query/query-2 [(create-sorted-set [:a 1] [:b 2] [:b 4]) [:b :?x]]
                        [(create-sorted-set 3 4 5) :?x]))))

(deftest test-query-with-substitution

  (is (= '({:?x :a, :?y :c}
           {:?x :d, :?y :e})
         (query/query-with-substitution {}
                                        [(create-sorted-set [:a :b :c]
                                                            [:d :b :e])
                                         [:?x :b :?y]])))

  (is (= '({:?x :a, :?y :c})
         (query/query-with-substitution {:?x :a}
                                        [(create-sorted-set [:a :b :c]
                                                            [:d :b :e])
                                         [:?x :b :?y]])))


  (is (= '({:?x :a
            :?friend :b
            :?friend-name "Joe"})
         (query/query-with-substitution {:?x :a}
                                        [(create-sorted-set [:a :friend :b]
                                                            [:c :friend :a])
                                         [:?x :friend :?friend]]

                                        [(create-sorted-set [:a :name "John"]
                                                            [:b :name "Joe"])
                                         [:?friend :name :?friend-name]]))))
