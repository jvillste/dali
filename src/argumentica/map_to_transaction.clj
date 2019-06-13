(ns argumentica.map-to-transaction
  (:require [flatland.useful.map :as map]
            [clojure.test :refer :all]
            [argumentica.db.common :as common]
            [clojure.set :as set]
            [clojure.string :as string]))

(def initial-label-sequence-labeler-state {:id-map {}
                                           :id-sequence (range)})

(defn label-with-label-sequence [state value]
  (let [state (if (get (:id-map state) value)
                state
                (-> state
                    (update :id-map
                            assoc
                            value
                            (first (:id-sequence state)))
                    (update :id-sequence rest)))]
    [state (get-in state [:id-map value])]))

(def initial-label-generator-labeler-state {})

(defn labeler-for-label-generator [label-generator]
  (fn [state value]
    (let [state (if (get state value)
                  state
                  (assoc state value (label-generator)))]
      [state (get state value)])))

(defn map-with-state [initial-state function input-sequence]
  (loop [state initial-state
         input-sequence input-sequence
         result-sequence []]
    (if-let [value (first input-sequence)]
      (let [[new-state new-value] (function state value)]
        (recur new-state
               (rest input-sequence)
               (conj result-sequence new-value)))
      [state result-sequence])))

(deftest test-map-with-state
  (is (= [0 1 0 2]
         (second (map-with-state initial-label-sequence-labeler-state
                                 label-with-label-sequence
                                 [:a :b :a :c]))))

  (is (= [0 1 0 2]
         (second (map-with-state initial-label-generator-labeler-state
                                 (labeler-for-label-generator (common/create-test-id-generator))
                                 [:a :b :a :c])))))

(defn map-vals-with-state [initial-state function input-map]
  (let [[new-state key-value-pairs] (map-with-state initial-state
                                                    (fn [state [key value]]
                                                      (let [[new-state new-value] (function state value)]
                                                        [new-state [key new-value]]))
                                                    input-map)]
    [new-state (into {}
                     key-value-pairs)]))

(deftest test-map-vals-with-state
  (is (= {1 0
          2 1
          3 0
          4 2}
         (second (map-vals-with-state initial-label-sequence-labeler-state
                                      label-with-label-sequence
                                      {1 :a
                                       2 :b
                                       3 :a
                                       4 :c})))))

(defn- assign-ids
  ([create-id a-map]
   (second (assign-ids {}
                       create-id
                       a-map)))
  ([keyword-ids-map create-id a-map]
   (let [keyword-ids-map (if (and (keyword? (:dali/id a-map))
                                  (nil? (get keyword-ids-map (:dali/id a-map))))
                           (assoc keyword-ids-map
                                  (:dali/id a-map)
                                  (create-id))
                           keyword-ids-map)]
     (map-vals-with-state keyword-ids-map
                          (fn [keyword-ids-map value]
                            (cond (map? value)
                                  (assign-ids keyword-ids-map
                                              create-id
                                              value)

                                  (set? value)
                                  (let [[keyword-ids-map new-values] (map-with-state keyword-ids-map
                                                                                     (fn [keyword-ids-map value-in-set]
                                                                                       (if (map? value-in-set)
                                                                                         (assign-ids keyword-ids-map
                                                                                                     create-id
                                                                                                     value-in-set)
                                                                                         [keyword-ids-map value-in-set]))
                                                                                     value)]
                                    [keyword-ids-map (set new-values)])

                                  :default
                                  [keyword-ids-map value]))
                          (assoc a-map :dali/id (cond (keyword? (:dali/id a-map))
                                           (get keyword-ids-map (:dali/id a-map))

                                           (not (nil? (:dali/id a-map)))
                                           (:dali/id a-map)

                                           :default
                                           (create-id)))))))

(deftest test-assign-ids
  (is (= {:child {:type :child, :dali/id 1},
          :children #{{:type :child, :number 2, :dali/id 2}
                      {:type :child, :number 1, :dali/id 3}},
          :child-with-id {:type :child, :dali/id 10},
          :dali/id 0}
         (assign-ids (common/create-test-id-generator)
                     {:child {:type :child}
                      :children #{{:type :child
                                   :number 1}
                                  {:type :child
                                   :number 2}}
                      :child-with-id {:type :child
                                      :dali/id 10}})))

  (testing "keyword ids"
    (is (= {:dali/id 0, :foo 1}
           (assign-ids (common/create-test-id-generator)
                       {:dali/id :id-1
                        :foo 1})))

    (is (= {:children #{{:dali/id 1, :bar 2}
                        {:dali/id 1, :foo 1}}
            :dali/id 0}
           (assign-ids (common/create-test-id-generator)
                       {:children #{{:dali/id :id-1
                                     :foo 1}
                                    {:dali/id :id-1
                                     :bar 2}}})))

    (is (= {:child-1 {:dali/id 1}
            :child-2 {:dali/id 2}
            :child-3 {:dali/id 1}
            :dali/id 0}
           (assign-ids (common/create-test-id-generator)
                       {:child-1 {:dali/id :id-1}
                        :child-2 {:dali/id :id-2}
                        :child-3 {:dali/id :id-1}})))))

(defn- value-to-transaction-value [value]
  (if (map? value)
    (:dali/id value)
    value))

(defn- reverse-attribute? [attribute]
  (string/starts-with? (name attribute)
                       "<-"))

(defn- forward-attribute [reverse-attribute]
  (keyword (namespace reverse-attribute)
           (.substring (name reverse-attribute)
                       2)))

(deftest test-forward-attribute
  (is (= :foo (forward-attribute :<-foo)))
  (is (= ::foo (forward-attribute ::<-foo))))

(defn- one-map-to-statements [a-map]
  (reduce (fn [transaction [attribute value]]
            (cond (and (reverse-attribute? attribute)
                       (set? value))
                  (concat transaction
                          (for [value-in-set value]
                            [(value-to-transaction-value value-in-set)
                             (forward-attribute attribute)
                             :add
                             (:dali/id a-map)]))

                  (and (reverse-attribute? attribute)
                       (not (set? value)))
                  (conj transaction [(value-to-transaction-value value)
                                     (forward-attribute attribute)
                                     :set
                                     (:dali/id a-map)])

                  (= :dali/id attribute)
                  transaction

                  (set? value)
                  (concat transaction
                          (for [value-in-set value]
                            [(:dali/id a-map)
                             attribute
                             :add
                             (value-to-transaction-value value-in-set)]))


                  :default
                  (conj transaction [(:dali/id a-map)
                                     attribute
                                     :set
                                     (value-to-transaction-value value)])))
          []
          a-map))

(deftest test-one-map-to-statements
  (is (= '[[0 :type :set :parent]
           [0 :child :set 1]]
         (one-map-to-statements {:type :parent
                                 :child {:type :child, :dali/id 1},
                                 :dali/id 0}))))

(defn- map-to-statements [id-generator a-map]
  (let [a-map-with-ids (assign-ids id-generator a-map)]
    (concat (one-map-to-statements a-map-with-ids)
            (mapcat (partial map-to-statements id-generator)
                    (filter map? (vals a-map-with-ids)))
            (mapcat (partial map-to-statements id-generator)
                    (mapcat (partial filter map?) (filter set? (vals a-map-with-ids)))))))


(deftest test-map-to-statements
  (is (= '([6 :one-parent-is :set 0]
           [0 :child-with-id :set 10]
           [0 :child :set 3]
           [0 :type :set :parent]
           [0 :children :add 1]
           [0 :children :add 2]
           [4 :parent-is :add 0]
           [5 :parent-is :add 0]
           [3 :type :set :child]
           [10 :type :set :child]
           [6 :type :set :child]
           [6 :name :set "Child 5"]
           [1 :type :set :child]
           [1 :name :set "Child 2"]
           [2 :type :set :child]
           [2 :name :set "Child 1"]
           [4 :type :set :child]
           [4 :name :set "Child 4"]
           [5 :type :set :child]
           [5 :name :set "Child 3"])
         (map-to-statements (common/create-test-id-generator)
                            {:type :parent
                             :children #{{:type :child
                                          :name "Child 1"}
                                         {:type :child
                                          :name "Child 2"}}
                             :child {:type :child},
                             :child-with-id {:type :child
                                             :dali/id 10}
                             :<-parent-is #{{:type :child
                                             :name "Child 3"}
                                            {:type :child
                                             :name "Child 4"}}
                             :<-one-parent-is {:type :child
                                               :name "Child 5"}}))))

(defn map-to-transaction [a-map]
  (set (map-to-statements (common/create-id-generator)
                          a-map)))

(defn maps-to-transaction [& maps]
  (apply set/union (map map-to-transaction maps)))

(deftest test-maps-to-transaction
  (is (= #{[0 :things :add 2]
           [2 :tags :add 100]
           [200 :title :set "tag 2"]
           [1 :tags :add 100]
           [100 :title :set "tag 1"]
           [2 :tags :add 200]
           [0 :things :add 1]}
         (binding [common/create-id-generator common/create-test-id-generator]
           (maps-to-transaction (let [tag-1 {:dali/id 100
                                             :title "tag 1"}
                                      tag-2 {:dali/id 200
                                             :title "tag 2"}]
                                  {:things #{{:tags #{tag-1 tag-2}}
                                             {:tags #{tag-1}}}}))))))
