(ns argumentica.map-to-transaction
  (:require [flatland.useful.map :as map]
            [clojure.test :refer :all]
            [argumentica.db.common :as common]
            [clojure.set :as set]
            [clojure.string :as string]))

(defn- assign-ids [create-id a-map]
  (-> (assoc a-map :dali/id (or (:dali/id a-map)
                                (create-id)))
      (map/map-vals (fn [value]
                      (cond (map? value)
                            (assign-ids create-id
                                        value)
                            (set? value)
                            (set (map (fn [value-in-set]
                                        (if (map? value-in-set)
                                          (assign-ids create-id
                                                      value-in-set)
                                          value-in-set))
                                      value))

                            :default
                            value)))))

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
                                      :dali/id 10}}))))

(defn- value-to-transaction-value [value]
  (if (map? value)
    (:dali/id value)
    value))

(defn- reverse-attribute? [attribute]
  (string/starts-with? (name attribute)
                       "<-"))

(defn- forward-attribute [reverse-attribute]
  (keyword (.substring (name reverse-attribute)
                       2)))

(defn- one-map-to-statements [a-map]
  (reduce (fn [transaction [attribute value]]
            (cond (and (reverse-attribute? attribute)
                       (set? value))
                  (concat transaction
                          (for [value-in-set value]
                            [(value-to-transaction-value value-in-set)
                             (forward-attribute attribute)
                             :set
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
                             :set
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
           [0 :children :set 1]
           [0 :children :set 2]
           [4 :parent-is :set 0]
           [5 :parent-is :set 0]
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
