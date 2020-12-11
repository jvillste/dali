(ns argumentica.map-to-transaction
  (:require [flatland.useful.map :as map]
            [clojure.test :refer :all]
            [argumentica.db.common :as common]
            [clojure.set :as set]
            [clojure.string :as string]
            [argumentica.comparator :as comparator]
            [argumentica.util :as util]
            [schema.core :as schema]))

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
                                  (nil? (namespace (:dali/id a-map)))
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
                          (assoc a-map :dali/id (cond (and (keyword? (:dali/id a-map))
                                                           (nil? (namespace (:dali/id a-map))))
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

    (testing "fully qualified keywords are left untouched"
      (is (= {:dali/id :foo/bar}
             (assign-ids (common/create-test-id-generator)
                         {:dali/id :foo/bar}))))

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

(defn assign-ids-to-maps [create-id maps]
  (second (map-with-state {}
                          (fn [keyword-ids-map a-map]
                            (assign-ids keyword-ids-map
                                        create-id
                                        a-map))
                          maps)))

(deftest test-assign-ids-to-maps
  (is (= [#:dali{:id 0}
          #:dali{:id 1}
          #:dali{:id 100}
          #:dali{:id 2}
          #:dali{:id 2}
          #:dali{:id :bar/foo}]
         (assign-ids-to-maps (common/create-test-id-generator)
                             [{}
                              {}
                              {:dali/id 100}
                              {:dali/id :foo}
                              {:dali/id :foo}
                              {:dali/id :bar/foo}]))))

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

(def map-to-statements-options {(schema/optional-key :use-set-operator?) schema/Bool})

(util/defno map-to-statements [a-map {:keys [use-set-operator?]
                                      :or {use-set-operator? false}}
                               :- map-to-statements-options]
  (reduce (fn [transaction [attribute value]]
            (cond (= :dali/id attribute)
                  transaction

                  (and (reverse-attribute? attribute)
                       (set? value))
                  (concat transaction
                          (for [value-in-set value]
                            [(if use-set-operator?
                               :set
                               :add)
                             (value-to-transaction-value value-in-set)
                             (forward-attribute attribute)
                             (:dali/id a-map)]))

                  (and (reverse-attribute? attribute)
                       (not (set? value)))
                  (conj transaction [(if use-set-operator?
                                       :set
                                       :add)
                                     (value-to-transaction-value value)
                                     (forward-attribute attribute)
                                     (:dali/id a-map)])

                  (set? value)
                  (concat transaction
                          (for [value-in-set value]
                            [:add
                             (:dali/id a-map)
                             attribute
                             (value-to-transaction-value value-in-set)]))

                  :default
                  (conj transaction [(if use-set-operator?
                                       :set
                                       :add)
                                     (:dali/id a-map)
                                     attribute
                                     (value-to-transaction-value value)])))
          []
          a-map))

(deftest test-map-to-statements
  (is (= [[:add 1 :type :parent]]
         (map-to-statements {:dali/id 1
                             :type :parent})))

  (is (= [[:set 1 :type :parent]]
         (map-to-statements {:dali/id 1
                             :type :parent}
                            {:use-set-operator? true})))

  (is (= [[:add 2 :parent 1]]
         (map-to-statements {:dali/id 1 :<-parent {:dali/id 2}})))

  (is (= [[:add 3 :parent 1]
          [:add 2 :parent 1]]
         (map-to-statements {:dali/id 1 :<-parent #{{:dali/id 2}
                                                    {:dali/id 3}}}))))

(defn- map-with-ids-to-statements [a-map-with-ids]
  (concat (map-to-statements a-map-with-ids)
          (mapcat map-with-ids-to-statements
                  (filter map? (vals a-map-with-ids)))
          (mapcat map-with-ids-to-statements
                  (mapcat (partial filter map?)
                          (filter set? (vals a-map-with-ids))))))

(deftest test-map-with-ids-to-statements
  (is (= '([:add 1 :child 2]
           [:add 2 :type :child])
         (sort (map-with-ids-to-statements {:dali/id 1
                                            :child {:dali/id 2
                                                    :type :child}}))))

  (is (= '([:add 1 :children 2]
           [:add 1 :children 3]
           [:add 2 :name "Child 1"]
           [:add 3 :name "Child 2"])
         (sort (map-with-ids-to-statements {:dali/id 1
                                            :children #{{:dali/id 2
                                                         :name "Child 1"}
                                                        {:dali/id 3
                                                         :name "Child 2"}}}))))

  (is (= '([:add 2 :parent 1])
         (sort (map-with-ids-to-statements {:dali/id 1
                                            :<-parent {:dali/id 2}}))))

  (is (= '([:add 2 :parent 1]
           [:add 3 :parent 1])
         (sort comparator/compare-datoms
               (map-with-ids-to-statements {:dali/id 1
                                            :<-parent #{{:dali/id 2}
                                                        {:dali/id 3}}})))))

(defn maps-to-transaction [& maps]
  (assert (map? (first maps)))
  (apply set/union
         (map (fn [map-with-ids]
                (set (map-with-ids-to-statements map-with-ids)))
              (assign-ids-to-maps (common/create-id-generator)
                                  maps))))

(deftest test-maps-to-transaction
  (is (= #{[:add 0 :things 2]
           [:add 2 :tags 100]
           [:add 200 :title "tag 2"]
           [:add 1 :tags 100]
           [:add 100 :title "tag 1"]
           [:add 2 :tags 200]
           [:add 0 :things 1]}
         (binding [common/create-id-generator common/create-test-id-generator]
           (maps-to-transaction (let [tag-1 {:dali/id 100
                                             :title "tag 1"}
                                      tag-2 {:dali/id 200
                                             :title "tag 2"}]
                                  {:things #{{:tags #{tag-1 tag-2}}
                                             {:tags #{tag-1}}}})))))

  (is (= '([:add 0 :things 1]
           [:add 0 :things 3]
           [:add 1 :tags 2]
           [:add 2 :title "tag 1"]
           [:add 3 :tags 2]
           [:add 3 :tags 4]
           [:add 4 :title "tag 2"])
         (binding [common/create-id-generator common/create-test-id-generator]
           (sort (maps-to-transaction {:things #{{:tags #{{:dali/id :tag-1}
                                                          {:dali/id :tag-2}}}
                                                 {:tags #{{:dali/id :tag-1}}}}}
                                      {:dali/id :tag-1
                                       :title "tag 1"}
                                      {:dali/id :tag-2
                                       :title "tag 2"}))))))
