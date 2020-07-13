(ns argumentica.db.query
  (:require [argumentica.comparator :as comparator]
            [argumentica.index :as index]
            [argumentica.util :as util]
            [schema.core :as schema])
  (:use clojure.test))

(defn variable? [value]
  (and (keyword? value)
       (.endsWith (name value)
                  "?")))

(deftest test-variable?
  (is (variable? :foo?))
  (is (not (variable? :foo))))

(defn substitutions [datom pattern]
  (let [binding-pairs (->> (map (fn [datom-value pattern-value]
                                  (cond (variable? pattern-value)
                                        [pattern-value datom-value]

                                        (= datom-value pattern-value)
                                        :matching-constant

                                        :default
                                        :mismatch))
                                datom
                                pattern))]
    (if (some #{:mismatch} binding-pairs)
      nil
      (into {}
            (remove #{:matching-constant}
                    binding-pairs)))))

(deftest test-substitutions
  (is (= {}
         (substitutions [:a] [:a])))

  (is (= nil
         (substitutions [:a] [:b])))

  (is (= {:a? :a}
         (substitutions [:a] [:a?]))))

(defn substitute [pattern substitutions]
  (map (fn [pattern-value]
         (or (get substitutions pattern-value)
             pattern-value))
       pattern))

(deftest test-substitute
  (is (= [1 :b]
         (substitute [:a? :b]
                     {:a? 1}))))

(defn match? [datom pattern]
  (loop [datom-values datom
         pattern-values pattern]
    (if (empty? datom-values)
      true
      (let [datom-value (first datom-values)
            pattern-value (first pattern-values)]
        (if (or (nil? pattern-value)
                (= datom-value pattern-value))
          (recur (rest datom-values)
                 (rest pattern-values))
          false)))))

(deftest test-match?
  (are [datom pattern] (match? datom pattern)
    [] []
    [:a] [:a]
    [:a] [nil]
    [:a :b] [:a nil]
    [:a :b] [nil :b]
    [:a :b] [:a])

  (are [datom pattern] (not (match? datom pattern))
    [:a] [:b]
    [:a :b] [:a :a]
    [:a :b] [:b nil]
    [:a :b] [nil :a]))

(defn start-pattern [pattern]
  (take-while some? pattern))

(deftest test-start-pattern
  (is (= []
         (start-pattern [])))

  (is (= [:a]
         (start-pattern [:a])))
  
  (is (= [:a]
         (start-pattern [:a nil :b]))))

(def datoms-options {(schema/optional-key :reverse?) schema/Bool})

(defn has-trailing-constants? [pattern]
  (loop [pattern pattern]
    (cond (empty? pattern)
          false

          (and (nil? (first pattern))
               (some (complement nil?)
                     (rest pattern)))
          true

          (not (nil? (first pattern)))
          (recur (rest pattern))

          :default
          false)))

(deftest test-has-trailing-constants?
  (is (not (has-trailing-constants? [:a])))
  (is (not (has-trailing-constants? [nil])))
  (is (has-trailing-constants? [nil :a]))
  (is (has-trailing-constants? [:a nil :a])))

(util/defno datoms [index pattern {:keys [reverse?] :or {reverse? false}} :- datoms-options]
  (let [pattern-has-trailing-constants? (has-trailing-constants? pattern)]
    (or (->> (if reverse?
               (util/inclusive-reverse-subsequence (:index index)
                                                   (util/pad (count (first (util/inclusive-subsequence (:index index) nil)))
                                                             (start-pattern pattern)
                                                             ::comparator/max))
               (util/inclusive-subsequence (:index index)
                                           (start-pattern pattern)))
             (filter (fn [datom]
                       (or (not pattern-has-trailing-constants?)
                           (match? datom pattern)))))
        [])))

(defn wildcard-pattern [pattern]
  (map (fn [value]
         (if (variable? value)
           nil
           value))
       pattern))

(defn index-substitutions [index pattern]
  (let [wildcard-pattern (wildcard-pattern pattern)]
    (->> (datoms index
                 wildcard-pattern)
         (take-while #(match? % wildcard-pattern))
         (map #(substitutions % pattern)))))

(comment
  (query db [[:data-type? :nutrient? :amount? :food?]

             [:measurement? :nutrient :nutrient?]
             [:measurement? :amount :amount?]
             [:measurement? :food :food?]
             [:food? :data-type :data-type?]])
  ) ;; TODO: remove-me
