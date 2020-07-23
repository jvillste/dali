(ns argumentica.db.query
  (:require [argumentica.comparator :as comparator]
            [argumentica.index :as sorted-collection]
            [argumentica.util :as util]
            [schema.core :as schema])
  (:use clojure.test))

(defn variable? [value]
  (and (keyword? value)
       (or (= "v" (namespace value))
           (.endsWith (name value)
                      "?"))))

(deftest test-variable?
  (is (variable? :v/foo))
  (is (not (variable? :foo))))

(defn substitution-for-datom [datom pattern]
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

(deftest test-substitution-for-datom
  (is (= {}
         (substitution-for-datom [:a] [:a])))

  (is (= nil
         (substitution-for-datom [:a] [:b])))

  (is (= {:v/a :a}
         (substitution-for-datom [:a] [:v/a]))))

(defn substitute [pattern substitutions]
  (map (fn [pattern-value]
         (or (get substitutions pattern-value)
             pattern-value))
       pattern))

(deftest test-substitute
  (is (= [1 :b]
         (substitute [:v/a :b]
                     {:v/a 1}))))

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

(util/defno datoms [sorted-collection pattern {:keys [reverse?] :or {reverse? false}} :- datoms-options]
  (let [pattern-has-trailing-constants? (has-trailing-constants? pattern)]
    (or (->> (if reverse?
               (util/inclusive-reverse-subsequence sorted-collection
                                                   (util/pad (count (first (util/inclusive-subsequence sorted-collection nil)))
                                                             (start-pattern pattern)
                                                             ::comparator/max))
               (util/inclusive-subsequence sorted-collection
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

(defn substitutions-for-collection [sorted-collection pattern]
  (let [wildcard-pattern (wildcard-pattern pattern)]
    (->> (datoms sorted-collection
                 wildcard-pattern)
         (take-while #(match? % wildcard-pattern))
         (map #(substitution-for-datom % pattern)))))

(defn apply-substitution [pattern substitution]
  (map (fn [value]
         (if (variable? value)
           (or (get substitution value)
               value)
           value))
       pattern))

(deftest test-apply-substitution
  (is (= '(1 2 :v/b)
         (apply-substitution [:v/a 2 :v/b]
                             {:v/a 1}))))

(def query-options {(schema/optional-key :substitution) (schema/pred map?)})

(util/defno query [sorted-collection patterns options :- query-options]
  (loop [substitutions (if-let [substitution (:substitution options)]
                         [substitution]
                         (substitutions-for-collection sorted-collection
                                                       (first patterns)))
         patterns (rest patterns)]
    (if-let [pattern (first patterns)]
      (recur (mapcat (fn [substitution]
                       (map (fn [substitution-2]
                              (merge substitution
                                     substitution-2))
                            (substitutions-for-collection sorted-collection
                                                          (apply-substitution pattern substitution))))
                     substitutions)
             (rest patterns))
      substitutions)))

(defn project [variables substitution]
  ((apply juxt variables) substitution))

(deftest test-project
  (is (= [1 2] (project [:v/a :v/b]
                        {:v/a 1
                         :v/b 2
                         :v/c 3}))))

(defn projections [variables substitutions]
  (map (partial project variables)
       substitutions))

(defn join [participants]
  (loop [substitutions (query (:sorted-collection (first participants))
                              (:patterns (first participants)))
         participants (rest participants)]
    (if-let [participant (first participants)]
      (recur (rest participants)
             (apply concat (for [substitution substitutions]
                             (query (:sorted-collection participant)
                                    (:patterns participant)
                                    {:substitution substitution}))))
      substitutions)))
