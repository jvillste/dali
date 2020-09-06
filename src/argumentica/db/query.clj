(ns argumentica.db.query
  (:require [argumentica.comparator :as comparator]
            [argumentica.index :as tuplect]
            [argumentica.util :as util]
            [schema.core :as schema])
  (:use clojure.test))

(defn variable? [value]
  (and (keyword? value)
       (or (= "v" (namespace value))
           (.startsWith (name value)
                        "?"))))

(deftest test-variable?
  (is (variable? :v/foo))
  (is (variable? :?foo))
  (is (not (variable? :foo))))

(defn- unify-term [value term]
  (cond (variable? term)
        [term value]

        (= value term)
        :matching-constant

        :default
        :mismatch))

(defn unify [value pattern]
  (let [substitution-pairs (if (sequential? pattern)
                             (->> (map unify-term
                                       value
                                       pattern))
                             [(unify-term value pattern)])]
    (if (some #{:mismatch} substitution-pairs)
      nil
      (into {}
            (remove #{:matching-constant}
                    substitution-pairs)))))

(deftest test-unify
  (is (= {}
         (unify [:a] [:a])))

  (is (= nil
         (unify [:a] [:b])))

  (is (= {:v/a :a}
         (unify [:a] [:v/a])))

  (is (= {:v/a :a}
         (unify :a :v/a))))

(defn- substitute-term [term substitution]
  (or (get substitution term)
      term))

(defn substitute [pattern substitution]
  (if (sequential? pattern)
    (map #(substitute-term % substitution)
         pattern)
    (substitute-term pattern substitution)))

(deftest test-substitute
  (is (= 1
         (substitute :v/a
                     {:v/a 1})))
  
  (is (= [1 :b]
         (substitute [:v/a :b]
                     {:v/a 1}))))

(defn term-matches? [value term]
  (or (nil? term)
      (= value term)))

(defn match? [value pattern]
  (if (sequential? pattern)
    (every? true? (map term-matches? value pattern))
    (term-matches? value pattern)))

(deftest test-match?
  (is (match? 1 1))
  (is (not (match? 1 2)))
  (is (match? 1 nil))

  (are [tuple pattern] (match? tuple pattern)
    [] []
    [:a] [:a]
    [:a] [nil]
    [:a :b] [:a nil]
    [:a :b] [nil :b]
    [:a :b] [:a])

  (are [tuple pattern] (not (match? tuple pattern))
    [:a] [:b]
    [:a :b] [:a :a]
    [:a :b] [:b nil]
    [:a :b] [nil :a]))

(defn start-pattern [pattern]
  (if (sequential? pattern)
    (take-while some? pattern)
    pattern))

(deftest test-start-pattern
  (is (= :a (start-pattern :a)))

  (is (= []
         (start-pattern [])))

  (is (= [:a]
         (start-pattern [:a])))

  (is (= [:a]
         (start-pattern [:a nil :b]))))

(def filter-by-pattern-options {(schema/optional-key :reverse?) schema/Bool})

(defn has-trailing-constants? [pattern]
  (if (sequential? pattern)
    (loop [pattern pattern]
      (cond (not (sequential? pattern))
            false

            (empty? pattern)
            false

            (and (nil? (first pattern))
                 (some (complement nil?)
                       (rest pattern)))
            true

            (not (nil? (first pattern)))
            (recur (rest pattern))

            :default
            false))
    false))

(deftest test-has-trailing-constants?
  (is (not (has-trailing-constants? :a)))
  (is (not (has-trailing-constants? [:a])))
  (is (not (has-trailing-constants? [nil])))
  (is (has-trailing-constants? [nil :a]))
  (is (has-trailing-constants? [:a nil :a]))
  (is (has-trailing-constants? (concat [:a] [nil] [:a]))))

(util/defno filter-by-pattern [sorted-set pattern {:keys [reverse?] :or {reverse? false}} :- filter-by-pattern-options]
  (let [pattern-has-trailing-constants? (has-trailing-constants? pattern)]
    (or (->> (if reverse?
               (util/inclusive-reverse-subsequence sorted-set
                                                   (util/pad (count (first (util/inclusive-subsequence sorted-set nil)))
                                                             (start-pattern pattern)
                                                             ::comparator/max))
               (util/inclusive-subsequence sorted-set
                                           (start-pattern pattern)))
             (filter (fn [datom]
                       (or (not pattern-has-trailing-constants?)
                           (match? datom pattern)))))
        [])))

(defn- variable-to-nil [term]
  (if (variable? term)
    nil
    term))

(defn wildcard-pattern [pattern]
  (if (sequential? pattern)
    (map variable-to-nil
         pattern)
    (variable-to-nil pattern)))

(defn substitutions-for-collection [collection pattern]
  (let [wildcard-pattern (wildcard-pattern pattern)]
    (->> (filter-by-pattern collection
                            wildcard-pattern)
         (take-while #(match? % wildcard-pattern))
         (map #(unify % pattern)))))

(deftest test-substitutions-for-collection
  (is (= [{}] (substitutions-for-collection (sorted-set 1 2 3)
                                            2)))
  (is (= '({:?x 1} {:?x 2} {:?x 3})
         (substitutions-for-collection (sorted-set 1 2 3)
                                       :?x))))

(defn substitute-term [term substitution]
  (if (variable? term)
    (or (get substitution term)
        term)
    term))

(defn substitute-pattern [pattern substitution]
  (if (sequential? pattern)
    (map #(substitute-term % substitution)
         pattern)
    (substitute-term pattern substitution)))

(deftest test-substitute-pattern
  (is (= '(1 2 :v/b)
         (substitute-pattern [:v/a 2 :v/b]
                             {:v/a 1})))

  (is (= 1
         (substitute-pattern :v/a
                             {:v/a 1}))))

(def substitutions-for-patterns-options {(schema/optional-key :substitution) (schema/pred map?)})

(util/defno substitutions-for-patterns [sorted-set patterns options :- substitutions-for-patterns-options]
  (assert (instance? clojure.lang.Sorted sorted-set))
  (loop [substitutions (if-let [substitution (:substitution options)]
                         [substitution]
                         (substitutions-for-collection sorted-set
                                                       (first patterns)))
         patterns (if (:substitution options)
                    patterns
                    (rest patterns))]

    (if-let [pattern (first patterns)]
      (recur (mapcat (fn [substitution]
                       (map #(merge substitution %)
                            (substitutions-for-collection sorted-set
                                                          (substitute-pattern pattern substitution))))
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

(defn query [participants]
  (loop [substitutions (substitutions-for-patterns (:sorted-set (first participants))
                                                   (:patterns (first participants)))
         participants (rest participants)]

    (if-let [participant (first participants)]
      (recur (apply concat (for [substitution substitutions]
                             (substitutions-for-patterns (:sorted-set participant)
                                                         (:patterns participant)
                                                         {:substitution substitution})))
             (rest participants))
      substitutions)))

(defn query-2 [& body]
  (query (for [[sorted-set & patterns] body]
           {:sorted-set sorted-set
            :patterns patterns})))

(defn query-with-substitution [substitution & body]
  (loop [substitutions [substitution]
         participants (for [[sorted-set & patterns] body]
                        {:sorted-set sorted-set
                         :patterns patterns})]

    (if-let [participant (first participants)]
      (recur (apply concat (for [substitution substitutions]
                             (substitutions-for-patterns (:sorted-set participant)
                                                         (:patterns participant)
                                                         {:substitution substitution})))
             (rest participants))
      substitutions)))