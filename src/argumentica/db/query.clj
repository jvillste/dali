(ns argumentica.db.query
  (:require [argumentica.comparator :as comparator]
            [argumentica.index :as tuplect]
            [argumentica.util :as util]
            [schema.core :as schema]
            [argumentica.transducible-collection :as transducible-collection]
            [argumentica.sorted-reducible :as sorted-reducible]
            [argumentica.transducing :as transducing]
            [argumentica.reduction :as reduction]
            [clojure.string :as string]
            [medley.core :as medley])
  (:use clojure.test))

(defn variable? [value]
  (and (keyword? value)
       (or (= "v" (namespace value))
           (.startsWith (name value)
                        "?"))))

(defn anything? [value]
  (and (keyword? value)
       (= "*" (name value))))

(deftest test-variable?
  (is (variable? :v/foo))
  (is (variable? :?foo))
  (is (not (variable? :foo))))

(defn term-matches? [value term]
  (or (nil? term)
      (anything? term)
      (variable? term)
      (if-let [match (:match term)]
        (match value)
        (= value term))))

(defn- unify-term [value term]
  (cond (variable? term)
        [term value]

        (and (:key term)
             (term-matches? value term))
        [(:key term) value]

        (or (term-matches? value term)
            (anything? term))
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
         (unify :a :v/a)))

  (is (= {:?x :a}
         (unify [:a 2] [:?x {:minimum-value 1
                             :match <}])))

  (is (= {:?x :a, :?y 2}
         (unify [:a 2] [:?x {:minimum-value 1
                             :match <
                             :key :?y}])))

  (is (= nil
         (unify [:a 2] [:?x {:minimum-value 2
                             :match <}]))))

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

(defn match? [value pattern]
  (if (sequential? pattern)
    (every? true? (map term-matches? value pattern))
    (term-matches? value pattern)))

(deftest test-match?
  (is (match? 1 1))
  (is (not (match? 1 2)))
  (is (match? 1 nil))

  (is (match? 2
              {:match #(< 1 %)}))

  (is (not (match? 2
                   {:match #(= 1 %)})))

  (is (match? 2
              {:match (fn [value] (= 2 value))}))

  (is (not (match? 3
                   {:match (fn [value] (= 2 value))})))

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

(defn nil-or-varialbe? [value]
  (or (nil? value)
      (variable? value)
      (anything? value)))

(defn has-trailing-constants? [pattern]
  (if (sequential? pattern)
    (loop [pattern pattern]
      (cond (not (sequential? pattern))
            false

            (empty? pattern)
            false

            (and (nil-or-varialbe? (first pattern))
                 (some (complement nil-or-varialbe?)
                       (rest pattern)))
            true

            (not (nil-or-varialbe? (first pattern)))
            (recur (rest pattern))

            :default
            false))
    false))

(deftest test-has-trailing-constants?
  (is (not (has-trailing-constants? :a)))
  (is (not (has-trailing-constants? [:a])))
  (is (not (has-trailing-constants? [nil])))
  (is (has-trailing-constants? [nil :a]))
  (is (has-trailing-constants? [:?b :a]))
  (is (has-trailing-constants? [:a nil :a]))
  (is (has-trailing-constants? (concat [:a] [nil] [:a]))))

(def filter-by-pattern-options {(schema/optional-key :reverse?) schema/Bool})

(util/defno filter-by-pattern [sorted-set pattern {:keys [reverse?] :or {reverse? false}} :- filter-by-pattern-options]
  (throw (Exception. "filter-by-pattern is deprecated"))

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

(defn first-from-transducible-collection [transducible-collection]
  (transducible-collection/transduce transducible-collection
                                     nil
                                     {:transducer (take 1)
                                      :reducer reduction/last-value}))

(defn pattern-for-reverse-iteration [pattern sample-datom]
  (util/pad (count sample-datom)
            (start-pattern pattern)
            ::comparator/max))

(util/defno transduce-pattern [transducible-collection pattern options :- transducible-collection/transduce-options]
  (throw (Exception. "transduce-pattern is deprecated!"))

  (let [options (merge transducible-collection/default-transduce-options
                       options)
        pattern-has-trailing-constants? (has-trailing-constants? pattern)]
    (transducible-collection/transduce transducible-collection
                                       (if (= :backwards (:direction options))
                                         (pattern-for-reverse-iteration pattern
                                                                        (first-from-transducible-collection transducible-collection))
                                         pattern)
                                       (merge options
                                              {:transducer (if pattern-has-trailing-constants?
                                                             (comp (filter #(match? % pattern))
                                                                   (:transducer options))
                                                             (:transducer options))}))))

(def reducible-for-pattern-options {(schema/optional-key :direction) (schema/enum :forwards :backwards)})
(def default-reducible-for-pattern-options {:direction :forwards})

(defn starting-pattern [sorted-reducible pattern direction]
  (if (= :backwards direction)
    (pattern-for-reverse-iteration pattern (transduce (take 1)
                                                      reduction/last-value
                                                      (sorted-reducible/subreducible sorted-reducible ::comparator/min)))
    pattern))

(defn function-term-to-value [term]
  (or (:minimum-value term)
      term))

(util/defno reducible-for-pattern [sorted-reducible pattern options :- reducible-for-pattern-options]
  (let [options (merge default-reducible-for-pattern-options
                       options)
        pattern (starting-pattern sorted-reducible
                                  pattern
                                  (:direction options))]

    (sorted-reducible/subreducible sorted-reducible
                                   pattern
                                   (:direction options))))


(deftest test-reducible-for-pattern

  (is (= [[:measurement-1 :food :food-1 0 :add]
          [:measurement-1 :nutrient :nutrient-1 0 :add]]
         (into [] (reducible-for-pattern (apply sorted-set-by
                                                comparator/compare-datoms
                                                #{[:food-1 :category :pastry 0 :add]
                                                  [:measurement-1 :amount 10 0 :add]
                                                  [:measurement-1 :food :food-1 0 :add]
                                                  [:measurement-1 :nutrient :nutrient-1 0 :add]})
                                         [:measurement-1 :food]))))
  (is (= [2 3]
         (into [] (reducible-for-pattern (sorted-set 1 2 3)
                                         2))))

  (is (= [1 2 3]
         (into [] (reducible-for-pattern (sorted-set 1 2 3)
                                         nil))))

  (is (= [[:a 1] [:b 1]]
         (into [] (reducible-for-pattern (sorted-set-by comparator/compare-datoms
                                                        [:a 1]
                                                        [:b 1])
                                         [:a nil]))))

  (is (= [[:b 1]]
         (into [] (reducible-for-pattern (sorted-set-by comparator/compare-datoms
                                                        [:a 1]
                                                        [:b 1])
                                         [:b nil]))))

  (is (= [[:a 1 :b] [:a 2 :c] [:a 3 :b]]
         (into [] (reducible-for-pattern (sorted-set-by comparator/compare-datoms
                                                        [:a 1 :b]
                                                        [:a 2 :c]
                                                        [:a 3 :b])
                                         [:a nil :b])))))

(defn- variable-to-nil [term]
  (if (or (variable? term)
          (anything? term))
    nil
    term))

(defn wildcard-pattern [pattern]
  (if (sequential? pattern)
    (map (comp function-term-to-value
               variable-to-nil)
         pattern)
    (variable-to-nil (function-term-to-value pattern))))

(deftest test-wildcard-pattern
  (is (= [nil]
         (wildcard-pattern [:?a])))

  (is (= [1]
         (wildcard-pattern [1])))

  (is (= [1]
         (wildcard-pattern [{:minimum-value 1}])))

  (is (= 1
         (wildcard-pattern {:minimum-value 1})))

  (is (= nil
         (wildcard-pattern :?a))))

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

(defn substitution-transducer [pattern]
  (let [wildcard-pattern (wildcard-pattern pattern)]
    (comp (take-while #(match? % wildcard-pattern))
          (map #(unify % pattern)))))

(util/defno transduce-substitutions-for-collection [collection pattern options :- transducible-collection/transduce-options]
  (transduce-pattern collection
                     pattern
                     (transducible-collection/prepend-transducer options
                                                                 (substitution-transducer pattern))))

(deftest test-transduce-substitutions-for-collection
  (is (= [{}] (transduce-substitutions-for-collection (sorted-set 1 2 3)
                                                      2
                                                      {:reducer conj})))

  (is (= [{:?x 1} {:?x 2} {:?x 3}]
         (transduce-substitutions-for-collection (sorted-set-by comparator/compare-datoms
                                                                1 2 3)
                                                 :?x
                                                 {:reducer conj})))

  (is (= [{:?x 1} {:?x 3}]
         (transduce-substitutions-for-collection (sorted-set-by comparator/compare-datoms
                                                                [:a 1]
                                                                [:b 2]
                                                                [:a 3])
                                                 [:a :?x]
                                                 {:reducer conj}))))

(defn continue-scan-for-term? [value term]
  (or (term-matches? value term)
      (if-let [continue-scan? (:continue-scan? term)]
        (continue-scan? value))
      (and (:match term)
           (not (:minimum-value term)))))

(defn continue-scan? [pattern value]
  (if (sequential? pattern)
    (every? true? (map continue-scan-for-term? value pattern))
    (continue-scan-for-term? value pattern)))

(defn substitution-reducible [sorted-reducible pattern]
  (let [wildcard-pattern (wildcard-pattern pattern)
        has-trailing-constants? (has-trailing-constants? pattern)]
    (eduction (comp (take-while #(or has-trailing-constants?
                                     (continue-scan? pattern %)))
                    (map #(unify % pattern))
                    (remove nil?))
              (reducible-for-pattern sorted-reducible
                                     wildcard-pattern))))

(defn with-scan-length [body-function]
  (let [{:keys [count result]} (util/with-call-count #'unify body-function)]
    {:scan-length count
     :result result}))

(deftest test-substitution-reducible
  (is (= [{}] (into [] (substitution-reducible (sorted-set 1 2 3)
                                               2))))

  (is (= [{:?x 1} {:?x 2} {:?x 3}]
         (into [] (substitution-reducible (sorted-set-by comparator/compare-datoms
                                                         1 2 3)
                                          :?x))))

  (is (= [{:?x 1} {:?x 3}]
         (into [] (substitution-reducible (sorted-set-by comparator/compare-datoms
                                                         [:a 1]
                                                         [:b 2]
                                                         [:a 3])
                                          [:a :?x]))))

  (is (= [{:?x 1} {:?x 3}]
         (into [] (substitution-reducible (sorted-set-by comparator/compare-datoms
                                                         [:a 1 :c]
                                                         [:b 2 :c]
                                                         [:a 3 :c])
                                          [:a :?x]))))

  (is (= [{:?a :a}
          {:?a :b}]
         (into [] (substitution-reducible (sorted-set-by comparator/compare-datoms
                                                         [:a 1 :c]
                                                         [:b 2 :c])
                                          [:?a :*]))))

  (is (= [{:?y 2, :?x :b}
          {:?y 3, :?x :c}]
         (into [] (substitution-reducible (sorted-set-by comparator/compare-datoms
                                                         [1 :a]
                                                         [2 :b]
                                                         [3 :c])
                                          [{:key :?y
                                            :minimum-value 2
                                            :match <=}
                                           :?x]))))

  (is (= [{:?x :b}
          {:?x :c}]
         (into [] (substitution-reducible (sorted-set-by comparator/compare-datoms
                                                         [1 :a]
                                                         [2 :b]
                                                         [3 :c])
                                          [{:minimum-value 2
                                            :match <=}
                                           :?x]))))

  (is (= {:scan-length 1,
          :result [{:?x :b}]}
         (with-scan-length (fn [] (into [] (substitution-reducible (sorted-set-by comparator/compare-datoms
                                                                                  [1 :a]
                                                                                  [2 :b]
                                                                                  [3 :c])
                                                                   [{:minimum-value 2
                                                                     :match #(= 2 %)}
                                                                    :?x]))))))

  (is (= {:scan-length 3,
          :result [{:?x 2} {:?x 3} {:?x 4}]}
         (with-scan-length (fn [] (into [] (substitution-reducible (sorted-set 1 2 3 4 5 6)
                                                                   {:minimum-value 2
                                                                    :match (fn [value]
                                                                             (and (>= value 2)
                                                                                  (<= value 4)))
                                                                    :key :?x}))))))

  (is (= {:scan-length 4, :result [{:?x 2}]}
         (with-scan-length (fn [] (into [] (substitution-reducible (sorted-set-by comparator/compare-datoms
                                                                                  [1 0]
                                                                                  [2 1]
                                                                                  [3 0]
                                                                                  [4 0])
                                                                   [:?x
                                                                    {:match #(= % 1)}]))))))


  (is (= {:scan-length 4, :result [{:?x 2, :?y 11}
                                   {:?x 3, :?y 12}]}
         (with-scan-length (fn [] (into [] (substitution-reducible (sorted-set-by comparator/compare-datoms
                                                                                  [1 10]
                                                                                  [2 11]
                                                                                  [3 12]
                                                                                  [4 13])
                                                                   [:?x
                                                                    {:minimum-value 11
                                                                     :match (fn [value]
                                                                              (and (>= value 11)
                                                                                   (<= value 12)))
                                                                     :key :?y}]))))))

  (is (= [{:?x :?a} {:?x 1} {:?x 3}]
         (into [] (substitution-reducible (sorted-set-by comparator/compare-datoms
                                                         [:a :?a]
                                                         [:a 1]
                                                         [:b 2]
                                                         [:a 3])
                                          [:a :?x])))))

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
  (assert (satisfies? transducible-collection/TransducibleCollection sorted-set))
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


(util/defno substitution-reducible-for-patterns [sorted-reducible patterns options :- substitutions-for-patterns-options]
  (assert (satisfies? sorted-reducible/SortedReducible sorted-reducible))

  (loop [substitutions (if-let [substitution (:substitution options)]
                         [substitution]
                         (substitution-reducible sorted-reducible
                                                 (first patterns)))
         patterns (if (:substitution options)
                    patterns
                    (rest patterns))]

    (if-let [pattern (first patterns)]
      (recur (eduction (mapcat (fn [substitution]
                                 (eduction (map #(merge substitution %))
                                           (substitution-reducible sorted-reducible
                                                                   (substitute-pattern pattern substitution)))))
                       substitutions)

             (rest patterns))
      substitutions)))

(deftest test-substitution-reducile-for-patterns
  (is (= '({:?x :?a} {:?x 1} {:?x 3})
         (substitution-reducible-for-patterns (sorted-set-by comparator/compare-datoms
                                                             [:a :?a]
                                                             [:a 1]
                                                             [:b 2]
                                                             [:a 3])
                                              [[:a :?x]]))))

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

(defn reducible-query [substitution & body]
  (assert (or (nil? substitution)
              (map? substitution)))

  (let [participants (map (fn [[sorted-reducible & patterns]]
                            {:sorted-reducible sorted-reducible
                             :patterns patterns})
                          body)]

    (loop [substitutions (if substitution
                           [substitution]
                           (substitution-reducible-for-patterns (:sorted-reducible (first participants))
                                                                (:patterns (first participants))))
           participants (if substitution
                          participants
                          (rest participants))]

      (if-let [participant (first participants)]
        (recur (eduction (mapcat (fn [substitution]
                                   (substitution-reducible-for-patterns (:sorted-reducible participant)
                                                                        (:patterns participant)
                                                                        {:substitution substitution})))
                         substitutions)
               (rest participants))
        substitutions))))

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
