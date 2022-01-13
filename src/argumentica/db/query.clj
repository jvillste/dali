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
            [medley.core :as medley]
            [clojure.set :as set]
            [argumentica.util :as util])
  (:use clojure.test))

;; from https://stackoverflow.com/questions/18246549/cartesian-product-in-clojure
(defn cartesian-product [collections]
  (if (empty? collections)
    [[]]
    (for [other-values (cartesian-product (rest collections))
          value (first collections)]
      (cons value other-values))))

(comment
  (take 1 (cartesian-product [(range 100) (range 100 200)]))
  (chunked-seq? (range 10))
  ) ;; TODO: remove-me

(deftest test-cartesian-product
  (is (= '((1 3 5)
           (2 3 5)
           (1 4 5)
           (2 4 5)
           (1 3 6)
           (2 3 6)
           (1 4 6)
           (2 4 6))
         (take 3 (cartesian-product [[1 2] [3 4] [5 6]])))))

(defn keyword-to-unconditional-variable [a-keyword]
  (keyword (str "?" (name a-keyword))))

(defn variable-key-name [variable-key]
  (subs (name variable-key)
        1))

(defn unconditional-variable-name [unconditional-variable]
  (variable-key-name (if-let [key (:key unconditional-variable)]
                       key
                       unconditional-variable)))

(defn unconditional-variable-to-keyword [unconditional-variable]
  (keyword (unconditional-variable-name unconditional-variable)))

(defn variable-keys-to-keywords [maps]
  (map (partial medley/map-keys unconditional-variable-to-keyword)
       maps))

(defn varialbe-key? [value]
  (and (keyword? value)
       (or (= "v" (namespace value))
           (.startsWith (name value)
                        "?"))))

(defn unconditional-variable? [value]
  (or (varialbe-key? value)
      (and (:key value)
           (not (:match value)))))

(defn conditional-variable? [term]
  (and (some? (:match term))
       (some? (:key term))))

(defn variable? [term]
  (or (unconditional-variable? term)
      (conditional-variable? term)))

(defn variable-key [term]
  (cond (unconditional-variable? term)
        term

        (conditional-variable? term)
        (:key term)

        :default
        nil))

(defn anything? [value]
  (and (keyword? value)
       (= "*" (name value))))

(deftest test-variable?
  (is (unconditional-variable? :v/foo))
  (is (unconditional-variable? :?foo))
  (is (not (unconditional-variable? :foo))))

(defn term-matches? [value term]
  (or (nil? term)
      (anything? term)
      (unconditional-variable? term)
      (if-let [match (:match term)]
        (match value)
        (= value term))))

(defn- unify-term [value term]
  (cond (varialbe-key? term)
        [term value]

        (and (:key term)
             (term-matches? value term))
        [(:key term) value]

        (or (term-matches? value term)
            (anything? term))
        :matching-constant

        :default
        :mismatch))

(defn unify-value [value pattern]
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

(deftest test-unify-value
  (is (= {}
         (unify-value [:a] [:a])))

  (is (= nil
         (unify-value [:a] [:b])))

  (is (= {:v/a :a}
         (unify-value [:a] [:v/a])))

  (is (= {:v/a :a}
         (unify-value :a :v/a)))

  (is (= {:?x :a}
         (unify-value [:a 2] [:?x {:minimum-value 1
                             :match <}])))

  (is (= {:?x :a, :?y 2}
         (unify-value [:a 2] [:?x {:minimum-value 1
                             :match <
                             :key :?y}])))

  (is (= nil
         (unify-value [:a 2] [:?x {:minimum-value 2
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
      (unconditional-variable? value)
      (anything? value)))

(defn has-trailing-conditions? [pattern]
  (if (sequential? pattern)
    (loop [preceding-variable? false
           pattern pattern]

      (cond (empty? pattern)
            false

            (nil-or-varialbe? (first pattern))
            (recur true
                   (rest pattern))

            preceding-variable?
            true

            :default
            (recur false
                   (rest pattern))))
    false))

(deftest test-has-trailing-conditions?
  (is (not (has-trailing-conditions? :a)))
  (is (not (has-trailing-conditions? [:a])))
  (is (not (has-trailing-conditions? [nil])))
  (is (has-trailing-conditions? [nil :a]))
  (is (has-trailing-conditions? [:?b :a]))
  (is (has-trailing-conditions? [:a nil :a]))
  (is (has-trailing-conditions? (concat [:a] [nil] [:a]))))

(def filter-by-pattern-options {(schema/optional-key :reverse?) schema/Bool})

(util/defno filter-by-pattern [sorted-set pattern {:keys [reverse?] :or {reverse? false}} :- filter-by-pattern-options]
  (let [pattern-has-trailing-conditions? (has-trailing-conditions? pattern)]
    (or (->> (if reverse?
               (util/inclusive-reverse-subsequence sorted-set
                                                   (util/pad (count (first (util/inclusive-subsequence sorted-set nil)))
                                                             (start-pattern pattern)
                                                             ::comparator/max))
               (util/inclusive-subsequence sorted-set
                                           (start-pattern pattern)))
             (filter (fn [datom]
                       (or (not pattern-has-trailing-conditions?)
                           (match? datom pattern)))))
        [])))

(defn nonconstant? [term]
  (or (unconditional-variable? term)
      (anything? term)))

(defn- variable-to-nil [term]
  (if (nonconstant? term)
    nil
    term))

(defn function-term-to-value [term]
  (or (:minimum-value term)
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

  (is (= [nil 1 nil]
         (wildcard-pattern [:?a 1 :?b])))

  (is (= [1]
         (wildcard-pattern [1])))

  (is (= [1]
         (wildcard-pattern [{:minimum-value 1}])))

  (is (= 1
         (wildcard-pattern {:minimum-value 1})))

  (is (= nil
         (wildcard-pattern :?a))))

(defn subsequence [sorted pattern]
  (util/inclusive-subsequence sorted (wildcard-pattern pattern)))

(defn- unique-subsequence-tail [sorted previous-result]
  (when-let [next-row (first (subseq sorted
                                     >=
                                     (concat previous-result
                                             [::comparator/max])))]
    (let [next-result (take (count previous-result)
                            next-row)]
      (lazy-seq (cons next-result
                      (unique-subsequence-tail sorted
                                               next-result))))))

(deftest test-unique-subsequence-tail
  (is (= '((2) (3))
         (unique-subsequence-tail (util/row-set [1 1]
                                                [1 2]
                                                [2 3]
                                                [2 4]
                                                [3 5]
                                                [3 6])
                                  [1]))))

(defn unique-subsequence [sorted starting-pattern]
  (when-let [first-row (first (subseq sorted >= (wildcard-pattern starting-pattern)))]
    (let [first-result (take (count starting-pattern)
                             first-row)]
      (lazy-seq (cons first-result
                      (unique-subsequence-tail sorted first-result))))))

(deftest test-unique-subsequence
  (is (= '((1) (2) (3))
         (unique-subsequence (util/row-set [1 1]
                                           [1 2]
                                           [2 3]
                                           [2 4]
                                           [3 5]
                                           [3 6])
                             [1])))

  (is (= '((1) (2) (3))
         (unique-subsequence (util/row-set [1 1]
                                           [1 2]
                                           [2 3]
                                           [2 4]
                                           [3 5]
                                           [3 6])
                             [nil])))

  (is (= '((1 1) (1 2) (2 3))
         (unique-subsequence (util/row-set [1 1]
                                           [1 2]
                                           [2 3])
                             [1 nil])))


  (is (= '((1 1) (1 2) (2 3))
         (unique-subsequence (util/row-set [1 1 1]
                                           [1 2 1]
                                           [1 2 2]
                                           [2 3 1])
                             [1 nil]))))

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
        pattern-has-trailing-conditions? (has-trailing-conditions? pattern)]
    (transducible-collection/transduce transducible-collection
                                       (if (= :backwards (:direction options))
                                         (pattern-for-reverse-iteration pattern
                                                                        (first-from-transducible-collection transducible-collection))
                                         pattern)
                                       (merge options
                                              {:transducer (if pattern-has-trailing-conditions?
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

(defn matching-subsequence [sorted pattern]
  (let [conditional-pattern-prefix (take-while #(and (not (unconditional-variable? %))
                                                     (not (anything? %)))
                                               pattern)
        subsequence (subsequence sorted pattern)]
    (if (has-trailing-conditions? pattern)
      (->> subsequence
           (take-while #(match? % conditional-pattern-prefix))
           (filter #(match? % pattern)))
      (->> subsequence
           (take-while #(match? % pattern))))))

(deftest test-matching-subsequence
  (is (= '([1 4 3])
         (matching-subsequence (util/row-set [1 2 3]
                                             [1 4 3])
                               [1 4 :?b])))

  (is (= '([2 1 2]
           [2 3 2])
         (matching-subsequence (util/row-set [1 1 1]
                                             [1 2 1]
                                             [2 1 2]
                                             [2 2 1]
                                             [2 3 2]
                                             [3 1 1]
                                             [3 2 1])
                               [2 :?x 2])))

  (is (= '([1 2]
           [2 2])
         (matching-subsequence (util/row-set [1 1]
                                             [1 2]
                                             [2 1]
                                             [2 2])
                               [:?x 2]))))

(defn select [sorted pattern]
  (->> (matching-subsequence sorted pattern)
       (map #(unify-value % pattern))))

(defn substitutions-for-collection [collection pattern]
  (let [wildcard-pattern (wildcard-pattern pattern)]
    (->> (filter-by-pattern collection
                            wildcard-pattern)
         (take-while #(match? % pattern))
         (map #(unify-value % pattern)))))

(defn matching-unique-subsequence [sorted pattern]
  (->> (unique-subsequence sorted
                           (wildcard-pattern pattern))
       (take-while #(match? % pattern))))

(defn unique-substitutions [sorted pattern]
  (->> (matching-unique-subsequence sorted pattern)
       (map #(unify-value % pattern))))

(deftest test-substitutions-for-collection
  (is (= [{}] (substitutions-for-collection (sorted-set 1 2 3)
                                            2)))
  (is (= '({:?x 1} {:?x 2} {:?x 3})
         (substitutions-for-collection (sorted-set 1 2 3)
                                       :?x))))

(defn substitution-transducer [pattern]
  (let [wildcard-pattern (wildcard-pattern pattern)]
    (comp (take-while #(match? % wildcard-pattern))
          (map #(unify-value % pattern)))))

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

(defn substitution-reducible [sorted-reducible pattern]
  (let [wildcard-pattern (wildcard-pattern pattern)
        has-trailing-conditions? (has-trailing-conditions? pattern)]
    (eduction (comp (map #(unify-value % pattern))
                    (take-while #(or has-trailing-conditions?
                                     (not (nil? %))))
                    (remove nil?))
              (reducible-for-pattern sorted-reducible
                                     wildcard-pattern))))

(defn with-scan-length [body-function]
  (let [{:keys [count result]} (util/with-call-count #'unify-value body-function)]
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


  (is (= {:scan-length 3
          :result [{:?y 2} {:?y 3}]}
         (with-scan-length (fn [] (into [] (substitution-reducible (sorted-set-by comparator/compare-datoms
                                                                                  [1 :a 1]
                                                                                  [2 :b 2]
                                                                                  [2 :c 3]
                                                                                  [3 :d 4])
                                                                   [2
                                                                    :*
                                                                    :?y]))))))

  (is (= {:scan-length 3
          :result [{}]}
         (with-scan-length (fn [] (into [] (substitution-reducible (sorted-set-by comparator/compare-datoms
                                                                                  [1 :a 1]
                                                                                  [2 :b 2]
                                                                                  [2 :c 3]
                                                                                  [3 :d 4])
                                                                   [2
                                                                    :*
                                                                    3]))))))

  (is (= {:scan-length 2,
          :result [{:?x :b}]}
         (with-scan-length (fn [] (into [] (substitution-reducible (sorted-set-by comparator/compare-datoms
                                                                                  [1 :a]
                                                                                  [2 :b]
                                                                                  [3 :c])
                                                                   [{:minimum-value 2
                                                                     :match #(= 2 %)}
                                                                    :?x]))))))

  (is (= {:scan-length 4,
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
  (cond (varialbe-key? term)
        (or (get substitution term)
            term)

        (:key term)
        (or (get substitution (:key term))
            term)

        :default
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
;;  (assert (satisfies? transducible-collection/TransducibleCollection sorted-set))
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
  (is (= [{:?x :?a} {:?x 1} {:?x 3}]
         (into []
               (substitution-reducible-for-patterns (sorted-set-by comparator/compare-datoms
                                                                   [:a :?a]
                                                                   [:a 1]
                                                                   [:b 2]
                                                                   [:a 3])
                                                    [[:a :?x]]))))

  ;; The call counts are doubled because of defno
  (is (= {:count 8,
          :result [{:?x 2} {:?x 3}]}
         (util/with-call-count #'reducible-for-pattern
           (fn []
             (into []
                   (substitution-reducible-for-patterns (sorted-set-by comparator/compare-datoms
                                                                       [:a 1]
                                                                       [:a 2]
                                                                       [:a 3]

                                                                       [:b 2]
                                                                       [:b 3])
                                                        [[:a :?x]
                                                         [:b :?x]]))))))


  (is (= {:count 6, :result [{:?x 2}]}
         (util/with-call-count #'reducible-for-pattern
           (fn []
             (into []
                   (take 1)
                   (substitution-reducible-for-patterns (sorted-set-by comparator/compare-datoms
                                                                       [:a 1]
                                                                       [:a 2]
                                                                       [:a 3]
                                                                       [:b 2]
                                                                       [:b 3])
                                                        [[:a :?x]
                                                         [:b :?x]]))))))

  ;;then implement https://en.wikipedia.org/wiki/Sort-merge_join

  )

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

(defn reducible-query-with-substitution [substitution & body]
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

(defn reducible-query [& body]
  (apply reducible-query-with-substitution nil body))

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



;; merge join


(defn variable-keys [pattern]
  (->> pattern
       (filter variable?)
       (map variable-key)))

(defn- common-variables [patterns]
  (->> patterns
       (map variable-keys)
       (map set)
       (apply set/intersection)))

(defn- pattern-to-combined-key [variables-set pattern]
  (->> pattern
       (drop-while (complement variables-set))
       (take-while variables-set)))

(deftest test-pattern-to-combined-key
  (is (= '(:?a :?b)
         (pattern-to-combined-key #{:?a :?b}
                                  [1 :?a :?b 2])))

  (is (= '(:?a)
         (pattern-to-combined-key #{:?a :?b}
                                  [1 :?a 2 :?b]))))

(defn common-combined-key-pattern [patterns]
  (->> patterns
       (map (partial pattern-to-combined-key
                     (common-variables patterns)))
       (map vec)
       (sort)
       (first)))

(deftest test-common-combined-key-pattern
  (is (= [:?a :?b]
         (common-combined-key-pattern [[1 :?a :?b]
                                       [1 2 :?a :?b :?c]])))

  (is (= [:?a]
         (common-combined-key-pattern [[1 :?a :?b]
                                       [1 :?a 2 :?b]]))))


(defn pattern-to-substitution [pattern value]
  (into {} (map vector pattern value)))

(deftest test-pattern-to-substitution
  (is (= {:?a 1, :?b 2}
         (pattern-to-substitution [:?a :?b] [1 2]))))

(defn advance-one-cursor-with-same-key [key-pattern cursors]
  (loop [cursors cursors
         checked-cursors []]
    (if-let [cursor (first cursors)]
      (if (and (some? (second (:substitution-sequence cursor)))
               (= (substitute-pattern key-pattern
                                      (first (:substitution-sequence cursor)))
                  (substitute-pattern key-pattern
                                      (second (:substitution-sequence cursor)))))
        (concat checked-cursors
                [(assoc cursor
                        :substitution-sequence (rest (:substitution-sequence cursor)))]
                (rest cursors))
        (recur (rest cursors)
               (conj checked-cursors
                     cursor)))
      checked-cursors)))

(deftest test-advance-one-cursor-with-same-key
  (is (= '({:substitution-sequence ({:?id 1})}
           {:substitution-sequence [{:?id 1}]})
         (advance-one-cursor-with-same-key [:?id]
                                           [{:substitution-sequence [{:?id 1}]}
                                            {:substitution-sequence [{:?id 1}]}])))

  (is (= '({:substitution-sequence ({:?id 1})}
           {:substitution-sequence [{:?id 1}]})
         (advance-one-cursor-with-same-key [:?id]
                                           [{:substitution-sequence [{:?id 1} {:?id 1}]}
                                            {:substitution-sequence [{:?id 1}]}])))

  (is (= '({:substitution-sequence ({:?id 1})}
           {:substitution-sequence [{:?id 1} {:?id 1}]})
         (advance-one-cursor-with-same-key [:?id]
                                           [{:substitution-sequence [{:?id 1} {:?id 1}]}
                                            {:substitution-sequence [{:?id 1} {:?id 1}]}]))))

(defn advance-all-cursors [key-pattern cursors]
  (map (fn [cursor]
         (let [substitution-sequence (rest (:substitution-sequence cursor))]
           (when (and (not (empty? substitution-sequence))
                      (some? (first substitution-sequence)))
             (assoc cursor
                    :substitution-sequence substitution-sequence
                    :first-key (vec (substitute-pattern key-pattern
                                                        (first substitution-sequence)))))))
       cursors))


(defn merge-join-selections [participants]
  (let [common-combined-key-pattern (common-combined-key-pattern (map :pattern participants))
        substitution-to-common-combined-key (fn [substitution]
                                              (vec (substitute-pattern common-combined-key-pattern
                                                                       substitution)))]
    (loop [cursors (map (fn [participant]
                          (let [substitution-sequence #_(substitutions-for-collection (:sorted participant)
                                                                                      (:pattern participant))
                                (->> (subseq (:sorted participant)
                                             >=
                                             (wildcard-pattern (:pattern participant)))
                                     (map #(unify-value % (:pattern participant))))]
                            (when (not (empty? substitution-sequence))
                              {:substitution-sequence substitution-sequence
                               :first-key (substitution-to-common-combined-key (first substitution-sequence))
                               :participant participant})))
                        participants)
           substitutions []]

      (if (every? some? cursors)
        (let [maximum-key (->> cursors
                               (map :first-key)
                               (sort)
                               (last))
              maximum-substitution (pattern-to-substitution common-combined-key-pattern
                                                            maximum-key)]

          (if (every? #(= maximum-key %)
                      (map :first-key cursors))
            (recur (let [advanced-cursors (advance-one-cursor-with-same-key common-combined-key-pattern
                                                                            cursors)]
                     (if (= cursors advanced-cursors)
                       (advance-all-cursors common-combined-key-pattern
                                            cursors)
                       advanced-cursors))
                   (conj substitutions
                         (apply merge (map #(first (:substitution-sequence %))
                                           cursors))))
            (recur (map (fn [cursor]
                          (if (= maximum-key (:first-key cursor))
                            cursor
                            (let [participant (:participant cursor)
                                  substitution-sequence (->> (subseq (get-in cursor [:participant :sorted])
                                                                     >=
                                                                     (wildcard-pattern (substitute-pattern (:pattern participant)
                                                                                                           maximum-substitution)))
                                                             (map #(unify-value % (:pattern participant))))]
                              (when (and (not (empty? substitution-sequence))
                                         (not (nil? (first substitution-sequence))))
                                (assoc cursor
                                       :substitution-sequence substitution-sequence
                                       :first-key (substitution-to-common-combined-key (first substitution-sequence)))))))
                        cursors)
                   substitutions)))
        substitutions))))



(deftest test-mergejoin-slections
  (is (= [{:?id1 2, :?id2 5, :?b :?b2, :?a :?a2-1}
          {:?id1 2, :?id2 5, :?b :?b2, :?a :?a2-2}]
         (merge-join-selections [{:sorted (util/row-set ["a" 1 4 :?b1]
                                             ["b" 2 5 :?b2]
                                             ["b" 3 7 :?b3])
                       :pattern ["b" :?id1 :?id2 :?b]}

                      {:sorted (util/row-set ["a" 1 4 :?a1]
                                             ["a" 2 5 :?a2-1]
                                             ["a" 2 5 :?a2-2]
                                             ["b" 3 7 :?a3])
                       :pattern ["a" :?id1 :?id2 :?a]}])))

  (is (= [{:?id 1, :?a :a1, :?b :b1, :?c :c1}
          {:?id 2, :?a :a2, :?b :b2, :?c :c2}
          {:?id 3, :?a :a3, :?b :b3, :?c :c3}]
         (merge-join-selections [{:sorted (util/row-set [1 :a1]
                                             [2 :a2]
                                             [3 :a3])
                       :pattern [:?id :?a]}

                      {:sorted (util/row-set [1 :b1]
                                             [2 :b2]
                                             [3 :b3])
                       :pattern [:?id :?b]}

                      {:sorted (util/row-set [1 :c1]
                                             [2 :c2]
                                             [3 :c3])
                       :pattern [:?id :?c]}]))))

(defn term-name [term]
  (cond (:original-key term)
        (unconditional-variable-name (:original-key term))

        (varialbe-key? term)
        (unconditional-variable-name term)

        (:key term)
        (unconditional-variable-name (:key term))

        :default
        nil))

(defn check-pattern! [columns pattern]
  (when (< (count columns)
           (count pattern))
    (throw (ex-info "Pattern is too long" {})))

  (loop [names (map name columns)
         terms pattern]
    (when-let [term (first terms)]
      (if-let [term-name (term-name term)]
        (if (= (first names)
               term-name)
          (recur (rest names)
                 (rest terms))
          (throw (ex-info "term name does not match column name" {:column-name (first names)
                                                                  :term-name term-name})))
        (recur (rest names)
               (rest terms))))))

(deftest test-check-pattern!
  (is (nil? (check-pattern! [:a :b]
                           [:?a :?b])))

  (is (nil? (check-pattern! [:a :b]
                           [:?a])))

  (is (nil? (check-pattern! [:a :b]
                           [{:original-key :?a}])))

  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"Pattern is too long"
                        (check-pattern! [:a :b]
                                        [:?a :?b :?c]))))

(defn loop-join [index pattern substitutions]
  (check-pattern! (:columns index) pattern)

  (apply concat (for [substitution substitutions]
                  (map (fn [new-substitution]
                         (merge substitution new-substitution))
                       (unique-substitutions (:sorted index)
                                                   (substitute pattern substitution))))))

(defn merge-join [partitioned-index-pattern-pairs]
  (let [partitioned-index-pattern-pairs partitioned-index-pattern-pairs]
    (run! (fn [[index pattern]]
            (check-pattern! (:columns index) pattern))
          partitioned-index-pattern-pairs)

    (merge-join-selections (map (fn [[index pattern]]
                                  {:sorted (:sorted index)
                                   :pattern pattern})
                                partitioned-index-pattern-pairs))))


(defn select-unique [index pattern]
  (check-pattern! (:columns index) pattern)

  (matching-unique-subsequence (:sorted index)
                               pattern))

(defn select [index pattern]
  (check-pattern! (:columns index) pattern)

  (let [result-row-length (count pattern)]
    (map (partial take result-row-length)
         (matching-subsequence (:sorted index)
                                     pattern))))

(defn unify [index pattern]
  (map #(unify % pattern)
       (select index pattern)))

(defn unify-unique [index pattern]
  (map #(unify % pattern)
       (select-unique index pattern)))

(defn rename [original-key-or-term new-key]
  (if (conditional-variable? original-key-or-term)
    (assoc original-key-or-term
           :original-key (:key original-key-or-term)
           :key new-key)

    {:original-key original-key-or-term
     :key new-key}))


(defn starts-with [string & [key]]
  {:minimum-value string
   :match (fn [value]
            (string/starts-with? value string))
   :key key})

(defn equals [key value]
  {:minimum-value value
   :match (partial = value)
   :key key})
