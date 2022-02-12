(ns argumentica.merged-sorted
  (:require
   [argumentica.comparator :as comparator]
   [argumentica.db.common :as db-common]
   [clojure.string :as string]
   [clojure.test :refer :all])
  (:import
   (argumentica.comparator DatomComparator)))

(defn add-to-transaction-number [number datom]
  (vec (concat (drop-last 2 datom)
               [(+ number
                   (db-common/datom-transaction-number datom))
                (db-common/datom-command datom)])))

(defn offset-transaction-number-in-pattern [offset sorted pattern]
  (let [datom-length (count (first (subseq sorted >= [])))]
    (cond (= (count pattern)
             datom-length)
          (add-to-transaction-number offset pattern)

          (= (count pattern)
             (dec datom-length))
          (vec (concat (drop-last 1 pattern)
                       [(+ (last pattern)
                           offset)]))

          :else
          pattern)))

(defn lazy-merged-sequence [upstream-sequence downstream-sequence]
  (let [upstream-datom (first upstream-sequence)
        downstream-datom (first downstream-sequence)]
    (when (or (some? upstream-datom)
              (some? downstream-datom))
      (lazy-seq (cond (nil? upstream-datom)
                      (cons downstream-datom
                            (lazy-merged-sequence upstream-sequence
                                                    (rest downstream-sequence)))
                      (nil? downstream-datom)
                      (cons upstream-datom
                            (lazy-merged-sequence (rest upstream-sequence)
                                                    downstream-sequence))

                      (> 0 (comparator/compare-datoms upstream-datom
                                                      downstream-datom))

                      (cons upstream-datom
                            (lazy-merged-sequence (rest upstream-sequence)
                                                    downstream-sequence))
                      :else
                      (cons downstream-datom
                            (lazy-merged-sequence upstream-sequence
                                                    (rest downstream-sequence))))))))

(defn merged-datom-sequence [upstream-sorted downstream-sorted last-upstream-transaction-number starting-pattern direction]
  (lazy-merged-sequence (->> (if (= :forwards direction)
                                 (subseq upstream-sorted
                                         >=
                                         starting-pattern)
                                 (rsubseq upstream-sorted
                                          <=
                                          starting-pattern))
                               (filter (fn [datom]
                                         (>= last-upstream-transaction-number
                                             (db-common/datom-transaction-number datom)))))
                          (let [downstream-starting-pattern (if (and (empty? upstream-sorted)
                                                                     (empty? downstream-sorted))
                                                              starting-pattern
                                                              (offset-transaction-number-in-pattern (- (inc last-upstream-transaction-number))
                                                                                                    (if (empty? upstream-sorted)
                                                                                                      downstream-sorted
                                                                                                      upstream-sorted)
                                                                                                    starting-pattern))]
                            (->> (if (= :forwards direction)
                                   (subseq downstream-sorted
                                           >=
                                           downstream-starting-pattern)
                                   (rsubseq downstream-sorted
                                            <=
                                            downstream-starting-pattern))
                                 (map (fn [datom]
                                        (->> datom
                                             (add-to-transaction-number (inc last-upstream-transaction-number)))))))))

(deftype MergedSorted [upstream-sorted downstream-sorted last-upstream-transaction-number]
  clojure.lang.Sequential
  clojure.lang.Seqable
  (seq [this]
    (merged-datom-sequence upstream-sorted
                             downstream-sorted
                             last-upstream-transaction-number
                             []
                             :forwards))
  clojure.lang.Sorted
  (comparator [this]
    (DatomComparator.))
  (entryKey [this entry]
    entry)
  (seq [this ascending?]
    (merged-datom-sequence upstream-sorted
                             downstream-sorted
                             last-upstream-transaction-number
                             (if ascending? [] :comparator/max)
                             (if ascending? :forwards :backwards)))
  (seqFrom [this value ascending?]
    (merged-datom-sequence upstream-sorted
                             downstream-sorted
                             last-upstream-transaction-number
                             value
                             (if ascending? :forwards :backwards))))


(defmethod print-method MergedSorted [branch ^java.io.Writer writer]
  (.write writer (string/trim (with-out-str (clojure.pprint/pprint (if (empty? branch)
                                                                     '()
                                                                     (seq branch)))))))


(defn create-test-merged-sorted [upstream-datoms downstream-datoms last-upstream-transaction-number]
  (->MergedSorted (apply sorted-set-by
                         comparator/compare-datoms
                         upstream-datoms)
                  (apply sorted-set-by
                         comparator/compare-datoms
                         downstream-datoms)
                  last-upstream-transaction-number))

(deftest test-merged-sorted
  (is (= '()
         (create-test-merged-sorted []
                                    []
                                    0)))

  (is (= '([:x 0 :add])
         (create-test-merged-sorted [[:x 0 :add]]
                                    []
                                    0)))

  (is (= '([:x 1 :add])
         (create-test-merged-sorted []
                                    [[:x 0 :add]]
                                    0)))

  (is (= '([:x 0 :add]
           [:y 1 :add])
         (create-test-merged-sorted [[:x 0 :add]]
                                    [[:y 0 :add]]
                                    0)))

  (is (= '([1 :name "bar" 1 :add]
           [1 :name "foo" 0 :add]
           [1 :name "foo" 1 :remove])
         (subseq (create-test-merged-sorted [[1 :name "foo" 0 :add]
                                             [2 :name "baz" 1 :add]]
                                            [[1 :name "foo" 0 :remove]
                                             [1 :name "bar" 0 :add]]
                                            0)
                 >=
                 [])))

  (is (= '([1 :name "bar" 2 :add]
           [1 :name "foo" 0 :add]
           [1 :name "foo" 2 :remove]
           [2 :name "baz" 1 :add])
         (subseq (create-test-merged-sorted [[1 :name "foo" 0 :add]
                                             [2 :name "baz" 1 :add]]
                                            [[1 :name "foo" 0 :remove]
                                             [1 :name "bar" 0 :add]]
                                            1)
                 >=
                 [])))

  (is (= '([1 :name "foo" 0 :add]
           [1 :name "foo" 1 :remove])
         (subseq (create-test-merged-sorted [[1 :name "foo" 0 :add]
                                             [2 :name "baz" 1 :add]]
                                            [[1 :name "foo" 0 :remove]
                                             [1 :name "bar" 0 :add]]
                                            0)
                 >=
                 [1 :name "foo" 0 :add])))

  (is (= '([1 :name "foo" 0 :add]
           [1 :name "foo" 1 :remove])
         (subseq (create-test-merged-sorted [[1 :name "foo" 0 :add]
                                             [2 :name "baz" 1 :add]]
                                            [[1 :name "foo" 0 :remove]
                                             [1 :name "bar" 0 :add]]
                                            0)
                 >=
                 [1 :name "foo" 0])))

  (is (= '([1 :name "bar" 1 :add]
           [1 :name "foo" 0 :add]
           [1 :name "foo" 1 :remove])
         (subseq (create-test-merged-sorted [[1 :name "foo" 0 :add]
                                             [2 :name "baz" 1 :add]]
                                            [[1 :name "foo" 0 :remove]
                                             [1 :name "bar" 0 :add]]
                                            0)
                 >=
                 [1 :name]))))
