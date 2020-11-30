(ns argumentica.reduction
  (:require [clojure.test :refer :all]))

(defn double-reduced [reducing-function]
  (fn
    ([result]
     (reducing-function result))
    ([accumulator value]
     (let [result (reducing-function accumulator value)]
       (if (reduced? result)
         (reduced result)
         result)))))

(deftest test-double-reduced
  (is (reduced? (transduce (comp double-reduced
                                 (take 2))
                           conj
                           []
                           (range 4)))))

(def value-count
  (completing (fn [count value_]
                (inc count))))

(defn last-value
  ([] nil)
  ([result] result)
  ([accumulator_ value] value))

(defn find-first [predicate]
  (fn
    ([] nil)
    ([result] result)
    ([_result value]
     (if (not (predicate value))
       nil
       (reduced value)))))

(deftest test-find-first
  (is (= 4
         (reduce (find-first #(> % 3))
                 (range 10)))))

(defn take-up-to [predicate]
  (fn [reducing-function]
    (fn
      ([result]
       (reducing-function result))
      ([reduced-value new-value]
       (if (not (predicate new-value))
         (reducing-function reduced-value new-value)
         (ensure-reduced (reducing-function reduced-value new-value)))))))

(deftest test-take-up-to
  (is (= 4
         (reduce ((take-up-to #(> % 3))
                  last-value)
                 (range 10)))))

(comment
  (reduce ((comp (drop-while #(< % 4))
                 (take 1))
           last-value)
          (range 10)))
