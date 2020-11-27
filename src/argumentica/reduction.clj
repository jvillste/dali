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
