(ns argumentica.reduction
  (:require [clojure.test :refer :all])
  (:import clojure.lang.IReduceInit
           clojure.lang.IReduce))

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

(defn first-value
  ([] ::initial-value)
  ([result] result)
  ([reduced-value value]
   (reduced (if (= ::initial-value reduced-value)
              value
              reduced-value))))

(deftest test-first-value
  (is (= 0
         (reduce first-value (range 10))))

  (is (= nil
         (reduce first-value [nil 1 2])))

  (is (= 4
         (reduce ((filter #(> % 3))
                  first-value)
                 ::initial-value
                 (range 10))))

  (is (= 4
         (transduce (filter #(> % 3))
                    first-value
                    (range 10))))

  (is (= nil
         (transduce identity
                    first-value
                    [nil 1 2]))))

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

(defn process! [reducible & transducers]
  (transduce (apply comp transducers)
             (constantly nil)
             reducible))

(defmacro do-reducible [binding & body]
  (let [[variable reducible] binding]
    `(reduce (fn [result_# ~variable]
               ~@body
               nil)
             nil
             ~reducible)))

(deftest test-do-reducible
  (is (= [0 1 2]
         (let [result-atom (atom [])]
           (do-reducible [x (range 3)]
                         (swap! result-atom conj x))
           @result-atom))))

(defn educe [reducible & transducers]
  (eduction (apply comp transducers)
            reducible))

(deftest test-educe
  (is (= [1 2]
         (into []
               (educe (range 10)
                      (map inc)
                      (take 2))))))

(defn reduce-tree [root children reducing-function initial-value]
  (loop [reduced-value initial-value
         nodes [root]]
    (if-let [node (first nodes)]
      (if-let [the-children (children node)]
        (recur reduced-value
               (concat the-children
                       (rest nodes)))
        (let [reusult (reducing-function reduced-value
                                         node)]
          (if (reduced? reusult)
            @reusult
            (recur reusult
                   (rest nodes)))))
      reduced-value)))

(defn tree-reducible [root children]
  (reify
    IReduceInit
    (reduce [this reducing-function initial-reduced-value]
      (reducing-function (reduce-tree root
                                      children
                                      reducing-function
                                      initial-reduced-value)))

    IReduce
    (reduce [this reducing-function]
      (reducing-function (reduce-tree root
                                      children
                                      reducing-function
                                      (reducing-function))))))

(deftest test-tree-reducible
  (is (= [2 3 4]
         (into []
               (map inc)
               (tree-reducible {:a {:b 1
                                    :c 2}
                                :d 3}
                               (fn [value]
                                 (when (map? value)
                                   (vals value))))))))

(defn reducible [the-reduce]
  (reify
    IReduceInit
    (reduce [this reducing-function initial-value]
      (reducing-function (the-reduce reducing-function initial-value)))

    IReduce
    (reduce [this reducing-function]
      (reducing-function (the-reduce reducing-function (reducing-function))))))
