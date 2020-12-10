(ns argumentica.reducible
  (:import clojure.lang.IReduceInit
           clojure.lang.IReduce))


;; from https://juxt.pro/blog/ontheflycollections-with-reducible

(defn preserving-reduced
  "copy-and-pasted from clojure.core, which declares it as private"
  [rf]
  #(let [ret (rf %1 %2)]
     (if (reduced? ret)
       (reduced ret)
       ret)))

(defn reduce-preserving-reduced
  "like reduce but preserves reduced"
  [reducing-function initial-value collection]
  (loop [reduced-value initial-value
         values collection]
    (if-let [value (first values)]
      (let [result (reducing-function reduced-value
                                      value)]
        (if (reduced? result)
          result
          (recur result
                 (rest values))))
      reduced-value)))

(defn chaining-reducible
  "like concat but for reducibles
  takes a coll of colls.
  Returns reducible that chains call to reduce over each coll"
  [coll-of-colls]
  (reify IReduceInit
    (reduce [_ f init]
      (let [prf (preserving-reduced f)]
        (reduce (partial reduce prf)
          init
          coll-of-colls)))))
