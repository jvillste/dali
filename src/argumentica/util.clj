(ns argumentica.util
  (:use clojure.test)
  (:require [clojure.string :as string]
            [flatland.useful.map :as map]))

(defn filter-sorted-map-keys [the-sorted-map predicate]
  (apply sorted-map (apply concat (filter (fn [[key _value]]
                                            (predicate key))
                                          the-sorted-map))))

(deftest test-filter-sorted-map-keys
  (is (= {2 :2
          3 :3}
         (filter-sorted-map-keys (sorted-map 1 :1 2 :2 3 :3)
                                 #(<= 2 %)))))

;; from https://gist.github.com/ptaoussanis/e537bd8ffdc943bbbce7
(defn identity-transducing-function
  [reducing-function] ; A 'completed' reducing fn (i.e. with an `[accumulation]` arity)
  (fn
    ([] (reducing-function)) ; Only called/necessary when no init-accumulator given
    ([accumulation] (reducing-function accumulation))
    ([accumulation new-input]
     (reducing-function accumulation new-input))))

(defn count-logger
  "Count the number of items. Either used directly as a transducer or invoked with two args
   as a transducing context."
  [interval]
  (fn [reducing-function]
    (let [atomic-long-count (java.util.concurrent.atomic.AtomicLong.)]
      (fn
        ([]
         (reducing-function))

        ([accumulator]
         (reducing-function accumulator))

        ([accumulator new-input]
         (let [current-count (.incrementAndGet atomic-long-count)]
           (when (= 0 (mod current-count interval))
             (println current-count)))
         (reducing-function accumulator new-input))))))

(comment
  (transduce (count-logger 3) + (range 10)))

(defmacro defn-alias [target-symbol source-symbol]
  (let [metadata (meta (find-var (symbol (or (namespace source-symbol)
                                             (name (ns-name *ns*)))
                                         (name source-symbol))))]
    `(def ~(with-meta target-symbol {:doc (:doc metadata) :arglists `(quote ~(:arglists metadata))})
       ~source-symbol)))


(defn map-values [the-map function]
  (reduce (fn [result key] (update result key function))
          the-map
          (keys the-map)))

(defn deep-deref [value]
  (cond (instance? clojure.lang.Atom value)
        (atom (deep-deref (deref value)))

        (record? value)
        (map-values value
                           deep-deref)
        (map? value)
        (map-values value
                      deep-deref)

        (vector? value)
        (mapv deep-deref value)

        :default
        value))
