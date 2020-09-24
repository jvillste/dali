(ns argumentica.transducing
  (:refer-clojure :exclude [transduce])

  (:require [schema.core :as schema]
            [argumentica.util :as util]))

(def reduction-options {(schema/optional-key :initial-value) schema/Any
                        (schema/optional-key :transducer) (schema/pred fn?)
                        (schema/optional-key :reducer) (schema/pred fn?)})

(def default-reduction-options {:transducer identity
                                :reducer (constantly nil)})

(defn prepend-transducer [options transducer]
  (merge options
         {:transducer (comp transducer
                            (:transducer (merge default-reduction-options
                                                options)))}))

(defn stop-completion [reducing-function]
  (fn
    ([] (reducing-function))
    ([result] result)
    ([result input] (reducing-function result input))))

(schema/defn transduce [options :- reduction-options collection]
  (let [options (merge default-reduction-options
                       options)]
    (if (contains? options :initial-value)
      (clojure.core/transduce  (:transducer options)
                               (:reducer options)
                               (:initial-value options)
                               collection)
      (clojure.core/transduce  (:transducer options)
                               (:reducer options)
                               collection))))

(util/defno reduce-without-completion [reducing-function initial-value collection]
  (loop [reduced-value initial-value
         values collection]
    (if-let [value (first values)]
      (let [result (reducing-function reduced-value
                                      value)]
        (if (reduced? result)
          (reduced (reducing-function @result))
          (recur result
                 (rest values))))
      reduced-value)))


(def print-transducer (map (fn [value]
                             (prn value)
                             value)))

;; experiments

#_(schema/defn transduce-increments [collection options :- reduction-options]
  (let [{:keys [reducing-function initial-value]} (initialize-transducing (prepend-transducer options
                                                                                            (take 2)))]
    (loop [reduced-value initial-value
           increment 0]
      (let [reduced-value (reduce-without-completion reducing-function
                                                     reduced-value
                                                     (map #(+ increment %)
                                                          collection))]
        (if (reduced? reduced-value)
          (reducing-function @reduced-value)
          (recur reduced-value
                 (inc increment)))))))

(comment
  (transduce {:transducer cat
              :reducer conj} [(eduction (map #(+ 1 %)) [1 2 3])
                              (eduction (map #(+ 2 %)) [1 2 3])])

  (reduce conj [] (eduction (map #(+ 1 %)) [1 2 3]))
  (transduce-increments [1 2 3]
                        {:transducer (comp (take 5)
                                           (partition-all 3))
                         :reducer conj})

  (transduce {:transducer (comp (take 5)
                                (partition-all 3))
              :reducer conj}
             (range 10))
  ) ;; TODO: remove-me
