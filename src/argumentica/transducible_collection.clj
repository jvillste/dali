(ns argumentica.transducible-collection
  (:require [schema.core :as schema]
            [clojure.test :refer :all]))

(def transduce-options {(schema/optional-key :direction) (schema/enum :forwards :backwards)
                        (schema/optional-key :initial-value) schema/Any
                        (schema/optional-key :transducer) (schema/pred fn?)
                        (schema/optional-key :reducer) (schema/pred fn?)
                        schema/Keyword schema/Any})

(def default-transduce-options {:transducer identity
                                :reducer (constantly nil)
                                :direction :forwards})

(defprotocol TransducibleCollection
  (transduce [this from-key options]))

(extend-protocol TransducibleCollection
  clojure.lang.Sorted
  (transduce [this from-key options]
    (let [options (merge default-transduce-options
                         options)
          subsequence (if (= :forwards (:direction options))
                        (subseq this >= from-key)
                        (rsubseq this <= from-key))]
      (if (contains? options
                     :initial-value)
        (clojure.core/transduce (:transducer options)
                                (:reducer options)
                                (:initial-value options)
                                subsequence)
        (clojure.core/transduce (:transducer options)
                                (:reducer options)
                                subsequence)))))

(deftest test-transduce
  (is (= [2 3]
         (transduce (sorted-set 1 2 3)
                    2
                    {:reducer conj})))

  (is (= [3 4]
         (transduce (sorted-set 1 2 3)
                    2
                    {:reducer conj
                     :transducer (map inc)})))

  (is (= [0 2 3]
         (transduce (sorted-set 1 2 3)
                    2
                    {:reducer conj
                     :initial-value [0]}))))

(defn prepend-transducer [options transducer]
  (merge options
         {:transducer (comp transducer
                            (:transducer (merge default-transduce-options
                                                options)))}))
