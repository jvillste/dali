(ns argumentica.util
  (:use clojure.test)
  (:require [clojure.string :as string]
            [flatland.useful.map :as map]
            [schema.core :as schema]
            [medley.core :as medley]))

(defn pad [n coll val]
  (take n (concat coll (repeat val))))

(defmacro defno [name arguments & body]
  (let [arguments-without-options (vec (drop-last 3 arguments))]
    `(schema/defn ~name
       (~arguments-without-options
        (~name ~@(conj arguments-without-options {})))

       (~arguments
        ~@body))))

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

(defn locking-swap-volatile! [volatile function & arguments]
  (locking volatile
    (vreset! volatile
             (apply function
                    @volatile
                    arguments))))

(defn inclusive-subsequence [sorted-collection minimum-value]
  (subseq sorted-collection >= minimum-value))

(defn inclusive-reverse-subsequence [sorted-collection maximum-value]
  (rsubseq sorted-collection <= maximum-value))

(def open-schema {schema/Keyword schema/Any})

(defmacro def-with-macro
  "Defines a macro that calls the given body as a function.
  The function will always be the last argument.
  For example:

  (def-with-macro with-start-and-end-logging [name body-function]
    (println \"start\" name)
    (body-function)
    (println \"end\" name))"
  [macro-name & docstring-arguments-body]

  (let [[docstring arguments & body] (if (string? (first docstring-arguments-body))
                                       docstring-arguments-body
                                       (concat [nil]
                                               docstring-arguments-body))
        function-name-symbol (with-meta (symbol (str (name macro-name) "*"))
                               (meta macro-name))
        body-symbol (gensym "body")
        macro-arguments-without-body (vec (map #(symbol (str "argument-" %))
                                               (range (dec (count arguments)))))]

    `(do (defn ~function-name-symbol ~arguments ~@body)
         (defmacro ~macro-name ~(vec (concat macro-arguments-without-body
                                             ['& body-symbol]))
           (list (symbol ~(str (ns-name *ns*))
                         ~(str (name function-name-symbol)))
                 ~@macro-arguments-without-body
                 (concat (list 'bound-fn [])
                         ~body-symbol)))
         (when ~docstring
           (alter-meta! (var ~macro-name) assoc :arglists (quote ~[(vec (concat (drop-last arguments)
                                                                                ['& 'body]))])
                        :doc ~docstring)))))

(alter-meta! #'def-with-macro assoc :arglists '([name doc-string [arguments*] & body]
                                                [name [arguments*] & body]))

(def-with-macro cancel-after-timeout [timeout return-value-if-timeout body-function]
  (let [the-future (future-call body-function)
        result (deref the-future timeout ::timeout)]
    (if (= ::timeout result)
      (do (future-cancel the-future)
          return-value-if-timeout)
      result)))

(comment
  (cancel-after-timeout 1000
                        :timeout
                        (Thread/sleep 100)
                        (println "hello"))
  )

(defn hexify [number]
  (format "%02x" number))

(deftest test-hexify
  (is (= "01"
         (hexify 1)))
  (is (= "10"
         (hexify 16))))

(defn unhexify [hex]
  (get (byte-array [(Integer/parseInt hex 16)]) 0))

(defn byte-array-values-to-vectors [the-map]
  (medley/map-vals (fn [value]
                     (cond (bytes? value)
                           (vec (map hexify value))

                           (map? value)
                           (byte-array-values-to-vectors value)

                           (vector? value)
                           (vec (map byte-array-values-to-vectors value))

                           :default
                           value))
                   the-map))

(deftest test-byte-array-values-to-vectors
  (is (= {:a ["61"]}
         (byte-array-values-to-vectors {:a (.getBytes "a")}))))

(defn with-call-count [counted-function-var body-function]
  (let [original-counted-function @counted-function-var
        count-atom (atom 0)]
    (with-redefs-fn {counted-function-var (fn [& arguments]
                                            (swap! count-atom inc)
                                            (apply original-counted-function arguments))}
      (fn []
        (let [result (body-function)]
          {:count @count-atom
           :result result})))))

(deftest test-with-call-count
  (defn test-identity [x] x)
  (is (= {:count 2, :result 3}
         (with-call-count #'test-identity
           (fn []
             (+ (test-identity 1)
                (test-identity 2)))))))
