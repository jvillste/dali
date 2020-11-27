(ns argumentica.db.persistent-sequence
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all])
  (:import java.io.ByteArrayOutputStream))

(defn write-to-stream! [output-stream value]
  (.write output-stream
          (.getBytes (prn-str value)
                     "UTF-8"))
  (.flush output-stream)
  output-stream)

(defn reduce-values-from-reader [ ^java.io.BufferedReader reader reducing-function initial-reduced-value]
  (loop [reduced-value initial-reduced-value]
    (if-let [line (.readLine reader)]
      (let [reduced-value (reducing-function reduced-value (read-string line))]
        (if (reduced? reduced-value)
          @reduced-value
          (recur reduced-value)))
      reduced-value)))

(deftest test-persistent-sequence
  (let [byte-array (.toByteArray (doto (ByteArrayOutputStream.)
                                   (write-to-stream! 1)
                                   (write-to-stream! 2)
                                   (write-to-stream! 3)))]
    (is (= [1 2 3]
           (reduce-values-from-reader (io/reader byte-array) conj [])))

    (is (= [1 2]
           (reduce-values-from-reader (io/reader byte-array)
                                      ((take 2) conj)
                                      [])))))
