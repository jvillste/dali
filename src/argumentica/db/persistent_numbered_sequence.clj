(ns argumentica.db.persistent-numbered-sequence
  (:require [argumentica.db.persistent-sequence :as persistent-sequence]
            [clojure.java.io :as io]
            [clojure.test :refer :all])
  (:import java.io.ByteArrayOutputStream))

(defn write-to-stream! [output-stream value]
  (persistent-sequence/write-to-stream! output-stream value))

(defn initialize-numbered-sequence [output-stream first-number]
  (persistent-sequence/write-to-stream! output-stream first-number))

(defn first-number-from-reader [^java.io.BufferedReader reader]
  (read-string (.readLine reader)))

(defn reduce-values-from-reader [^java.io.BufferedReader reader reducing-function initial-reduced-value starting-number]
  (let [first-number (first-number-from-reader reader)]
    (persistent-sequence/reduce-values-from-reader reader
                                                   ((drop (- starting-number first-number)) reducing-function)
                                                   initial-reduced-value)))

(deftest test-reduce-values-from-reader
  (let [byte-array (.toByteArray (doto (ByteArrayOutputStream.)
                                   (initialize-numbered-sequence 5)
                                   (write-to-stream! 1)
                                   (write-to-stream! 2)
                                   (write-to-stream! 3)))]
    (is (= [1 2 3]
           (reduce-values-from-reader (io/reader byte-array)
                                      conj
                                      []
                                      5)))

    (is (= [2 3]
           (reduce-values-from-reader (io/reader byte-array)
                                      conj
                                      []
                                      6)))

    (is (= [2]
           (reduce-values-from-reader (io/reader byte-array)
                                      ((take 1) conj)
                                      []
                                      6)))))
