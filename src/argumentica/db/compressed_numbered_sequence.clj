(ns argumentica.db.compressed-numbered-sequence
  (:require [argumentica.reduction :as reduction]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [taoensso.nippy :as nippy]
            [argumentica.util :as util])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream DataInputStream DataOutputStream EOFException]))

(defn- write-chunk-metadata [data-output-stream compressed-chunk-size first-number value-count]
  (.writeInt data-output-stream compressed-chunk-size)
  (.writeInt data-output-stream first-number)
  (.writeInt data-output-stream value-count))

(defn- read-chunk-metadata [data-input-stream]
  (try
    {:length (.readInt data-input-stream)
     :first-number (.readInt data-input-stream)
     :value-count (.readInt data-input-stream)}
    (catch EOFException exception_
      nil)))

(defn write-values-to-stream! [output-stream first-number values]
  (let [data-output-stream (DataOutputStream. output-stream)
        byte-array (nippy/freeze values)
        compressed-chunk-size (alength byte-array)]
    (write-chunk-metadata data-output-stream
                          compressed-chunk-size
                          first-number
                          (count values))
    (.write data-output-stream
            byte-array
            0
            compressed-chunk-size)))

(defn- create-test-input-stream [& chunks]
  (let [create-byte-array (fn [function]
                            (with-open [byte-array-output-stream (ByteArrayOutputStream.)
                                        data-output-stream (DataOutputStream. byte-array-output-stream)]
                              (function data-output-stream)
                              (.toByteArray byte-array-output-stream)))]
    (ByteArrayInputStream. (create-byte-array (fn [data-output-stream]
                                                (doseq [[number values] (partition 2 chunks)]
                                                  (write-values-to-stream! data-output-stream number values)))))))

(defn- read-values [data-input-stream chunk-length]
  (let [buffer (byte-array chunk-length)]
    (.read data-input-stream buffer)
    (nippy/thaw buffer)))

(defn- reduce-chunk-metadata [data-input-stream reducing-function initial-value]
  (loop [value initial-value]
    (if-let [chunk-metadata (read-chunk-metadata data-input-stream)]
      (let [result (reducing-function value chunk-metadata)]
        (if (reduced? result)
          @result
          (do (.skipBytes data-input-stream
                          (:length chunk-metadata))
              (recur result))))
      value)))

(deftest test-reduce-chunk-metadata
  (is (= [{:length 11, :first-number 10, :value-count 3}
          {:length 11, :first-number 13, :value-count 3}]
         (reduce-chunk-metadata (DataInputStream. (create-test-input-stream 10 [10 11 12]
                                                                            13 [13 14 15]))
                                conj
                                []))))

(defn- seek-chunk! [data-input-stream number]
  (reduce-chunk-metadata data-input-stream
                         ((comp (drop-while (fn [chunk-metadata]
                                              (< (+ (:value-count chunk-metadata)
                                                    (:first-number chunk-metadata))
                                                 number)))
                                (take 1))
                          reduction/last-value)
                         #_(reduction/find-first (fn [chunk-metadata]
                                                   (> (+ (:value-count chunk-metadata)
                                                         (:first-number chunk-metadata))
                                                      number)))
                         nil))

(deftest test-seek-chunk!
  (is (= {:length 11, :first-number 10, :value-count 3}
         (seek-chunk! (DataInputStream. (create-test-input-stream 10 [10 11 12]
                                                                  13 [13 14 15]))
                      0))))

(defn reduce-values-from-stream [input-stream reducing-function initial-reduced-value first-number]
  (let [data-input-stream (DataInputStream. input-stream)
        reducing-function (reduction/double-reduced reducing-function)]
    (loop [reduced-value initial-reduced-value
           chunk-metadata (seek-chunk! data-input-stream first-number)]
      (if chunk-metadata
        (let [reduced-value (reduce reducing-function
                                    reduced-value
                                    (drop (- first-number (:first-number chunk-metadata))
                                          (read-values data-input-stream (:length chunk-metadata))))]
          (if (reduced? reduced-value)
            @reduced-value
            (recur reduced-value
                   (read-chunk-metadata data-input-stream))))
        reduced-value))))

(deftest test-reduce-values-from-stream
  (is (= [10 11 12 13 14 15]
         (reduce-values-from-stream (create-test-input-stream 10 [10 11 12]
                                                              13 [13 14 15])
                                    conj
                                    []
                                    0)))

  (is (= [12 13 14 15]
         (reduce-values-from-stream (create-test-input-stream 10 [10 11 12]
                                                              13 [13 14 15])
                                    conj
                                    []
                                    12)))

  (is (= [13 14 15]
         (reduce-values-from-stream (create-test-input-stream 10 [10 11 12]
                                                              13 [13 14 15])
                                    conj
                                    []
                                    13)))

  (is (= [12]
         (reduce-values-from-stream (create-test-input-stream 10 [10 11 12]
                                                              13 [13 14 15])
                                    ((take 1) conj)
                                    []
                                    12)))

  (is (= [12 13]
         (reduce-values-from-stream (create-test-input-stream 10 [10 11 12]
                                                              13 [13 14 15])
                                    ((take 2)
                                     conj)
                                    []
                                    12)))

  (is (= [10 11]
         (reduce-values-from-stream (create-test-input-stream 10 [10 11 12]
                                                              13 [13 14 15])
                                    ((take 2) conj)
                                    []
                                    10)))

  (testing "Reduction is not completed"
    (is (= [[10 11]]
           (reduce-values-from-stream (create-test-input-stream 10 [10 11 12]
                                                                13 [13 14 15])
                                      ((comp (take 3)
                                             (partition-all 2))
                                       conj)
                                      []
                                      10)))

    (is (= [[10 11]]
           (reduce-values-from-stream (create-test-input-stream 10 [10 11 12])
                                      ((partition-all 2)
                                       conj)
                                      []
                                      10)))))


(comment
  (with-open [output-stream (io/output-stream "temp/test-compressed-sequence")]
    (write-values-to-stream! output-stream 0 [1 2])
    (write-values-to-stream! output-stream 2 [1 2]))

  (with-open [input-stream (io/input-stream "temp/test-compressed-sequence")]
    (seek-chunk! (DataInputStream. input-stream) 0))

  (with-open [input-stream (io/input-stream #_"temp/test-compressed-sequence"
                                            "/Users/jukka/google-drive/src/nucalc/temp/food_nutrient/transaction-log/archive")]
    (reduce-chunk-metadata (DataInputStream. input-stream) conj []))

  (with-open [input-stream (io/input-stream #_"temp/test-compressed-sequence"
                                            "/Users/jukka/google-drive/src/nucalc/temp/food_nutrient/transaction-log/archive")]
    (reduce-values-from-stream input-stream ((take 2) conj) [] 1000))

  ) ;; TODO: remove-me
