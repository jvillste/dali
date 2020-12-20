(ns argumentica.fressian
  (:require [clojure.data.fressian :as fressian]
            [clojure.test :refer :all]
            [medley.core :as medley]
            [clojure.set :as set])
  (:import java.io.ByteArrayOutputStream
           org.fressian.handlers.WriteHandler
           org.fressian.handlers.ReadHandler
           java.io.ByteArrayInputStream
           org.fressian.Writer
           org.fressian.Reader
           org.fressian.impl.RawOutput
           org.fressian.impl.RawInput
           java.nio.ByteBuffer))

(defn hexify [number]
  (format "%02x" number))

(deftest test-hexify
  (is (= "01"
         (hexify 1)))
  (is (= "10"
         (hexify 16))))

(defn unhexify [hex]
  (Integer/parseInt hex 16))

(def write-handlers
  {java.lang.Integer
   {"compressed"
    (reify WriteHandler
      (write [_ w integer]
        (.writeTag w "compressed" 1)
        (.writeInt w integer)))}})


(def dictionary-reference-code (unhexify "b6"))
(def unqualified-keyword-code (unhexify "b7"))
(def qualified-keyword-code (unhexify "b8"))

(def read-handlers
  "Standard set of read handlers for Clojure data."
  {"c"
   (reify ReadHandler
     (read [_ rdr tag component-count]
       ({1 "a" 2 "b"} (.readObject rdr))))})

(defn initialize-output []
  (let [byte-array-output-stream (ByteArrayOutputStream.)]
    {:writer (fressian/create-writer byte-array-output-stream)
     :byte-array-output-stream byte-array-output-stream}))

(defn initialize-input [byte-array]
  (let [byte-array-input-stream (ByteArrayInputStream. byte-array)]
    {:reader (fressian/create-reader byte-array-input-stream)
     :byte-array-input-stream byte-array-input-stream}))

(defn write-value [^Writer writer value]
  (cond (qualified-keyword? value)
        (doto writer
          (.writeCode qualified-keyword-code)
          (.writeObject (namespace value))
          (.writeObject (name value)))

        (keyword? value)
        (doto writer
          (.writeCode unqualified-keyword-code)
          (.writeObject (name value)))

        :default
        (.writeObject writer value)))

(comment
  (namespace :a/b)
  ) ;; TODO: remove-me

(defn write [values-to-codes value output]
  (if-let [code (get values-to-codes value)]
    (doto ^Writer (:writer output)
      (.writeCode dictionary-reference-code)
      (.writeObject code))
    (write-value (:writer output)
                 value)))

(defn create-test-byte-array [dictionary value]
  (let [output (initialize-output)]
    (write dictionary value output)
    (vec (map hexify (.toByteArray (:byte-array-output-stream output))))))

(deftest test-write
  (is (= ["01"]
         (create-test-byte-array {} 1)))

  (is (= ["db" "61"]
         (create-test-byte-array {} "a")))

  (is (= ["b7" "db" "61"]
         (create-test-byte-array {} :a)))

  (is (= ["b8" "db" "61" "db" "62"]
         (create-test-byte-array {} :a/b)))

  (is (= ["b6" "00"]
         (create-test-byte-array {"a" 0} "a"))))

(defn- read-with-code [code ^Reader reader ^ByteArrayInputStream byte-array-input-stream]
  (case code
    184 ;; qualified-keyword-code
    (keyword (.readObject reader)
             (.readObject reader))

    183 ;; unqualified-keyword-code
    (keyword (.readObject reader))

    (do (.reset byte-array-input-stream)
        (.readObject reader))))

(defn read-uncompressed-value [input]
  (let [^ByteArrayInputStream byte-array-input-stream (:byte-array-input-stream input)]
    (.mark byte-array-input-stream 0)
    (read-with-code (.read byte-array-input-stream)
                    (:reader input)
                    byte-array-input-stream)))

(defn values-to-codes [values]
  (->> values
       frequencies
       (filter (fn [[value count]]
                 (and (< 1 (.limit (fressian/write value)))
                      (< 1 count))))
       (map (comp vec reverse))
       (sort-by identity #(compare %2 %1))
       (map-indexed (fn [index [_count value]]
                      [value index]))
       (into {})))

(deftest test-values-to-codes
  (is (= {}
         (values-to-codes [:a])))

  (is (= {:a 0}
         (values-to-codes [:a :a :b])))

  (is (= {:b 0, :a 1}
         (values-to-codes [:a :a :b :b :b]))))


(defn write-dictionary [values]
  (let [index-byte-array-output-stream (ByteArrayOutputStream.)
        index-raw-output (RawOutput. index-byte-array-output-stream)
        values-output (initialize-output)]
    (doseq [value values]
      (.writeRawInt16 index-raw-output (.size (:byte-array-output-stream values-output)))
      (write-value (:writer values-output)
                   value))
    {:index (.toByteArray index-byte-array-output-stream)
     :rows (.toByteArray (:byte-array-output-stream values-output))}))

(defn open-dictionary [dictionary]
  {:index-short-buffer (.asShortBuffer (ByteBuffer/wrap (:index dictionary)))
   :rows-input (initialize-input (:rows dictionary))})

(defn value-from-dictionary [value-number open-dictionary]
  (doto (-> open-dictionary :rows-input :byte-array-input-stream)
    (.reset)
    (.skip (.get (:index-short-buffer open-dictionary)
                 value-number)))
  (read-uncompressed-value (:rows-input open-dictionary)))

(deftest test-value-from-dictionary
  (is (= :b
         (value-from-dictionary 1
                                (open-dictionary (write-dictionary [:a :b :c]))))))


(defn values-to-codes-to-sorted-values [values-to-codes]
  (map first (sort-by second values-to-codes)))

(deftest test-values-to-codes-to-sorted-values
  (is (= [:b :a]
         (values-to-codes-to-sorted-values {:a 1 :b 0}))))

(defn read [open-dictionary input]
  (let [^Reader reader (:reader input)
        ^ByteArrayInputStream byte-array-input-stream (:byte-array-input-stream input)]
    (.mark byte-array-input-stream 0)
    (let [code (.read byte-array-input-stream)]
      (if (= code dictionary-reference-code)
        (value-from-dictionary (fressian/read-object reader)
                               open-dictionary)
        (read-with-code code reader byte-array-input-stream)))))

(defn- round-trip-compression [values-to-codes value]
  (let [output (initialize-output)
        the-open-dictionary (->> values-to-codes
                                 values-to-codes-to-sorted-values
                                 write-dictionary
                                 open-dictionary)]
    (write values-to-codes value output)
    (->> (.toByteArray (:byte-array-output-stream output))
         (initialize-input)
         (read the-open-dictionary))))

(deftest test-read
  (is (= "foo"
         (read (open-dictionary (write-dictionary ["foo"]))
               (initialize-input (byte-array (map unhexify ["b6" "00"]))))))

  (is (= "foo"
         (round-trip-compression {"foo" 0} "foo")))

  (is (= :a
         (round-trip-compression {} :a)))

  (is (= :a/b
         (round-trip-compression {} :a/b)))

  (is (= ["foo" "bar"]
         (let [values-to-codes {"foo" 0}
               open-dictionary (->> values-to-codes
                                    values-to-codes-to-sorted-values
                                    write-dictionary
                                    open-dictionary)
               input (->> (let [output (initialize-output)]
                            (write values-to-codes "foo" output)
                            (write values-to-codes "bar" output)
                            (.toByteArray (:byte-array-output-stream output)))
                          (initialize-input))]
           [(read open-dictionary input)
            (read open-dictionary input)]))))

(defn write-rows [rows]
  (let [index-byte-array-output-stream (ByteArrayOutputStream.)
        index-raw-output (RawOutput. index-byte-array-output-stream)
        values-output (initialize-output)
        values-to-codes (values-to-codes (apply concat rows))]
    (doseq [row rows]
      (.writeRawInt16 index-raw-output (.size (:byte-array-output-stream values-output)))
      (doseq [value row]
        (write values-to-codes
               value
               values-output)))
    {:index (.toByteArray index-byte-array-output-stream)
     :row-length (count (first rows))
     :rows (.toByteArray (:byte-array-output-stream values-output))
     :dictionary (write-dictionary (values-to-codes-to-sorted-values values-to-codes) )}))

(defn byte-array-values-to-vectors [the-map]
  (medley/map-vals (fn [value]
                     (if (bytes? value)
                       (vec (map hexify value))
                       value))
                   the-map))

(deftest test-byte-array-values-to-vectors
  (is (= {:a ["61"]}
         (byte-array-values-to-vectors {:a (.getBytes "a")}))))

(defn rows-to-test-result [rows]
  (byte-array-values-to-vectors (update rows
                                        :dictionary byte-array-values-to-vectors)))

(deftest test-write-rows
  (is (= {:index ["00" "00"],
          :row-length 1,
          :rows ["01"],
          :dictionary {:index [], :rows []}}
         (rows-to-test-result (write-rows [[1]]))))

  (is (= {:index ["00" "00"],
          :row-length 2,
          :rows ["01" "02"],
          :dictionary {:index [], :rows []}}
         (rows-to-test-result (write-rows [[1 2]]))))

  (is (= {:index ["00" "00" "00" "01"],
          :row-length 1,
          :rows ["01" "02"],
          :dictionary {:index [], :rows []}}
         (rows-to-test-result (write-rows [[1]
                                           [2]]))))

  (is (= {:index ["00" "00" "00" "01"],
          :row-length 1,
          :rows ["01" "01"],
          :dictionary {:index [], :rows []}}
         (rows-to-test-result (write-rows [[1]
                                           [1]]))))

  (is (= {:index ["00" "00" "00" "02"],
          :row-length 1,
          :rows ["b6" "00" "b6" "00"],
          :dictionary {:index ["00" "00"],
                       :rows ["b7" "db" "61"]}}
         (rows-to-test-result (write-rows [[:a]
                                           [:a]])))))


(defn reduce-rows [row-number direction reducing-function initial-value node]
  (let [index-short-buffer (.asShortBuffer (ByteBuffer/wrap (:index node)))
        rows-input (initialize-input (:rows node))
        open-dictionary (open-dictionary (:dictionary node))]
    (let [offset (.get index-short-buffer row-number)]
      (.skip (:byte-array-input-stream rows-input) offset)
      (loop [reduced-value initial-value]
        (if (< 0 (.available (:byte-array-input-stream rows-input)))
          (let [reduced-value (reducing-function reduced-value
                                                 (read open-dictionary
                                                       rows-input))]
            (if (reduced? reduced-value)
              (reducing-function @reduced-value)
              (recur reduced-value)))
          (reducing-function reduced-value))))))

(deftest test-reduce-rows
  (is (= [2 3]
         (reduce-rows 1
                      :forwards
                      conj
                      []
                      (write-rows [[1] [2] [3]]))))

  (is (= [:a/b 3]
         (reduce-rows 1
                      :forwards
                      conj
                      []
                      (write-rows [[:a/b] [:a/b] [3]])))))

(comment

  (.toString (fressian/write "b"))
  (map hexify (.array (fressian/write :a)))
  (map hexify (.array (fressian/write "a")))


  (fressian/read (fressian/write [1 2])
                 :handlers (fressian/associative-lookup {}))

  (map hexify (write-test-bytes))
  (initialize-input (write-test-bytes))

  (read (write-test-bytes {}))


  (fressian/read (write-test-bytes)
                 :handlers (fressian/associative-lookup read-handlers))

  ;;("ef" "db" "63" "1" "1")
  ;;("ef" "db" "63" "1" "1" "a0" "1")
  ;;("ef" "db" "63" "1" "1" "a0" "2")


  ) ;; TODO: remove-me
