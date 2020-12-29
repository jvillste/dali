(ns argumentica.leaf-node-serialization
  (:require [clojure.data.fressian :as fressian]
            [clojure.test :refer :all]
            [medley.core :as medley]
            [clojure.set :as set]
            [argumentica.comparator :as comparator]
            [argumentica.util :as util]
            [argumentica.reduction :as reduction])
  (:import java.io.ByteArrayOutputStream
           org.fressian.handlers.WriteHandler
           org.fressian.handlers.ReadHandler
           java.io.ByteArrayInputStream
           org.fressian.Writer
           org.fressian.Reader
           org.fressian.impl.RawOutput
           org.fressian.impl.RawInput
           java.nio.ByteBuffer
           java.io.InputStream))


(defn unsinged-byte [byte]
  (bit-and byte 0xFF))

(def write-handlers
  {java.lang.Integer
   {"compressed"
    (reify WriteHandler
      (write [_ w integer]
        (.writeTag w "compressed" 1)
        (.writeInt w integer)))}})

(def dictionary-reference-code (util/unhexify "b6"))
(def unqualified-keyword-code (util/unhexify "b7"))
(def qualified-keyword-code (util/unhexify "b8"))

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

(defn byte-buffer-input-stream [byte-buffer]
  (proxy [InputStream] []
    (read
      ([] (unsinged-byte (.get byte-buffer)))
      ([target-byte-array offset length]
       (if (= 0 (.remaining byte-buffer))
         -1
         (let [length (min (.remaining byte-buffer)
                           length)]
           (.get byte-buffer
                 target-byte-array
                 offset
                 (min (.remaining byte-buffer)
                      length))
           length))))))

(deftest test-byte-buffer-input-stream
  (is (= 11
         (let [byte-buffer (ByteBuffer/wrap (byte-array [10 11 12]))
               byte-buffer-input-stream ^InputStream (byte-buffer-input-stream byte-buffer)]
           (.position byte-buffer 1)
           (.read byte-buffer-input-stream))))


  (is (= [10, 11]
         (let [byte-buffer (ByteBuffer/wrap (byte-array [10 11]))
               target-byte-array (byte-array 2)
               byte-buffer-input-stream ^InputStream (byte-buffer-input-stream byte-buffer)]
           (.read byte-buffer-input-stream
                  target-byte-array
                  0
                  2)
           (vec target-byte-array)))))

(defn byte-buffer-reader [byte-array]
  (let [byte-buffer (ByteBuffer/wrap byte-array)]
    {:reader (fressian/create-reader (byte-buffer-input-stream byte-buffer))
     :byte-buffer byte-buffer}))

(defn read-object [byte-buffer-reader]
  (.readObject ^Reader (:reader byte-buffer-reader)))

(defn set-position [position byte-buffer-reader]
  (.position ^ByteBuffer (:byte-buffer byte-buffer-reader)
             position))

(defn increment-position [byte-buffer-reader]
  (let [byte-buffer ^ByteBuffer (:byte-buffer byte-buffer-reader)]
    (.position byte-buffer
               (inc (.position byte-buffer)))))

(defn peek-byte [byte-buffer-reader]
  (let [byte-buffer ^ByteBuffer (:byte-buffer byte-buffer-reader)]
    (.get byte-buffer
          (.position byte-buffer))))

(deftest test-peek-byte
  (is (= [10 10]
         (let [byte-buffer (byte-buffer-reader (byte-array [10]))
               values (atom [])]
           (swap! values conj (peek-byte byte-buffer))
           (swap! values conj (peek-byte byte-buffer))
           @values))))

(defn has-bytes-left? [byte-buffer-reader]
  (.hasRemaining ^ByteBuffer (:byte-buffer byte-buffer-reader)))

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
    (vec (map util/hexify (.toByteArray (:byte-array-output-stream output))))))

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


(defn read-uncompressed-value [byte-buffer-reader]
  (case (peek-byte byte-buffer-reader)
    -72 ;; qualified-keyword-code
    (do (increment-position byte-buffer-reader)
        (keyword (read-object byte-buffer-reader)
                 (read-object byte-buffer-reader)))

    -73 ;; unqualified-keyword-code
    (do (increment-position byte-buffer-reader)
        (keyword (read-object byte-buffer-reader)))

    (read-object byte-buffer-reader)))

(defn values-to-codes [values]
  (->> values
       frequencies
       (filter (fn [[value count]]
                 (and (< 1 (.limit (fressian/write value))) ;; TODO: we write everything twice because of this check, some faster heuristic might be enough
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
      (.writeRawInt24 index-raw-output
                      (.size (:byte-array-output-stream values-output)))
      (write-value (:writer values-output)
                   value))
    {:index (.toByteArray index-byte-array-output-stream)
     :rows (.toByteArray (:byte-array-output-stream values-output))}))

(defn open-dictionary [dictionary]
  (let [index-byte-buffer (ByteBuffer/wrap (:index dictionary))]
    {;;:index-short-buffer (.asShortBuffer (ByteBuffer/wrap (:index dictionary)))
     :index-byte-buffer index-byte-buffer
     :index-raw-input (RawInput. (byte-buffer-input-stream index-byte-buffer))
     :rows-byte-buffer-reader (byte-buffer-reader (:rows dictionary))}))

(def index-pointer-size 3)

(defn value-from-dictionary [value-number open-dictionary]
  (.position (:index-byte-buffer open-dictionary)
             (* value-number
                index-pointer-size))
  (.position (-> open-dictionary
                 :rows-byte-buffer-reader
                 :byte-buffer)
             (.readRawInt24 (:index-raw-input open-dictionary)))
  (read-uncompressed-value (:rows-byte-buffer-reader open-dictionary)))


(deftest test-value-from-dictionary
  (is (= :b
         (value-from-dictionary 1
                                (open-dictionary (write-dictionary [:a :b :c]))))))

(defn values-to-codes-to-sorted-values [values-to-codes]
  (map first (sort-by second values-to-codes)))

(deftest test-values-to-codes-to-sorted-values
  (is (= [:b :a]
         (values-to-codes-to-sorted-values {:a 1 :b 0}))))

(defn read [open-dictionary byte-buffer-reader]
  (let [code (peek-byte byte-buffer-reader)]
    (if (= code dictionary-reference-code)
      (value-from-dictionary (do (increment-position byte-buffer-reader)
                                 (read-object byte-buffer-reader))
                             open-dictionary)
      (read-uncompressed-value byte-buffer-reader))))

(defn- round-trip-compression [values-to-codes value]
  (let [output (initialize-output)
        the-open-dictionary (->> values-to-codes
                                 values-to-codes-to-sorted-values
                                 write-dictionary
                                 open-dictionary)]
    (write values-to-codes value output)
    (->> (.toByteArray (:byte-array-output-stream output))
         (byte-buffer-reader)
         (read the-open-dictionary))))

(deftest test-read
  (is (= "foo"
         (read (open-dictionary (write-dictionary ["foo"]))
               (byte-buffer-reader (byte-array (map util/unhexify ["b6" "00"]))))))

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
                          (byte-buffer-reader))]
           [(read open-dictionary input)
            (read open-dictionary input)]))))

(defn write-node [rows]
  (let [index-byte-array-output-stream (ByteArrayOutputStream.)
        index-raw-output (RawOutput. index-byte-array-output-stream)
        values-output (initialize-output)
        values-to-codes (values-to-codes (if (vector? (first rows))
                                           (apply concat rows)
                                           rows))]
    (doseq [row rows]
      (.writeRawInt24 index-raw-output
                      (int (.size (:byte-array-output-stream values-output))))
      (if (vector? row)
        (doseq [value row]
          (write values-to-codes
                 value
                 values-output))
        (write values-to-codes
               row
               values-output)))
    {:index (.toByteArray index-byte-array-output-stream)
     :row-length (if (vector? (first rows))
                   (count (first rows))
                   0)
     :rows (.toByteArray (:byte-array-output-stream values-output))
     :dictionary (write-dictionary (values-to-codes-to-sorted-values values-to-codes) )}))


(deftest test-write-rows
  (is (= {:index ["00" "00" "00"],
          :row-length 0,
          :rows ["01"],
          :dictionary {:index [], :rows []}}
         (util/byte-array-values-to-vectors (write-node [1]))))

  (is (= {:index ["00" "00" "00"],
          :row-length 1,
          :rows ["01"],
          :dictionary {:index [], :rows []}}
         (util/byte-array-values-to-vectors (write-node [[1]]))))

  (is (= {:index ["00" "00" "00"],
          :row-length 2,
          :rows ["01" "02"],
          :dictionary {:index [], :rows []}}
         (util/byte-array-values-to-vectors (write-node [[1 2]]))))

  (is (= {:index ["00" "00" "00" "00" "00" "01"],
          :row-length 1,
          :rows ["01" "02"],
          :dictionary {:index [], :rows []}}
         (util/byte-array-values-to-vectors (write-node [[1]
                                                         [2]]))))

  (is (= {:index ["00" "00" "00" "00" "00" "01"],
          :row-length 1,
          :rows ["01" "01"],
          :dictionary {:index [], :rows []}}
         (util/byte-array-values-to-vectors (write-node [[1]
                                                         [1]]))))

  (is (= {:index ["00" "00" "00" "00" "00" "02"],
          :row-length 1,
          :rows ["b6" "00" "b6" "00"],
          :dictionary {:index ["00" "00"],
                       :rows ["b7" "db" "61"]}}
         (util/byte-array-values-to-vectors (write-node [[:a]
                                                         [:a]])))))


(defn read-row [row-length open-dictionary rows-byte-buffer-reader]
  (if (= 0 row-length)
    (read open-dictionary
          rows-byte-buffer-reader)
    (vec (repeatedly row-length
                     (fn []
                       (read open-dictionary
                             rows-byte-buffer-reader))))))

(defn raw-byte-buffer [byte-array]
  (let [byte-buffer (ByteBuffer/wrap byte-array)]
    {:byte-buffer byte-buffer
     :raw-input (RawInput. (byte-buffer-input-stream byte-buffer))}))

(defn read-raw-int-24 [raw-byte-buffer index]
  (.position (:byte-buffer raw-byte-buffer)
             (* index 3))
  (.readRawInt24 (:raw-input raw-byte-buffer)))


(defn reduce-values-from-row-number [row-number direction reducing-function initial-value node]
  (let [index-raw-byte-buffer (raw-byte-buffer (:index node))
        rows-byte-buffer-reader (byte-buffer-reader (:rows node))
        open-dictionary (open-dictionary (:dictionary node))]

    (if (= :forwards direction)

      (do (set-position (read-raw-int-24 index-raw-byte-buffer
                                         row-number)
                        rows-byte-buffer-reader)
          (loop [reduced-value initial-value]
            (if (has-bytes-left? rows-byte-buffer-reader)
              (let [reduced-value (reducing-function reduced-value
                                                     (read-row (:row-length node)
                                                               open-dictionary
                                                               rows-byte-buffer-reader))]
                (if (reduced? reduced-value)
                  (reducing-function @reduced-value)
                  (recur reduced-value)))
              (reducing-function reduced-value))))

      (loop [reduced-value initial-value
             row-number row-number]
        (if (<= 0 row-number)
          (do (set-position (read-raw-int-24 index-raw-byte-buffer
                                             row-number)
                            rows-byte-buffer-reader)
              (let [reduced-value (reducing-function reduced-value
                                                     (read-row (:row-length node)
                                                               open-dictionary
                                                               rows-byte-buffer-reader))]
                (if (reduced? reduced-value)
                  (reducing-function @reduced-value)
                  (recur reduced-value
                         (if (= :forwards direction)
                           (inc row-number)
                           (dec row-number))))))
          (reducing-function reduced-value))))))

(deftest test-reduce-values
  (is (= [2 3]
         (reduce-values-from-row-number 1
                                        :forwards
                                        conj
                                        []
                                        (write-node [1 2 3]))))

  (is (= [[2 2] [3 2]]
         (reduce-values-from-row-number 1
                                        :forwards
                                        conj
                                        []
                                        (write-node [[1 2] [2 2] [3 2]]))))

  (is (= [[2] [1]]
         (reduce-values-from-row-number 1
                                        :backwards
                                        conj
                                        []
                                        (write-node [[1] [2] [3]]))))

  (is (= [[:a/b] [3]]
         (reduce-values-from-row-number 1
                                        :forwards
                                        conj
                                        []
                                        (write-node [[:a/b] [:a/b] [3]])))))

(defn row-count [node]
  (/ (alength (:index node))
     index-pointer-size))

(defn binary-search [greatest-number compare-to-target-number]
  (loop [lesser-number nil
         greater-number nil
         number (quot greatest-number
                      2)]
    (let [comparison-result (compare-to-target-number number)]
      (cond (> comparison-result 0)
            (cond (= lesser-number
                     (dec number))
                  {:greater-than lesser-number
                   :lesser-than number}

                  (= 0 number)
                  {:lesser-than 0}

                  :default
                  (recur lesser-number
                         number
                         (quot (+ number
                                  (or lesser-number
                                      0))
                               2)))

            (< comparison-result 0)
            (cond (= greater-number
                     (inc number))
                  {:greater-than number
                   :lesser-than greater-number}

                  (= greatest-number number)
                  {:greater-than greatest-number}

                  :default
                  (recur number
                         greater-number
                         (inc (quot (+ number
                                       (or greater-number
                                           greatest-number))
                                    2))))

            (= comparison-result 0)
            {:equal-to number}))))

(deftest test-binary-search

  (testing "middle number"
    (is (= {:equal-to 1}
           (binary-search 2
                          #(compare % 1)))))

  (testing "lesser than middle number"
    (is (= {:equal-to 3}
           (binary-search 10
                          #(compare % 3)))))

  (testing "greater than middle number"
    (is (= {:equal-to 7}
           (binary-search 10
                          #(compare % 7)))))

  (testing "smallest number"
    (is (= {:equal-to 0}
           (binary-search 10
                          #(compare % 0)))))

  (testing "greatest number"
    (is (= {:equal-to 10}
           (binary-search 10
                          #(compare % 10)))))

  (testing "between"
    (is (= {:greater-than 0
            :lesser-than 1}
           (binary-search 1
                          #(compare % 0.5)))))

  (testing "between lots of numbers, because it's cool to have lots of numbers"
    (is (= {:greater-than 30
            :lesser-than 31}
           (binary-search 100
                          #(compare %
                                    30.5)))))

  (testing "lesser than all of the numbers"
    (is (= {:lesser-than 0}
           (binary-search 2
                          #(compare % -1)))))

  (testing "greater than all of the numbers"
    (is (= {:greater-than 2}
           (binary-search 2
                          #(compare % 3))))))



(defn greater-or-equal [binary-search-result]
  (or (:greater-than binary-search-result)
      (:equal-to binary-search-result)))

(defn lesser-or-equal [binary-search-result]
  (or (:lesser-than binary-search-result)
      (:equal-to binary-search-result)))

(defn binary-search-node [target-row node]
  (let [index-raw-byte-buffer (raw-byte-buffer (:index node))
        rows-byte-buffer-reader (byte-buffer-reader (:rows node))
        open-dictionary (open-dictionary (:dictionary node))
        row-count (row-count node)]
    (when (> row-count 0)
     (binary-search (dec row-count)
                    (fn [number]
                      (set-position (read-raw-int-24 index-raw-byte-buffer
                                                     number)
                                    rows-byte-buffer-reader)
                      (comparator/compare-datoms (read-row (:row-length node)
                                                           open-dictionary
                                                           rows-byte-buffer-reader)
                                                 target-row))))))

(defn position-of-smallest-greater-than-or-equal [target-row node]
  (lesser-or-equal (binary-search-node target-row node)))

(deftest test-position-of-smallest-greater-than-or-equal
  (is (= nil
         (position-of-smallest-greater-than-or-equal 2
                                                     (write-node []))))

  (is (= 1
         (position-of-smallest-greater-than-or-equal 2
                                                     (write-node [1 3]))))

  (is (= 0
         (position-of-smallest-greater-than-or-equal 1
                                                     (write-node [1 3]))))

  (is (= 0
         (position-of-smallest-greater-than-or-equal 1
                                                     (write-node [1 2 3 4 5])))))

(defn position-of-greatest-less-than-or-equal [target-row node]
  (greater-or-equal (binary-search-node target-row node)))


(deftest test-position-of-greatest-less-than-or-equal
  (is (= nil
         (position-of-greatest-less-than-or-equal 0
                                                  (write-node [1 2 3 4 5]))))

  (is (= 0
         (position-of-greatest-less-than-or-equal 2
                                                  (write-node [1 3]))))

  (is (= 2
         (position-of-greatest-less-than-or-equal [3]
                                                  (write-node (map (fn [value] [value "foo"])
                                                                   (range 10))))))
  (is (= nil
         (position-of-greatest-less-than-or-equal [0]
                                                  (write-node [[1] [2] [3] [4] [5]]))))
  (is (= 0
         (position-of-greatest-less-than-or-equal [1]
                                                  (write-node [[1] [2] [3] [4] [5]]))))

  (is (= 1
         (position-of-greatest-less-than-or-equal [2]
                                                  (write-node [[1] [2] [3] [4] [5]]))))

  (is (= 2
         (position-of-greatest-less-than-or-equal [3]
                                                  (write-node [[1] [2] [3] [4] [5]]))))

  (is (= 3
         (position-of-greatest-less-than-or-equal [4]
                                                  (write-node [[1] [2] [3] [4] [5]]))))

  (is (= 4
         (position-of-greatest-less-than-or-equal [5]
                                                  (write-node [[1] [2] [3] [4] [5]]))))

  (is (= 4
         (position-of-greatest-less-than-or-equal [6]
                                                  (write-node [[1] [2] [3] [4] [5]])))))


(defn reduce-values [starting-value direction reducing-function initial-value node]
  (if-let [position (if (= :forwards
                           direction)
                      (position-of-smallest-greater-than-or-equal starting-value
                                                                  node)
                      (position-of-greatest-less-than-or-equal starting-value
                                                               node))]
    (reduce-values-from-row-number position
                                   direction
                                   reducing-function
                                   initial-value
                                   node)
    initial-value))


(defn reducible [starting-value direction node]
  (reduction/reducible (fn [reducing-function initial-value]
                         (reduce-values starting-value
                                        direction
                                        reducing-function
                                        initial-value
                                        node))))

(deftest test-reducible
  (is (= [1 2]
         (into []
               (reducible 1
                          :forwards
                          (write-node (range 3))))))

  (is (= [0 1 2]
         (into []
               (reducible -1
                          :forwards
                          (write-node (range 3))))))

  (is (= []
         (into []
               (reducible 10
                          :forwards
                          (write-node (range 3))))))

  (is (= [2 1 0]
         (into []
               (reducible 10
                          :backwards
                          (write-node (range 3))))))

  (is (= []
         (into []
               (reducible -1
                          :backwards
                          (write-node (range 3)))))))


(comment
  (comparator/compare-datoms 1 2)
  (let [output (initialize-output)]
    (fressian/write-object (:writer output)
                           :a)
    (fressian/write-object (:writer output)
                           :a)
    #_(.writeTag (:writer output) "c" 1)
    #_(.writeInt (:writer output) 1)
    (let [bytes (.toByteArray (:byte-array-output-stream output))]
      (prn (map util/hexify bytes))
      (read-uncompressed-value (byte-buffer-reader bytes))))

  (.toString (fressian/write "b"))
  (map util/hexify (.array (fressian/write :a)))
  (map util/hexify (.array (fressian/write "a")))


  (fressian/read (fressian/write [1 2])
                 :handlers (fressian/associative-lookup {}))

  (map util/hexify (write-test-bytes))
  (byte-buffer-reader (write-test-bytes))

  (read (write-test-bytes {}))


  (fressian/read (write-test-bytes)
                 :handlers (fressian/associative-lookup read-handlers))

  ;;("ef" "db" "63" "1" "1")
  ;;("ef" "db" "63" "1" "1" "a0" "1")
  ;;("ef" "db" "63" "1" "1" "a0" "2")


  ) ;; TODO: remove-me
