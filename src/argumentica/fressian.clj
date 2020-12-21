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
           java.nio.ByteBuffer
           java.io.InputStream))

(defn hexify [number]
  (format "%02x" number))

(deftest test-hexify
  (is (= "01"
         (hexify 1)))
  (is (= "10"
         (hexify 16))))

(defn unhexify [hex]
  (get (byte-array [(Integer/parseInt hex 16)]) 0))

(defn unsinged-byte [byte]
  (bit-and byte 0xFF))

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

;; (defn decrement-byte-buffer-position [^ByteBuffer byte-buffer]
;;   (.position byte-buffer
;;              (dec (.position byte-buffer))))

;; (deftest test-decrement-byte-buffer-position
;;   (is (= [10 11 11]
;;          (let [byte-buffer (ByteBuffer/wrap (byte-array [10 11 12]))
;;                values (atom [])]
;;            (swap! values conj (.get byte-buffer))
;;            (swap! values conj (.get byte-buffer))
;;            (decrement-byte-buffer-position byte-buffer)
;;            (swap! values conj (.get byte-buffer))
;;            @values))))

;; (defn decrement-position [byte-buffer-reader]
;;   (decrement-byte-buffer-position (:byte-buffer byte-buffer-reader)))

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

#_(defn- read-with-code [code ^Reader reader ^ByteArrayInputStream byte-array-input-stream]
  (case code
    184 ;; qualified-keyword-code
    (keyword (.readObject reader)
             (.readObject reader))

    183 ;; unqualified-keyword-code
    (keyword (.readObject reader))

    (do (.position (byte-buffer)) (.reset byte-array-input-stream)
        (.readObject reader))))

(comment
  (hexify -73)
  (set! *warn-on-reflection* false)
  ) ;; TODO: remove-me


#_(defn- read-with-code [byte-buffer-reader]
  (case (peek-byte byte-buffer-reader)
    184 ;; qualified-keyword-code
    (keyword (read-object byte-buffer-reader)
             (read-object byte-buffer-reader))

    183 ;; unqualified-keyword-code
    (keyword (read-object byte-buffer-reader))

    (read-object byte-buffer-reader)))

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
      (.writeRawInt16 index-raw-output (.size (:byte-array-output-stream values-output)))
      (write-value (:writer values-output)
                   value))
    {:index (.toByteArray index-byte-array-output-stream)
     :rows (.toByteArray (:byte-array-output-stream values-output))}))

(defn open-dictionary [dictionary]
  {:index-short-buffer (.asShortBuffer (ByteBuffer/wrap (:index dictionary)))
   :rows-byte-buffer-reader (byte-buffer-reader (:rows dictionary))})

(defn value-from-dictionary [value-number open-dictionary]
  (.position (-> open-dictionary :rows-byte-buffer-reader :byte-buffer)
             (.get (:index-short-buffer open-dictionary)
                   value-number))
  (read-uncompressed-value (:rows-byte-buffer-reader open-dictionary)))

(comment
  (hexify unqualified-keyword-code)
  ) ;; TODO: remove-me

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
               (byte-buffer-reader (byte-array (map unhexify ["b6" "00"]))))))

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
        rows-byte-buffer-reader (byte-buffer-reader (:rows node))
        open-dictionary (open-dictionary (:dictionary node))]
    (loop [reduced-value initial-value
           row-number row-number]
      (if (and (< row-number (.limit index-short-buffer))
               (<= 0 row-number))
        (do (set-position (.get index-short-buffer row-number)
                          rows-byte-buffer-reader)
            (let [reduced-value (reducing-function reduced-value
                                                   (read open-dictionary
                                                         rows-byte-buffer-reader))]
              (if (reduced? reduced-value)
                (reducing-function @reduced-value)
                (recur reduced-value
                       (if (= :forwards direction)
                         (inc row-number)
                         (dec row-number))))))
        (reducing-function reduced-value)))))

(deftest test-reduce-rows
  (is (= [2 3]
         (reduce-rows 1
                      :forwards
                      conj
                      []
                      (write-rows [[1] [2] [3]]))))

  (is (= [2 1]
         (reduce-rows 1
                      :backwards
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
  (byte-buffer-reader (write-test-bytes))

  (read (write-test-bytes {}))


  (fressian/read (write-test-bytes)
                 :handlers (fressian/associative-lookup read-handlers))

  ;;("ef" "db" "63" "1" "1")
  ;;("ef" "db" "63" "1" "1" "a0" "1")
  ;;("ef" "db" "63" "1" "1" "a0" "2")


  ) ;; TODO: remove-me
