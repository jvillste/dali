(ns dabric.file
  (:require [helins.binf :as binf]
            [helins.binf.leb128 :as leb128]
            [helins.binf.buffer :as buffer]
            [clojure.test :refer [deftest is]]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [argumentica.encode :as encode])
  (:import java.lang.Byte java.util.UUID java.util.Base64 java.nio.ByteBuffer))
(comment
  (leb128/wr-u64)
  ) ;; TODO: remove-me


(defn wr-date
  [view year month day]
  (-> view
      (binf/wr-b16 year)
      (binf/wr-b8 month)
      (binf/wr-b8 day)))


(defn rr-date
  [view]
  [(binf/rr-u16 view)
   (binf/rr-u8 view)
   (binf/rr-u8 view)])

(comment
  (.getBytes "a" "utf-8")
  ) ;; TODO: remove-me


;; Allocating a buffer of 1024 bytes
;;
(def my-buffer
     (buffer/alloc 1024))

;; Wrapping the buffer in view
;;
(def my-view
     (binf/view my-buffer))

;; The buffer can always be extracted from its view
;;
;; (binf/backing-buffer my-view)

(defn bit-count [number]
  (case number
    0 1
    1 1
    (int (Math/ceil (/ (Math/log (inc number))
                       (Math/log 2))))))

(defn bits [number]
  (loop [bits-left (bit-count number)
         shifted-number number
         bits (list)]
    (if (< 0 bits-left)
      (recur (dec bits-left)
             (unsigned-bit-shift-right shifted-number 1)
             (conj bits (bit-and shifted-number 1)))
      bits)))

(deftest test-bits
  (is (= '(1 0)
         (bits 2)))

  (is (= '(1 0 0)
         (bits 4))))

(defn left-pad [length padding values]
  (concat (repeat (- length (count values))
                  padding)
          values))

(deftest test-left-pad
  (is (= '(0 0 1 1)
         (left-pad 4 0 [1 1]))))


(defn bytes-to-bits [bytes]
  (for [byte (map #(Byte/toUnsignedInt %) bytes)]
    (left-pad 8 0 (bits byte))))

(defn encode-vlq [number]
  (loop [result []
         remaining-bits (bits number)]
    (if (empty? remaining-bits)
      result
      (recur (concat result
                     [(if (< 7 (count remaining-bits))
                        1
                        0)]
                     (left-pad 7 0 (take 7 remaining-bits)))
             (drop 7 remaining-bits)))))

(defn write-uuid [view uuid]
  (binf/wr-b64 view (.getMostSignificantBits uuid))
  (binf/wr-b64 view (.getLeastSignificantBits uuid)))

(defn read-uuid [view]
  (UUID. (binf/rr-i64 view)
         (binf/rr-i64 view)))

(defn read-sequence [read-value view]
  (let [last-position (+ (binf/position view)
                         (leb128/rr-u64 view))]
    (loop [values []]
      (if (< (binf/position view)
             last-position)
        (recur (conj values (read-value view)))
        values))))

(defn write-sequence [byte-count write-value view values]
  (let [byte-count (reduce + (map byte-count values))
        byte-count-with-byte-count-prefix (+ byte-count
                                             (leb128/n-byte-u64 byte-count))
        view (if (< (binf/remaining view)
                    byte-count-with-byte-count-prefix)
               (binf/grow view
                          (max 1000
                               (- byte-count-with-byte-count-prefix
                                  (binf/remaining view))))
               view)]
    (leb128/wr-u64 view byte-count)
    (run! (partial write-value view)
          values)
    view))

(defn read-uuid-sequence [view]
  (read-sequence read-uuid view))


(defn write-uuid-sequence [view uuids]
  (write-sequence (constantly 16)
                  write-uuid
                  view
                  uuids))

(defn read-leb128-sequence [view]
  (read-sequence leb128/rr-u64
                 view))

(defn write-leb128-sequence [view leb128s]
  (write-sequence leb128/n-byte-u64
                  leb128/wr-u64
                  view
                  leb128s))

(defn write-entity-id [view entity-id]
  (write-uuid view (first entity-id))
  #_(run! (partial leb128/wr-u64 view)
          ))

(defn write-type-definitions [view type-definition]
  (leb128/wr-u64 view (:byte-count type-definition))
  (write-leb128-sequence view (:entity-id type-definition)))

#_(defn write-record [view record]
  (doseq [[key value] record]
    (write-leb128-sequence view (:entity-id type-definition))
    ))

(defn base-64-to-byte-array [base-64-string]
  (.decode (Base64/getDecoder)
           base-64-string))

(defn base-64-to-uuid [base-64-string]
  (let [byte-buffer (ByteBuffer/wrap (base-64-to-byte-array base-64-string))]
    (UUID. (.getLong byte-buffer 0)
           (.getLong byte-buffer Long/BYTES))))

(defn byte-array-to-base-64 [byte-array]
  (.encodeToString (Base64/getEncoder)
                   byte-array)
  #_(string/replace (.encodeToString (Base64/getEncoder)
                                     byte-array)
                    "=" ""))

(deftest test-byte-array-to-base-64
  (is (= "AA"
         (byte-array-to-base-64 (byte-array [0])))))

(comment
  (vec (base-64-to-byte-array (byte-array-to-base-64 (byte-array [300]))))
  ) ;; TODO: remove-me


(defn uuid-to-base-64 [uuid]
  (let [byte-buffer (ByteBuffer/allocate (* 2 Long/BYTES))]
    (.putLong byte-buffer 0 (.getMostSignificantBits uuid))
    (.putLong byte-buffer Long/BYTES (.getLeastSignificantBits uuid))
    (byte-array-to-base-64 (.array byte-buffer))))

(deftest test-uuid-to-base-64
  (let [uuid #uuid "731cecc5-646c-4ac1-a312-2d3dac75b56d"]
    (is (= uuid
           (base-64-to-uuid (uuid-to-base-64 uuid))))))

(defn digit-count [base number]
  (int (Math/ceil (/ (Math/log (inc number))
                     (Math/log base)))))

(deftest test-digit-count
  (is (= 1
         (digit-count 2 1)))

  (is (= 2
         (digit-count 2 2)))

  (is (= 3
         (digit-count 2 5)))

  (is (= 4
         (digit-count 16
                      (dec (Math/pow 16 4))))))

(defn encode-number [characters number]
  (let [character-vector (vec characters)
        base (count characters)]
    (reverse (for [digit-number (range (digit-count base number))]
               (get character-vector
                    (int (mod (Math/floor (/ number
                                             (max 1
                                                  (Math/pow base digit-number))))
                              base)))))))

(deftest test-encode-number
  (is (= '(1 0)
         (encode-number (range 10)
                        10)))

  (is (= '(9)
         (encode-number (range 10)
                        9)))

  (is (= '(1 1)
         (encode-number (range 10)
                        11)))

  (is (= '(2 2)
         (encode-number (range 4)
                        10)))

  (is (= '(15 15)
         (encode-number (range 16)
                        255)))

  (is (= '(\F \F)
         (encode-number "0123456789ABCDEF"
                        255))))

(defn encode-number-in-base-64 [number]
  (apply str
         (encode-number "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
                        number)))

(defn encode-number-in-base-62 [number]
  (apply str
         (encode-number "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
                        number)))

(defn encode-number-in-base-52 [number]
  (apply str
         (encode-number "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
                        number)))

(defn encode-number-in-base-32 [number]
  (apply str
         (encode-number "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"
                        number)))

(defn encode-number-in-base-16 [number]
  (apply str
         (encode-number "0123456789ABCDEF"
                        number)))

(defn encode-number-in-base-10 [number]
  (apply str
         (encode-number "0123456789"
                        number)))

(defn bits-per-digit [base]
  (let [log-2 (/ (Math/log base)
                 (Math/log 2))]
    (if (= 0 (mod log-2 1))
      (int log-2)
      nil)))

(deftest test-bits-per-digit
  (is (= 1
         (bits-per-digit 2)))

  (is (= nil
         (bits-per-digit 3)))

  (is (= 2
         (bits-per-digit 4))))

#_(defn bit-slice [byte-array start end]
  )

#_(defn encode-byte-array [characters byte-array]
  (let [character-vector (vec characters)
        base (count characters)
        bits-per-digit (bits-per-digit base)]
    (reverse (for [digit-number (range (Math/ceil (/ bits-per-digit
                                                     (* 8 (count byte-array)))))]
               (get character-vector
                    (int (mod (Math/floor (/ number
                                             (max 1
                                                  (Math/pow base digit-number))))
                              base)))))))

(comment

  (encode-number-in-base-64 182)
  [["rf9GXhlwSIeaqrU75MHDsA"]
   [[[0 0]]]
   0 "rf9"]


  (float (let [bar-width 11
               gap 110
               total-width (- (/ 2100 2)
                              (/ bar-width 2))
               number-of-bars 9]
           (/ (- total-width
                 (* number-of-bars
                    bar-width))
              number-of-bars)))

  (mod 11 10)
  (bit-and 10000 0xFF)

  (digit-count 64 16000000)

  (repeatedly 100
              #(let [number (+ 0 #_(Math/pow 64
                                             4)
                               (rand-int (Math/pow 64
                                                   4)))]
                 [number
                  #_(encode-number-in-base-10 number)
                  #_(encode-number-in-base-16 number)
                  (encode-number-in-base-32 number)
                  (encode-number-in-base-52 number)
                  #_(encode-number-in-base-62 number)
                  (encode-number-in-base-64 number)]))

  (encode-number-in-base-32 1009)
  ;; => [16000000 "16000000" "F42400" "9CQA"]

  (Math/pow 2 4)
  ;; => 16.0

  (Math/pow 2 4)


  (encode-number-in-base-16 256)

  (encode-number-in-base-16 256)

  (/ (Math/log (* 365 24 60 60 1000))
     (Math/log 2)
     8)

  (/ (Math/pow 2 (* 8 5))
     (* 365 24 60 60 1000))
  ;; => 34.865285000507356

  (/ (Math/pow 2 (* 8 6))
     (* 365 24 60 60 1000))
  ;; => 8925.512960129883
  ) ;; TODO: remove-me

(defn hexify "Convert byte sequence to hex string" [coll]
  (let [hex [\0 \1 \2 \3 \4 \5 \6 \7 \8 \9 \a \b \c \d \e \f]]
    (letfn [(hexify-byte [b]
              (let [v (bit-and b 0xFF)]
                [(hex (bit-shift-right v 4)) (hex (bit-and v 0x0F))]))]
      (apply str (mapcat hexify-byte coll)))))

(defn hexify-str [s]
  (hexify (.getBytes s)))

(def dabric-prelude-types [:unsigned-integer :utf-8-string :type-tagged-value :struct :array])

(comment
  {:streams {:dabric {:id "Ja5cLdDSTtSqTtarUP5pMg"
                      :labels {:unsigned-integer 1
                               :float 2
                               :utf-8-string 3
                               :type-tagged-value 4
                               :struct 5
                               :array 6
                               :entity-id 7}}
             :nucalc {:id "4rgiDBuBS5+FsPrW3wGuIA"
                      :labels {:nutrient 1
                               :amount 2
                               :food 3}}}
   :value-type-instances {:measurement-array [[:dabric :array] 0 [[:dabric :struct] 0 [[:nucalc :nutrient] [[:dabric :entity-id] 0]]
                                                                  [[:nucalc :food] [[:dabric :entity-id] 0]]
                                                                  [[:nucalc :amount] [[:dabric :float] 4]]]]}
   :body [:measurement-array
          [[:nucalc 3] [:nucalc 8] 3.32
           [:nucalc 3] [:nucalc 7] 0.41]]}

  {:streams {:dabric {:id "Ja5cLdDSTtSqTtarUP5pMg"
                      :labels {:unsigned-integer 1
                               :float 2
                               :utf-8-string 3
                               :type-tagged-value 4
                               :struct 5
                               :array 6
                               :entity-id 7
                               :most-significant-bit-length-encoding 8
                               :fixed-length-encoding 9
                               :prefix-length-encoding 10
                               :value-count-prefix-length-endocing 11
                               :fixed-value-count-lenght-encoding 12}}
             :nucalc {:id "4rgiDBuBS5+FsPrW3wGuIA"
                      :labels {:nutrient 1
                               :amount 2
                               :food 3}}
             :nutrients {:id "qSsFB2igQkuQ3MmbeLfwdg"
                            :labels {:carbohydrates 1}}
             :foods {:id "JzflY3ubRFaiPg9vBKxIeQ"
                     :labels {:carrot 1}}
             :measurements {:id "rlphiiDhQnesoGVqlCFL3g"}}
   :value-type-instances {:float-32 [[:dabric :float] [[:dabric :fixed-length-encoding] 4]]
                          :entity-id [[:dabric :entity-id] [[:dabric :value-count-prefixed-encoding] 2] [:dabric :array]]
                          :datom-array [[:dabric :array] 0 [[:dabric :struct] 0
                                                            [[:dabric :entity] :entity-id]
                                                            [[:dabric :attribute] :entity-id]
                                                            [[:dabric :value] [[:dabric :type-tagged-value] 0]]
                                                            [[:dabric :transaction-number] [[:dabric :leb-128] 0]]
                                                            [[:dabric :operator] [[:dabric :unsigned-integer] 1]]]]}
   :body [:datom-array
          [[:measurements 3] [:nucalc :nutrient] [:entity-id [:nutrients :carbohydrates]] 1 0
           [:measurements 3] [:nucalc :food] [:entity-id [:foods :carrot]] 1 0
           [:measurements 3] [:nucalc :amount] [:float-32 3.32] 1 0]]}

  (take 10 (repeatedly #(uuid-to-base-64 (UUID/randomUUID))))

  (let [byte-buffer (ByteBuffer/allocate Long/BYTES)]
    (.putLong byte-buffer 0 1)
    (.encodeToString (Base64/getEncoder)
                     (.array byte-buffer)))

  (let [byte-buffer (ByteBuffer/allocate Long/BYTES)]
    (.putLong byte-buffer 0 0)
    (bytes-to-bits (.array byte-buffer)))

  (binf/rr-u64)

  (-> (buffer/alloc 1)
      (binf/view)
      (write-leb128-sequence (take 10 (repeatedly #(rand-int Integer/MAX_VALUE))))
      (binf/seek 0)
      (read-leb128-sequence))

  (count (buffer/alloc 8))

  (leb128/n-byte-u64 123213)

  (def uuid (java.util.UUID/randomUUID))
  (.getMostSignificantBits uuid)
  (let [uuids (take 3 (repeatedly #(java.util.UUID/randomUUID)))]
    (do ;; = uuids
      (-> (buffer/alloc 1 #_(+ 1 (* 3 16)))
          (binf/view)
          (write-uuid-sequence uuids)
          (binf/seek 0)
          (read-uuid-sequence)
          #_(binf/wr-buffer (.getBytes "DD" "utf-8"))
          #_(leb128/wr-u64 123213)
          #_(binf/backing-buffer)
          #_(bytes-to-bits))))
  (= uuid
     (-> (buffer/alloc (* 3 16))
         (binf/view)
         (write-uuid-sequence [] uuid)
         (binf/seek 0)
         (read-uuid)
         #_(binf/wr-buffer (.getBytes "DD" "utf-8"))
         #_(leb128/wr-u64 123213)
         #_(binf/backing-buffer)
         #_(bytes-to-bits)))

  (-> (buffer/alloc 8)
      (binf/view)
      (binf/wr-b64 1)
      (binf/backing-buffer)
      (bytes-to-bits))


  (bytes-to-bits (.toByteArray (bigint (Math/pow 2 8))))

  (type (.lpart (bigint (Math/pow 2 62))))

  (int (Math/ceil (/ (Math/log 1)
                     (Math/log 2))))
  (map bits (range 10))
  (/ (Math/log java.lang.Long/MAX_VALUE)
     (Math/log 2))


  ;; From the current position (0 for a new view)
  ;;
  (let [position-date (binf/position my-view)]
    (-> my-view
        (wr-date 2021
                 3
                 16)
        (binf/seek position-date)
        rr-date))


  (encode/base-16-encode )

  )
