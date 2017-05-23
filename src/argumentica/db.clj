(ns argumentica.db
  (:require [datascript.core :as d]
            [datascript.db :as db]
            [clojure.uuid :as uuid])
  (:import [java.security MessageDigest]
           [java.util Base64]
           [com.google.common.io BaseEncoding]
           [java.util UUID])
  (:use clojure.test))

(deftest test-entity
  (let [db (-> (d/empty-db)
               (d/db-with [{:db/id 1, :age 19}
                           {:db/id 2, :age "a"}
                           {:db/id 3, :age "b"}
                           {:db/id 4, :age 2}]))]
    (is (= nil
           (d/datoms db :eavt)))
    (is (= (:age (d/entity db 1)) 19))
    (is (= (:age (d/entity db 2)) "foo"))))

#_(let [db (:db-after (d/with (d/empty-db)
                              [{:db/id 1, :name "Foo"}] :haa))]

    (d/datoms db :eavt))


#_(def compare-transaction-index [a b]
    (if (= :max a)
      ))

(defn uuid []
  (.toString (UUID/randomUUID)))

#_ (uuid)

(defn base-64-encode [bytes]
  (.encode (BaseEncoding/base64)
           bytes))

(defn base-16-encode [bytes]
  (.encode (BaseEncoding/base16)
           bytes))

(defn sha-256 [message]
  (.digest (MessageDigest/getInstance "SHA-256")
           (.getBytes message "UTF-8")))

(defn sha-1 [message]
  (.digest (MessageDigest/getInstance "SHA-1")
           (.getBytes message "UTF-8")))




(defn transaction-id [transaction]
  (base-16-encode (sha-256 (pr-str transaction))))

(defn get-value [eatv e a]
  (nth (first (rsubseq eatv <= [e a Long/MAX_VALUE nil]))
       3))


(defn transaction [statements]
  )

(defn transact [eatv transaction statements]
  (apply conj eatv transaction )
  )


(deftest get-value-test
  (let [eatv (sorted-set [1 :name 1 "11"]
                         [2 :name 2 "21"]
                         [1 :name 2 "12"]
                         [2 :name 3 "22"])]
    (is (= "12" (get-value eatv 1 :name)))
    (is (= "22" (get-value eatv 2 :name)))))




(let [eatv (sorted-set [1 :name 1 "11"]
                       [1 :name 2 "12"]
                       [2 :name 2 "21"]
                       [2 :name 3 "22"])]
  (rsubseq eatv <= [2 :name Long/MAX_VALUE nil]))







