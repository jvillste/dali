(ns examples.cars
  (:require [argumentica.index :as index]
            [argumentica.db :as db]
            [argumentica.directory-db :as directory-db]
            [iota :as iota])
  (:import [java.util UUID]
           )
  (:use clojure.test))

(def source-file-name "/Users/jukka/Downloads/Tieliikenne 5.0.csv")


(comment (take 2 (iota/seq source-file-name))
         (uuid?))

(defn read-line [string]
  (clojure.string/split string  #";"))

(defn process-csv-lines [file-name function drop-count take-count]
  (with-open [rdr (clojure.java.io/reader file-name)]
    (doall (map function
                (take take-count
                      (map read-line
                           (drop drop-count
                                 (line-seq rdr))))))))


(defn process-csv-lines-as-maps [file-name function take-count]
  (let [csv-columns (map keyword (first (process-csv-lines source-file-name
                                                           identity
                                                           0
                                                           1)))]
    (process-csv-lines source-file-name
                       (fn [values]
                         (function (apply hash-map
                                          (interleave csv-columns
                                                      values))))
                       1
                       10)
    nil))


(defn new-entity-id []
  (let [uuid (UUID/randomUUID)]
    [(.getMostSignificantBits uuid)
     (.getLeastSignificantBits uuid)]))

(defn map-to-transaction [a-map]
  (let [entity-id (new-entity-id)]
    (reduce (fn [transaction [key value]]
              (conj transaction
                    [entity-id
                     key
                     value
                     :set]))
            []
            a-map)))

(deftest test-map-to-transaction
  (is (= [[[-6446814645696639752 -8132693421789540280] :name "Foo" :set]
          [[-6446814645696639752 -8132693421789540280] :age 20 :set]]
         (map-to-transaction {:name "Foo"
                              :age 20}))))

(comment (index/unload-index )

         (compare (new-entity-id)
                  (new-entity-id))

         (into (sorted-set) (take 10 (repeatedly (fn [] (UUID/randomUUID)))))
         
         (compare (UUID/randomUUID)
                  (UUID/randomUUID))
         
         (< (byte-array [1 2])
            (byte-array [1 2])))



(comment

  

  (process-csv-lines)

  (transduce (map inc)
             +
             [1 2 3])
  
  (transduce (map inc)
             (fn
               ([a b]
                (+ a b))
               ([a]
                a))
             0
             [1 2 3])

  (index/create (index/full-after-maximum-number-of-values 10)
                (create-directory-index ))
  
  )

(defn start []
  (let [index #_(directory-db/create-directory-index "data/1")
        (directory-db/create-hash-map-index)]
    (process-csv-lines-as-maps source-file-name
                               (fn [columns]
                                 (doseq [eacv (map-to-transaction columns)]
                                   (db/add-to-index index
                                                    (db/add-transaction-number-to-eacv 1 eacv))))
                               10)
    (db/unload-index index))

  (let [index (directory-db/create-directory-index "data/1"
                                                             "C7FA8B4622763597C3AA9B547297C443A79BBFB9A7B9688206E5B6D3DC21A477")]
    (db/eatcv-statements index
                         [-4480628169839524227 -4844517864935213435])))
