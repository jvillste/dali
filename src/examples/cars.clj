(ns examples.cars
  (:require [net.cgrand.xforms :as xforms]
            (argumentica [db :as db]
                         [index :as index]
                         [btree :as btree]
                         [btree-index :as btree-index]
                         [directory-storage :as directory-storage])

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

(defn conj-reducer [collection]
  (fn ([collection value]
       (conj collection
             value))
                      
    ([result]
     result)
                      
    ([]
     collection)))

(defn nop-reducer
  ([a b]
   nil)
                      
  ([a]
   nil)
                      
  ([]
   nil))

(defn transduce-csv-lines [file-name transducer reducer]
  (with-open [rdr (clojure.java.io/reader file-name)]
    (transduce (comp (map read-line)
                     transducer)
               reducer
               (line-seq rdr))))

(comment
  (transduce-csv-lines source-file-name
                       (comp (drop 1)
                             (take 5)
                             (map prn))
                       nop-reducer)

  (transduce-csv-lines source-file-name
                       (comp (drop 1)
                             (take 5))
                       (conj-reducer #{}))

  (transduce-csv-lines source-file-name
                       (comp (drop 1)
                             (take 5))
                       conj))

(defn keys-and-values-to-hash-map [keys values]
  (apply hash-map
         (interleave keys
                     values)))

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

(defn transduce-csv-lines-as-maps [file-name transducer]
  (let [csv-columns (map keyword (first (transduce-csv-lines source-file-name
                                                             (comp (take 1))
                                                             (conj-reducer []))))]

    (transduce-csv-lines file-name
                         (comp (drop 1)
                               (map (partial keys-and-values-to-hash-map
                                             csv-columns))
                               transducer)
                         nop-reducer)
    nil))

(comment
  (transduce-csv-lines-as-maps source-file-name
                               (comp (xforms/partition 2)
                                     (take 2)
                                     (map-indexed (fn [index foo]
                                                    (prn index foo))))))



(defn new-entity-id []
  (UUID/randomUUID))

(defn eatcv-to-eatcv-datom [e a t c v]
  [e a t c v])

(defn eatcv-to-avtec-datom [e a t c v]
  [a v t e c])

(defn map-to-transaction [transaction-number entity-id eatcv-to-datom a-map]
  (reduce (fn [transaction [key value]]
            (conj transaction
                  (eatcv-to-datom entity-id
                                  key
                                  transaction-number
                                  :set
                                  value)))
          []
          a-map))

(deftest test-map-to-transaction
  (is (= [[2 :name 1 :set "Foo"]
          [2 :age 1 :set 20]]
         (map-to-transaction 1
                             2
                             eatcv-to-eatcv-datom
                             {:name "Foo"
                              :age 20}))))

(comment (transduce (map inc)
                    +
                    [1 2 3])
  
         (transduce (map inc)
                    (fn
                      ([a b]
                       (conj a b))
                      
                      ([a]
                       a))
                    []
                    [1 2 3])

         (transduce (comp (drop 2)
                          (take 2)
                          (map prn))
                    (fn
                      ([a b]
                       nil)
                      
                      ([a]
                       nil)
                      
                      ([]
                       nil))
                    (range 10))

         (transduce (map inc)
                    to-array
                    0
                    [1 2 3])

         (let [conter (atom 0)]
           )

         (sequence (comp (map inc)
                         (take 2))
                   (range 10)))

(defn add-to-index [index transaction-number eatcv-to-datom entity-map]
  (doseq [datom (map-to-transaction transaction-number
                                    (new-entity-id)
                                    eatcv-to-datom
                                    entity-map)]

    (index/add-to-index index
                        datom)

    (swap! (:index-atom index)
             btree/unload-excess-nodes 5 #_100)))

(defn log-index-state [name btree-index log-state-atom]
  (swap! log-state-atom
         update :count  (fnil inc 0))
  (when (= 0 (mod (:count @log-state-atom)
                  100))
    (prn (conj (select-keys @(:index-atom btree-index)
                            [:loaded-nodes])
               @log-state-atom
               {:name name}))))

(defn start []
  (let [crate-index (fn []
                      (btree-index/create 11 #_000
                                          {}
                                          #_(directory-storage/create "data/1")))
        eatcv (crate-index)
        avtec (crate-index)
        eatcv-log-state (atom {})
        avtec-log-state (atom {})]

    (transduce-csv-lines-as-maps source-file-name
                                 (comp (xforms/partition 100)
                                       (take 1)
                                       (map-indexed (fn [transaction-number entity-maps]
                                                      (doseq [entity-map entity-maps]
                                                        
                                                        (add-to-index eatcv
                                                                      transaction-number
                                                                      eatcv-to-eatcv-datom
                                                                      entity-map)
                                 
                                                        (add-to-index avtec
                                                                      transaction-number
                                                                      eatcv-to-avtec-datom
                                                                      entity-map)

                                                        #_(swap! (:index-atom eatcv)
                                                                 btree/unload-btree)

                                                        #_(log-index-state "eatcv" eatcv eatcv-log-state)
                                                        #_(log-index-state "avtec" avtec avtec-log-state))))))

    
    #_(process-csv-lines-as-maps source-file-name
                                 (fn [columns]
                                   (add-to-index eatcv
                                                 1
                                                 eatcv-to-eatcv-datom
                                                 columns)
                                 
                                   (add-to-index avtec
                                                 1
                                                 eatcv-to-avtec-datom
                                                 columns))
                                 10)

    #_(:nodes @(:index-atom eatcv))
    #_(:nodes @(:index-atom avtec))


    #_(swap! (:index-atom eatcv)
           btree/unload-btree)
    #_(:storage-metadata @(:index-atom eatcv))
    (btree/unused-storage-keys @(:index-atom eatcv))
    #_(let [target-a :ajoneuvoluokka
            target-v "M1"
            latest-transaction-number 6]
        (take 10
              (take-while (fn [[a v t e c]]
                            (and (= a target-a)
                                 (= v target-v)
                                 (<= t latest-transaction-number)))
                          (index/inclusive-subsequence avtec
                                                       [target-a target-v nil nil nil]))))
    
    #_(db/unload-index eatcv)

    #_(db/eatcv-statements eatcv
                           [-4480628169839524227 -4844517864935213435]))

  #_(let [eatcv (btree-index/create 100
                                    (directory-storage/create "data/1")
                                    "C7FA8B4622763597C3AA9B547297C443A79BBFB9A7B9688206E5B6D3DC21A477")]
      (db/eatcv-statements eatcv
                           [-4480628169839524227 -4844517864935213435])))
