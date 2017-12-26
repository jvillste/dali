(ns examples.cars
  (:require [net.cgrand.xforms :as xforms]
            
            [clojure.string :as string]
            (argumentica [db :as db]
                         [transaction-log :as transaction-log]
                         [storage :as storage]
                         [hash-map-storage :as hash-map-storage]
                         [sorted-map-transaction-log :as sorted-map-transaction-log]
                         [comparator :as comparator]
                         [index :as index]
                         [btree :as btree]
                         [btree-index :as btree-index]
                         [directory-storage :as directory-storage])

            [iota :as iota])
  (:import [java.util UUID]
           )
  (:use clojure.test))

#_(def source-file-name "/Users/jukka/Downloads/Tieliikenne 5.0.csv")
(def source-file-name "/Users/jukka/Downloads/vastauksetavoimenadatana1.csv")


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
  (let [csv-columns (map (fn [key]
                           (keyword (string/replace key " " "_")))
                         (first (process-csv-lines source-file-name
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
  (let [csv-columns (map (fn [key]
                           (keyword (-> key
                                        (string/replace " " "_")
                                        (string/replace "|" "-")
                                        (string/replace ":" ""))))
                         (first (transduce-csv-lines source-file-name
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
                               (comp #_(xforms/partition 2)
                                     (take 1)
                                     (map-indexed (fn [index foo]
                                                    (prn index (keys foo)))))))



(defn new-entity-id []
  (UUID/randomUUID))

(defn eatcv-to-eatcv-datoms [e a t c v]
  [[e a t c v]])

(defn eatcv-to-avtec-datoms [e a t c v]
  [[a v t e c]])

(defn map-to-transaction [transaction-number entity-id eatcv-to-datoms a-map]
  (reduce (fn [transaction [key value]]
            (apply conj
                   transaction
                   (eatcv-to-datoms entity-id
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
                             eatcv-to-eatcv-datoms
                             {:name "Foo"
                              :age 20}))))

(comment
  (transduce (comp (take 2)
                   (map println))
             (constantly nil)
             [1 2 3])
  
  (transduce (map inc)
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

(defn add-to-index [btree-atom entity-id transaction-number eatcv-to-datom entity-map]
  (doseq [datom (map-to-transaction transaction-number
                                    entity-id
                                    eatcv-to-datom
                                    entity-map)]
    (swap! btree-atom btree/add datom)

    #_(swap! (:index-atom btree-atom)
             btree/unload-excess-nodes 5 #_100)))


(defn log-index-state [name btreee-atom log-state-atom]
  (swap! log-state-atom
         update :count  (fnil inc 0))
  (when (= 0 (mod (:count @log-state-atom)
                  100))
    (prn (conj (select-keys @btreee-atom
                            [:loaded-nodes])
               @log-state-atom
               {:name name}))))

(defn map-values [m f & args]
  (reduce #(apply update-in %1 [%2] f args)
          m
          (keys m)))

(defn tokenize [string]
  (string/split string #" "))

(defn create-indexes []
  (let [create-index (fn []
                       (btree/create (btree/full-after-maximum-number-of-values 3001)
                                     {}
                                     #_(directory-storage/create "data/1")))
        avtec-attributes #{:sukunimi}
        full-text-attributes #{:Vaalilupaus_1}
        indexes (-> {:eatcv {:index-atom (atom (create-index))
                             :eatcv-to-datoms eatcv-to-eatcv-datoms}

                     :avtec {:index-atom (atom (create-index))
                             :eatcv-to-datoms (fn [e a t c v]
                                                (if (avtec-attributes a)
                                                  [[a
                                                    (string/lower-case v)
                                                    t
                                                    e
                                                    c]]
                                                  []))}
                     
                     :attributes {:index-atom (atom (create-index))
                                  :eatcv-to-datoms (fn [e a t c v]
                                                     [[a]])}
                     
                     :full-text {:index-atom (atom (create-index))
                                 :eatcv-to-datoms (fn [e a t c v]
                                                    (for [token (tokenize v)]
                                                      [a
                                                       (string/lower-case token)
                                                       t
                                                       e
                                                       c]))}}
                    
                    (map-values assoc :log-state (atom {})))]

    (transduce-csv-lines-as-maps source-file-name
                                 (comp (take 200)
                                       (map (fn [entity-map]
                                              (let [entity-id (new-entity-id)]
                                                (doseq [index-key (keys indexes)]
                                                  (let [{:keys [index-atom eatcv-to-datoms log-state]} (get indexes
                                                                                                            index-key)]
                                                    (add-to-index index-atom
                                                                  entity-id
                                                                  0
                                                                  eatcv-to-datoms
                                                                  entity-map)
                                                    
                                                    (log-index-state (str index-key)
                                                                     index-atom
                                                                     log-state)))
                                                
                                                #_(add-to-index eatcv-atom
                                                                entity-id
                                                                0
                                                                eatcv-to-eatcv-datoms
                                                                entity-map)
                                                
                                                #_(add-to-index avtec-atom
                                                                entity-id
                                                                0
                                                                eatcv-to-avtec-datoms
                                                                entity-map))

                                              #_(log-index-state "eatcv-atom" eatcv-atom eatcv-log-state)
                                              #_(log-index-state "avtec-atom" avtec-atom avtec-log-state))))
                                 #_(comp (xforms/partition 10 #_000)
                                         (take 1000)
                                         (map-indexed (fn [transaction-number entity-maps]
                                                        (doseq [entity-map entity-maps]
                                                          (add-to-index eatcv-atom
                                                                        transaction-number
                                                                        eatcv-to-eatcv-datoms
                                                                        entity-map)
                                                          
                                                          (add-to-index avtec-atom
                                                                        transaction-number
                                                                        eatcv-to-avtec-datoms
                                                                        entity-map)

                                                          #_(swap! (:index-atom eatcv-atom)
                                                                   btree/unload-btree)

                                                          #_(log-index-state "eatcv-atom" eatcv-atom eatcv-log-state)
                                                          #_(log-index-state "avtec-atom" avtec-atom avtec-log-state))))))
    indexes))

(defonce indexes {})

(defn create-db [& {:keys [indexes
                           transaction-log]
                    :or {indexes {}
                         transaction-log (sorted-map-transaction-log/create)}}]
  {:next-transaction-number (if-let [last-transaction-number (transaction-log/last-transaction-number transaction-log)]
                              (inc last-transaction-number)
                              0)
   :indexes indexes
   :transaction-log transaction-log})

(defn add-log-entry [db eacv-statements]
  (transaction-log/add! (:transaction-log db)
                        (:next-transaction-number db)
                        eacv-statements)
  (update db
          :next-transaction-number
          inc))

(defn update-index [index transaction-log]

  (let [new-transactions (transaction-log/subseq transaction-log
                                                 (if-let [last-transaction-number (:last-transaction-number index)]
                                                   (inc last-transaction-number)
                                                   0))]
    
    (doseq [[t statements] new-transactions]
      (doseq [[e a c v] statements]
        (doseq [datom ((:eatcv-to-datoms index)
                       e
                       a
                       t
                       c
                       v)]
          (swap! (:index-atom index)
                 btree/add
                 datom)))

      #_(swap! (:index-atom index)
               btree/unload-excess-nodes 5 #_100))
    (assoc index
           :last-transaction-number
           (:transaction-number (last new-transactions)))))

(defn apply-to-indexes [db function & arguments]
  (update db :indexes
          (fn [indexes]
            (reduce (fn [indexes index-key]
                      (apply update
                             indexes
                             index-key
                             function
                             arguments))
                    indexes
                    (keys indexes)))))

(defn update-indexes [db]
  (apply-to-indexes db
                    update-index
                    (:transaction-log db)))

(defn flush-index-after-maximum-number-of-transactions [index last-transaction-number maximum-number-of-transactions-after-previous-flush]
  (when (<= maximum-number-of-transactions-after-previous-flush
            (- last-transaction-number
               (or (-> (btree/latest-root @(:index-atom index))
                       :metadata
                       :last-transaction-number)
                   0)))
    (swap! (:index-atom index)
           btree/store-root
           {:last-transaction-number last-transaction-number}))
  index)

(defn flush-indexes-after-maximum-number-of-transactions [db maximum-number-of-transactions-after-previous-flush]
  (let [last-transaction-number (transaction-log/last-transaction-number (:transaction-log db))]
    (apply-to-indexes db
                      flush-index-after-maximum-number-of-transactions
                      last-transaction-number
                      maximum-number-of-transactions-after-previous-flush)))

(defn transact [db statements]
  (-> db
      (add-log-entry statements)
      (update-indexes)))

(deftest test-transact
  (is (= '([1 :friend 0 :set 2]
           [1 :friend 1 :set 3]
           [2 :friend 0 :set 1])
         (let [db (-> (create-db :indexes {:eatcv {:index-atom (atom (btree/create))
                                                   :eatcv-to-datoms eatcv-to-eatcv-datoms}}
                                 :transaction-log (sorted-map-transaction-log/create))
                      (transact [[1 :friend :set 2]
                                 [2 :friend :set 1]])
                      (transact [[1 :friend :set 3]]))]
           (btree/inclusive-subsequence (-> db :indexes :eatcv :index-atom)
                                        [1 :friend nil nil nil])))))

(deftest read-only-index-test
  (let [metadata-storage (hash-map-storage/create)
        node-storage (hash-map-storage/create)
        transaction-log (sorted-map-transaction-log/create)
        transactor-db (-> (create-db :indexes {:eatcv {:index-atom (atom (btree/create-from-options :metadata-storage metadata-storage
                                                                                                    :node-storage node-storage))
                                                       :eatcv-to-datoms eatcv-to-eatcv-datoms}}
                                     :transaction-log transaction-log)
                          (transact [[1 :friend :set 2]
                                     [2 :friend :set 1]])
                          (flush-indexes-after-maximum-number-of-transactions 0)
                          (transact [[1 :friend :set 3]]))
        
        read-only-db (-> (create-db :indexes {:eatcv {:index-atom (atom (btree/create-from-options :metadata-storage metadata-storage
                                                                                                   :node-storage node-storage))
                                                      :eatcv-to-datoms eatcv-to-eatcv-datoms
                                                      :last-transaction-number (or (-> (btree/latest-root (btree/roots-from-metadata-storage metadata-storage))
                                                                                       :metadata
                                                                                       :last-transaction-number)
                                                                                   0)}}
                                    :transaction-log transaction-log)
                         (update-indexes))]
    
    (is (= '([1 :friend 0 :set 2]
             [1 :friend 1 :set 3]
             [2 :friend 0 :set 1])
           (btree/inclusive-subsequence (-> transactor-db :indexes :eatcv :index-atom)
                                        [1 :friend nil nil nil])
           (btree/inclusive-subsequence (-> read-only-db :indexes :eatcv :index-atom)
                                        [1 :friend nil nil nil])))))

(defn last-transaction-number [metadata-storage]
  (-> (btree/latest-root (btree/roots-from-metadata-storage metadata-storage))
      :metadata
      :last-transaction-number))

(deftest reaload-db
  (let [metadata-storage (hash-map-storage/create)
        node-storage (hash-map-storage/create)
        transaction-log (sorted-map-transaction-log/create)
        db1 (-> (create-db :indexes {:eatcv {:index-atom (atom (btree/create-from-options :metadata-storage metadata-storage
                                                                                          :node-storage node-storage))
                                             :eatcv-to-datoms eatcv-to-eatcv-datoms}}
                           :transaction-log transaction-log)
                (transact [[1 :friend :set 2]
                           [2 :friend :set 1]])
                (flush-indexes-after-maximum-number-of-transactions 0)
                (transact [[1 :friend :set 3]]))
        
        db2 (-> (create-db :indexes {:eatcv {:index-atom (atom (btree/create-from-options :metadata-storage metadata-storage
                                                                                          :node-storage node-storage))
                                             :eatcv-to-datoms eatcv-to-eatcv-datoms
                                             :last-transaction-number (last-transaction-number metadata-storage)}}
                           :transaction-log transaction-log)
                (transact [[1 :friend :set 4]]))]
    
    (is (= '([1 :friend 0 :set 2]
             [1 :friend 1 :set 3]
             [2 :friend 0 :set 1])
           (btree/inclusive-subsequence (-> db1 :indexes :eatcv :index-atom)
                                        [1 :friend nil nil nil])))

    (is (= '([1 :friend 0 :set 2]
             [1 :friend 1 :set 3]
             [1 :friend 2 :set 4]
             [2 :friend 0 :set 1])
           (btree/inclusive-subsequence (-> db2 :indexes :eatcv :index-atom)
                                        [1 :friend nil nil nil])))))

(comment
  (def indexes (create-indexes))

  (btree/loaded-node-count @(-> indexes :eatcv :index-atom)))

(defn start []

  
  (let [{:keys [eatcv-atom avtec-atom]} indexes]
    #_(:nodes @(:index-atom eatcv))
    #_(:nodes @(:index-atom avtec))


    #_(swap! (:index-atom eatcv)
             btree/unload-btree)
    #_(swap! (:index-atom eatcv)
             btree/collect-storage-garbage)
    #_(prn (:storage-metadata @(:index-atom eatcv)))
    
    #_(prn {:total-stoarge-size (float (/ (btree/total-storage-size @(:eatcv-atom indexes))
                                          1024))
            :max-node-size (float (/ (apply max (btree/stored-node-sizes @(:eatcv-atom indexes)))
                                     1024))
            :average-node-size (float (/ (btree/total-storage-size @(:eatcv-atom indexes))
                                         (count (btree/used-storage-keys @(:eatcv-atom indexes)))
                                         1024))
            :unused-stored-nodes (count (btree/unused-storage-keys @(:eatcv-atom indexes)))
            :used-stored-nodes (count (btree/used-storage-keys @(:eatcv-atom indexes)))})

    #_(let [target-a :sukunimi
            target-v "a"
            latest-transaction-number 6]
        (take 10
              (take-while (fn [[a v t e c]]
                            (and (= a target-a)
                                 (#{0 1} (compare v target-v))
                                 (<= t latest-transaction-number)))
                          (index/inclusive-subsequence avtec
                                                       [target-a target-v nil nil nil]))))

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

(defn storage-statistics [btree]
  {:loaded-node-count (btree/loaded-node-count btree)
   :total-stoarge-size (float (/ (btree/total-storage-size btree)
                                 1024))
   :max-node-size (let [stored-node-sizes (btree/stored-node-sizes btree)]
                    (if (empty? stored-node-sizes)
                      0
                      (float (/ (apply max stored-node-sizes)
                                1024)))) 
   :average-node-size (if (< 0 (count (btree/used-storage-keys btree)))
                        (float (/ (btree/total-storage-size btree)
                                  (count (btree/used-storage-keys btree))
                                  1024))
                        0)
   :unused-stored-nodes (count (btree/unused-storage-keys btree))
   :used-stored-nodes (count (btree/used-storage-keys btree))})

(defn entities-by-string-value [avtec-atom attribute pattern latest-transaction-number]
  (map (fn [datom]
         (nth datom
              3))
       (take-while (fn [[a v t e c]]
                     (and (= a attribute)
                          (string/starts-with? v pattern)
                          (<= t latest-transaction-number)))
                   (btree/inclusive-subsequence avtec-atom
                                                [attribute pattern nil nil nil]))))


(defn value [eatcv-atom entity-id attribute latest-transaction-number]
  (last (last (take-while (fn [[e a t c v]]
                            (and (= a attribute)
                                 (= e entity-id)
                                 (<= t latest-transaction-number)))
                          (btree/inclusive-subsequence eatcv-atom
                                                       [entity-id attribute nil nil nil])))))

(deftype Entity [indexes entity-id transaction-number]
  Object
  (toString [this]   (pr-str entity-id))
  (hashCode [this]   (hash this))       ; db?

  clojure.lang.Seqable
  (seq [this]           (seq []))

  clojure.lang.Associative
  (equiv [this other-object] (= this other-object))
  (containsKey [this attribute] (value (-> indexes :eatcv :index-atom)
                                       entity-id
                                       attribute
                                       transaction-number))
  (entryAt [this attribute]     (some->> (value (-> indexes :eatcv :index-atom)
                                                entity-id
                                                attribute
                                                transaction-number)
                                         (clojure.lang.MapEntry. attribute)))

  (empty [this]         (throw (UnsupportedOperationException.)))
  (assoc [this k v]     (throw (UnsupportedOperationException.)))
  (cons  [this [k v]]   (throw (UnsupportedOperationException.)))
  (count [this]         (throw (UnsupportedOperationException.)))

  clojure.lang.ILookup
  (valAt [this attribute] (value (-> indexes :eatcv :index-atom)
                                 entity-id
                                 attribute
                                 transaction-number))
  (valAt [this attribute not-found] (or (value (-> indexes :eatcv :index-atom)
                                               entity-id
                                               attribute
                                               transaction-number)
                                        not-found))

  clojure.lang.IFn
  (invoke [this attribute] (value (-> indexes :eatcv :index-atom)
                                  entity-id
                                  attribute
                                  transaction-number))
  (invoke [this attribute not-found] (or (value (-> indexes :eatcv :index-atom)
                                                entity-id
                                                attribute
                                                transaction-number)
                                         not-found)))



(comment
  (def indexes (create-indexes))

  (swap! (-> indexes :eatcv :index-atom)
         btree/unload-btree)

  (storage-statistics @(-> indexes :eatcv :index-atom))

  (storage-statistics @(-> indexes :avtec :index-atom))

  (:nodes @(-> indexes :eatcv :index-atom))
  
  (:etunimi (->Entity indexes
                      #uuid "83abeff7-44ab-4223-bd70-80c634ef8644"
                      0))
  
  
  (for [entity (map (fn [entity-id]
                      (->Entity indexes
                                entity-id
                                0))
                    (take 20
                          #_(entities-by-string-value (-> indexes :avtec :index-atom)
                                                      :sukunimi
                                                      "aa"
                                                      0)
                          (dedupe (entities-by-string-value (-> indexes :full-text :index-atom)
                                                            :Vaalilupaus_1
                                                            "vanh"
                                                            0))))]
    [(:etunimi entity)
     (:sukunimi entity)
     (:Vaalilupaus_1 entity)
     (storage-statistics @(-> indexes :eatcv :index-atom))
     (storage-statistics @(-> indexes :avtec :index-atom))
     (storage-statistics @(-> indexes :full-text :index-atom))])

  (keys @(-> indexes :eatcv :index-atom))
  
  (let [btree-atom (-> indexes :attributes :index-atom)]
    (btree/inclusive-subsequence btree-atom
                                 (first (btree/sequence-for-cursor @btree-atom
                                                                   (btree/first-cursor @btree-atom)))))

  
  
  (#uuid "ce04e20f-5940-451d-a8a0-8da68968dea5"
   #uuid "28d29033-db8c-4e3f-98eb-dca5da323b41"
   #uuid "84f2e907-4449-4bd4-821a-4e7e819eca11")
  
  (let [target-a :sukunimi
        target-v "Kauppinen"
        latest-transaction-number 0]
    (take 10
          (take-while (fn [[a v t e c]]
                        (and (= a target-a)
                             (= v target-v)
                             (<= t latest-transaction-number)))
                      (btree/inclusive-subsequence (:avtec-atom indexes)
                                                   [target-a target-v nil nil nil]))))

  
  (take 10
        (btree/inclusive-subsequence (-> indexes :avtec :index-atom)
                                     [:sukunimi "Kauppinen" nil nil nil]))

  (take 1
        (btree/inclusive-subsequence (-> indexes :eatcv :index-atom)
                                     [#uuid "ce04e20f-5940-451d-a8a0-8da68968dea5" :sukunimi nil nil nil]))


  (compare [#uuid "882c884a-cb0a-48e0-b6cb-e54ed7134300" "b"]
           [#uuid "882c884a-cb0a-48e0-b6cb-e54ed7134300" 1])
  
  (sort-by identity comparator/cc-cmp  [[#uuid "882c884a-cb0a-48e0-b6cb-e54ed7134300" "a"]
                                        [#uuid "882c884a-cb0a-48e0-b6cb-e54ed7134300" "b"]
                                        [#uuid "88495aa6-2854-4a4c-a140-1bf7acb1abb7" 1]
                                        [#uuid "88495aa6-2854-4a4c-a140-1bf7acb1abb7" "c"]]))
