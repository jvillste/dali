(ns examples.crud-server
  (:gen-class)
  (:require [argumentica.btree :as btree]
            [argumentica.btree-db :as btree-db]
            [argumentica.btree-index :as btree-index]
            [argumentica.csv :as csv]
            [argumentica.db.common :as db-common]
            [argumentica.db.db :as db]
            [argumentica.db.file-transaction-log :as file-transaction-log]
            [argumentica.db.server-api :as server-api]
            [argumentica.directory-storage :as directory-storage]
            [argumentica.sorted-map-transaction-log :as sorted-map-transaction-log]
            [argumentica.sorted-set-index :as sorted-set-index]
            [argumentica.storage :as storage]
            [argumentica.transaction-log :as transaction-log]
            [clojure.set :as set]
            [clojure.string :as string]
            [clojure.test :as t :refer :all]
            [cor.api :as cor-api]
            [cor.server :as server]
            [kixi.stats.core :as stats]
            [me.raynes.fs :as fs]
            [net.cgrand.xforms :as xforms]))

(def schema
  {:genres {:multivalued? true}
   :directors {:multivalued? true
               :reference? true}
   :knownForTitles {:multivalued? true
                    :reference? true}
   :primaryProfession {:multivalued? true}})

(defn tokenize [string]
  (->> (string/split string #" ")
       (map string/lower-case)))

(defn eatcv-to-full-text-avtec [db e a t c v]
  (if (string? v)
    (let [old-tokens (set (mapcat tokenize (db-common/values db e a (dec t))))]
      (case c

        :retract
        (for [token (set/difference old-tokens
                                    (set (mapcat tokenize
                                                 (db-common/values-from-eatcv-datoms (concat (db-common/datoms db
                                                                                                               :eatcv
                                                                                                               [e a nil nil nil])
                                                                                             [[e a t c v]])))))]
          [a token t e :retract])

        :add
        (for [token (set/difference (set (tokenize v))
                                    old-tokens)]
          [a token t e :add])

        :set
        (let [new-tokens (set (tokenize v))]
          (concat (for [token (set/difference new-tokens old-tokens)]
                    [a token t e :add])
                  (for [token (set/difference old-tokens new-tokens)]
                    [a token t e :retract])))))
    []))

(defn create-directory-btree-index [base-path node-size index-name]
  (btree-index/create-directory-btree-index (str base-path "/" index-name)
                                            node-size))

(defn add-index [indexes key eatcv-to-datoms create-index]
  (assoc indexes
         key
         {:index (create-index (name key))
          :eatcv-to-datoms eatcv-to-datoms}))

(defn index [key eatcv-to-datoms])

(def imdb-index-definition
  {:eatcv db-common/eatcv-to-eatcv-datoms
   :avtec db-common/eatcv-to-avtec-datoms
   :full-text eatcv-to-full-text-avtec})


(defn create-btree-db [create-index transaction-log]
  (db-common/db-from-index-definition imdb-index-definition
                                      create-index
                                      transaction-log))

(defn create-directory-btree-db [base-path]
  (create-btree-db (fn [index-name]
                     (btree-index/create-directory-btree-index (str base-path "/" index-name)
                                                               1001))
                   (file-transaction-log/create (str base-path "/transaction-log"))))

(defn create-in-memory-btree-db [node-size]
  (create-btree-db (fn [_index-name]
                     (btree-index/create-memory-btree-index node-size))
                   (sorted-map-transaction-log/create)))

(deftest test-full-text-index
  (let [db (-> (db-common/map->LocalDb (create-in-memory-btree-db 21))
               (db/transact [[:entity-1 :name :set "First Name"]])
               (db/transact [[:entity-1 :name :set "Second Name"]])
               (db/transact [[:entity-1 :name :add "Third Name"]])
               (db/transact [[:entity-1 :name :retract "Second Name"]]))]
    (is (= '([:name "first" 0 :entity-1 :add]
             [:name "first" 1 :entity-1 :retract]
             [:name "name" 0 :entity-1 :add]
             [:name "second" 1 :entity-1 :add]
             [:name "second" 3 :entity-1 :retract]
             [:name "third" 2 :entity-1 :add])
           (db/inclusive-subsequence db :full-text nil)))))

#_(defn create-directory-btree-db [base-path]
    (db-common/update-indexes (db-common/create :indexes {:eatcv {:index (btree-index/create-directory-btree-index (str base-path "/eatcv")
                                                                                                                   1001)
                                                                  :eatcv-to-datoms db-common/eatcv-to-eatcv-datoms}
                                                          :avtec {:index (btree-index/create-directory-btree-index (str base-path "/avtec")
                                                                                                                   1001)
                                                                  :eatcv-to-datoms db-common/eatcv-to-avtec-datoms}
                                                          :full-text {:index (btree-index/create-directory-btree-index (str base-path "/full-text")
                                                                                                                       1001)
                                                                      :eatcv-to-datoms eatcv-to-full-text-avtec}}
                                                :transaction-log (file-transaction-log/create (str base-path "/transaction-log"))
                                                #_(sorted-map-transaction-log/create))))

#_(defn create-in-memory-db []
    (db-common/create :indexes {:eatcv {:index (sorted-set-index/create)
                                        :eatcv-to-datoms db-common/eatcv-to-eatcv-datoms}
                                :avtec {:index (sorted-set-index/create)
                                        :eatcv-to-datoms db-common/eatcv-to-avtec-datoms}
                                :full-text {:index (sorted-set-index/create)
                                            :eatcv-to-datoms eatcv-to-full-text-avtec}}
                      :transaction-log (sorted-map-transaction-log/create)))

(defn entity-map-to-transaction [entity-map schema id-key]
  (let [id (get entity-map
                id-key)]
    (mapcat (fn [[attribute value]]
              (if (-> schema attribute :multivalued?)
                (for [one-value (string/split value #",")]
                  [id attribute :add one-value])
                [[id attribute :set value]]))
            (dissoc entity-map id-key))))

(t/deftest test-entity-map-to-transaction
  (t/is (= '(["tt0000001" :titleType :set "short"]
             ["tt0000001" :genres :add "Short"]
             ["tt0000001" :genres :add "Documentary"])
           (entity-map-to-transaction {:titleType "short",
                                       :genres "Short,Documentary"
                                       :tconst "tt0000001"}
                                      {:genres {:multivalued? true}}
                                      :tconst))))
(defn imbdb-file-to-transaction [file-name id-key transducer]
  (csv/transduce-maps file-name
                      {:separator #"\t"}
                      (comp transducer
                            (mapcat (fn [entity-map]
                                      (entity-map-to-transaction entity-map
                                                                 schema
                                                                 id-key))))
                      conj
                      []))

(defn transact-titles [db file-name transducer]
  (transaction-log/make-transient! (:transaction-log db))
  (csv/transduce-maps file-name
                      {:separator #"\t"}
                      (comp transducer
                            (map (fn [entity-map]
                                   (entity-map-to-transaction (assoc entity-map :type :title)
                                                              schema
                                                              :tconst)))
                            (map (fn [transaction]
                                   (db-common/transact db transaction))))
                      (constantly nil)
                      nil)
  (btree-db/store-index-roots-after-maximum-number-of-transactions db 0)
  (transaction-log/truncate! (:transaction-log db) (inc (transaction-log/last-transaction-number (:transaction-log db))))
  (transaction-log/make-persistent! (:transaction-log db))
  db)

(defn titles-as-transaction [transducer]
  (imbdb-file-to-transaction "data/imdb/title.basics.tsv"
                             :tconst
                             (comp (map (fn [title]
                                          (assoc title :type :title)))
                                   transducer)))

(defn crew-as-transaction [transducer]
  (imbdb-file-to-transaction "data/imdb/title.crew.tsv"
                             :tconst
                             transducer))

(defn persons-as-transaction [transducer]
  (imbdb-file-to-transaction "data/imdb/name.basics.tsv"
                             :nconst
                             (comp (map (fn [title]
                                          (assoc title :type :person)))
                                   transducer)))



(defn add-titles [db count]
  (let [db (db-common/transact db (titles-as-transaction (take count)))
        title-ids (set (db-common/entities db :type :title))]

    (db-common/transact db
                        (crew-as-transaction (comp (filter (fn [title]
                                                             (contains? title-ids
                                                                        (:tconst title))))
                                                   (take count))))))

(defn add-directors [db]
  (let [person-ids (->> (db-common/entities db :type :title)
                        (map (fn [entity-id]
                               (db-common/->Entity db schema entity-id)))
                        (mapcat :directors)
                        (map :entity/id)
                        (into #{}))]
    (db-common/transact db
                        (persons-as-transaction (comp (filter (fn [person]
                                                                (contains? person-ids
                                                                           (:nconst person))))
                                                      #_(take 5))))))



(defn node-size-report [node-sizes title-count max-title-count]
  (let [total-megabytes (/ (reduce + node-sizes)
                           (* 1024 1024))]
    (merge {:count (count node-sizes)
            :total-megabytes  (int total-megabytes)
            :whole-db-megabytes (int (* total-megabytes
                                        (/ max-title-count title-count)))}
           (transduce identity stats/summary node-sizes))))



(comment
  (csv/transduce-lines "data/imdb/title.basics.tsv"
                       (comp #_(util/count-logger 100000)
                             xforms/count)
                       conj
                       [])

  (transduce xforms/count conj [] [1 2 3 4])


  (csv/transduce-maps "data/imdb/title.basics.tsv"
                      {:separator #"\t"}
                      (take 10)
                      conj
                      [])

  (titles-as-transaction (take 10))


  (def db (-> (create-in-memory-btree-db 21)
              (transact-titles "data/imdb/title.basics.tsv" (take 20))))

  (db-common/value db "tt0000002" :primaryTitle)

  (db-common/values db "tt0000002" :genres)

  (db-common/entities db :genres "Short")

  (db-common/->Entity db schema "tt0000002")

  (:primaryTitle (db-common/->Entity db schema "tt0000002"))

  (let [target-value "b"]
    (db-common/entities-2 (-> db :indexes :full-text :index)
                          :primaryTitle
                          target-value
                          (fn [value]
                            (.startsWith value target-value))))

  (let [target-value "b"]
    (->> (db-common/entities-2 (-> db :indexes :full-text :index)
                               :primaryTitle
                               target-value
                               (fn [value]
                                 (.startsWith value target-value)))
         (map (fn [entity-id]
                (db-common/->Entity db schema entity-id)))))

  (def disk-db-directory "data/temp/imdbdb")

  (do (do (fs/delete-dir (str disk-db-directory "/avtec"))
          (fs/delete-dir (str disk-db-directory "/eatcv"))
          (fs/delete-dir (str disk-db-directory "/full-text"))
          (fs/delete-dir (str disk-db-directory "/transaction-log")))
      )



  (let [title-count 1000000
        max-title-count 4933210
        node-size 200001
        db #_(create-directory-btree-db disk-db-directory)
        (create-in-memory-btree-db node-size)]
    (try
      (-> db
          #_(add-titles 10)
          (transact-titles "data/imdb/title.basics.tsv" (take title-count))
          #_(btree-db/store-index-roots-after-maximum-number-of-transactions 0)
          #_(db-common/value "tt0000002" :primaryTitle)
          (get-in [:indexes :eatcv :index])
          (btree-index/btree)
          (btree/stored-node-sizes)
          (node-size-report title-count max-title-count)
          (assoc :node-size node-size)
          (assoc :title-count title-count))
      (finally (btree-db/close! db))))

  (-> (directory-storage/create (str disk-db-directory "/avtec/nodes"))
      (storage/get-edn-from-storage! "5A9923F103A588A4ED1B08FA5189C99FE92E6A085B2EA64EC75DE64F886BB249"))

  (-> (directory-storage/create (str disk-db-directory "/eatcv/nodes"))
      (storage/get-edn-from-storage! "5A9923F103A588A4ED1B08FA5189C99FE92E6A085B2EA64EC75DE64F886BB249"))

  (-> (directory-storage/create (str disk-db-directory "/full-text/nodes"))
      (storage/get-edn-from-storage! "98CB6A20C5B5587691B202AA4E38DE64E12EF9AB1A74D2A6366E3BC49E970004"
                                     #_"FBDC7869347D39568AEBB7D25D768E792965CA3A8C047DA2BD98C24DF95D8000"))
  (fs/mkdirs "data/temp/storage")

  (def node-size-report-for-100000-titles-with-node-size-10001
    {:count 175,
     :total-megabytes 7,
     :whole-db-megabytes 363,
     :min 13975.0,
     :q1 42481.0,
     :median 43927.0,
     :q3 45059.75,
     :max 66770.0,
     :iqr 2578.75})

  {:count 87,
   :total-megabytes 68,
   :whole-db-megabytes 338,
   :min 7137.0,
   :q1 764638.5,
   :median 818078.0,
   :q3 896682.25,
   :max 1337318.0,
   :iqr 132043.75
   :node-size 200001
   :title-count 1000000}

  )



(defn start-server [directory port]
  (server/start-server (cor-api/app (server-api/create-state (-> #_(create-directory-btree-db directory)
                                                                 (create-in-memory-btree-db 21)
                                                                 (transact-titles "data/imdb/title.basics.tsv" (take 20))))
                                    'argumentica.db.server-api)
                       port))

(defn -main [& [port]]
  (start-server port))


(defonce server (atom nil))

(defn start []
  (when @server
    (do (println "closing")
        (@server)
        (Thread/sleep 1000)))

  (.start (Thread. (fn [] (reset! server
                                  (start-server "data/temp/crud" 4010))))))


(comment
  (imbdb-file-to-transaction "data/imdb/name.basics.tsv"
                             :nconst
                             (comp (take 10)
                                   (map (fn [title]
                                          (assoc title :type :person)))))

  (let [db (-> (db-common/create :indexes {:eatcv {:index (sorted-set-index/create)
                                                   :eatcv-to-datoms db-common/eatcv-to-eatcv-datoms}
                                           :avtec {:index (sorted-set-index/create)
                                                   :eatcv-to-datoms db-common/eatcv-to-avtec-datoms}
                                           :full-text {:index (sorted-set-index/create)
                                                       :eatcv-to-datoms eatcv-to-full-text-avtec}}
                                 :transaction-log (sorted-map-transaction-log/create))
               (db-common/transact (titles-as-transaction (take 100))))
        title-ids (into #{} (db-common/entities db :type :title))
        db (db-common/transact db
                               (crew-as-transaction (comp (filter (fn [title]
                                                                    (contains? title-ids
                                                                               (:tconst title)))))))
        person-ids (->> (db-common/entities db :type :title)
                        (map (fn [entity-id]
                               (db-common/->Entity db schema entity-id)))
                        (mapcat :directors)
                        (map :entity/id)
                        (into #{}))
        db (db-common/transact db
                               (persons-as-transaction (comp (filter (fn [person]
                                                                       (contains? person-ids
                                                                                  (:nconst person))))
                                                             #_(take 5))))]
    #_title-ids
    #_(crew-as-transaction (comp (take 1000)
                                 (filter (fn [title]
                                           (contains? title-ids
                                                      (:tconst title))))))

    #_(crew-as-transaction (comp (take 10)
                                 (filter (fn [title]
                                           (contains? title-ids
                                                      (:tconst title))))))
    (->> (db-common/entities db :type :person)
         (map (fn [entity-id]
                (db-common/->Entity db schema entity-id)))
         #_(map :knownForTitles))
    #_(->> (db-common/entities db :type :title)
           (map (fn [entity-id]
                  (db-common/->Entity db schema entity-id)))
           #_(map :primaryTitle))
    #_person-ids)




  (csv/transduce-maps "data/imdb/title.crew.tsv"
                      {:separator #"\t"}
                      (comp (filter (fn [{:keys [directors]}]
                                      (.contains directors
                                                 "nm0005690")))
                            (take 2))
                      conj
                      [])

  (csv/transduce-maps "data/imdb/name.basics.tsv"
                      {:separator #"\t"}
                      (comp #_(filter (fn [value]
                                        (= "nm0005690"
                                           (:nconst value))))
                            (take 10))
                      conj
                      [])

  (csv/transduce-lines "/Users/jukka/Downloads/mbdump/mbdump/release"
                       (comp (map (partial csv/split-line #"\t"))
                             (take 2))
                       conj
                       [])

  (csv/transduce-lines "/Users/jukka/Downloads/mbdump/mbdump/release_group"
                       (comp (map (partial csv/split-line #"\t"))
                             (take 2))
                       conj
                       [])

  (csv/transduce-lines "/Users/jukka/Downloads/mbdump/mbdump/artist_credit"
                       (comp (map (partial csv/split-line #"\t"))
                             #_(drop 1000)
                             (take 20))
                       conj
                       [])

  (csv/transduce-maps-with-headers "/Users/jukka/Downloads/mbdump/mbdump/artist"
                                   {:separator #"\t"}
                                   [nil :id :name :name2]
                                   (comp (filter (fn [artist] (and (string? (:name artist))
                                                                   (.startsWith (:name artist)
                                                                                "Metallica"))))
                                         (take 1))
                                   conj
                                   [])

  (csv/transduce-maps-with-headers "/Users/jukka/Downloads/mbdump/mbdump/release"
                                   {:separator #"\t"}
                                   [nil :id :name]
                                   (comp #_(filter (fn [artist] (and (string? (:name artist))
                                                                     (.startsWith (:name artist)
                                                                                  "Metallica"))))
                                         (take 10))
                                   conj
                                   []))


