(ns crud-server
  (:gen-class)
  (:require [me.raynes.fs :as fs]
            [argumentica
             [berkeley-db-transaction-log :as berkeley-db-transaction-log]
             [btree-index :as btree-index]
             [csv :as csv]
             [sorted-map-transaction-log :as sorted-map-transaction-log]
             [sorted-set-index :as sorted-set-index]]
            [argumentica.db
             [common :as db-common]
             [server-api :as server-api]
             [file-transaction-log :as file-transaction-log]]
            [clojure
             [string :as string]
             [test :as t]]
            [cor
             [api :as cor-api]
             [server :as server]]
            [argumentica.btree-db :as btree-db]
            [argumentica.directory-storage :as directory-storage]
            [argumentica.storage :as storage]))

(defn tokenize [string]
  (string/split string #" "))

(defn eatcv-to-full-text-avtec [e a t c v]
  (if (string? v)
    (for [token (tokenize v)]
      [a
       (string/lower-case token)
       t
       e
       c])
    []))

(defn create-directory-btree-db [base-path]
  (db-common/update-indexes (db-common/create :indexes {:eatcv {:index (btree-index/create-directory-btree-index (str base-path "/eatcv"))
                                                                :eatcv-to-datoms db-common/eatcv-to-eatcv-datoms}
                                                        :avtec {:index (btree-index/create-directory-btree-index (str base-path "/avtec"))
                                                                :eatcv-to-datoms db-common/eatcv-to-avtec-datoms}
                                                        :full-text {:index (btree-index/create-directory-btree-index (str base-path "/full-text"))
                                                                    :eatcv-to-datoms eatcv-to-full-text-avtec}}
                                              :transaction-log (file-transaction-log/create (str base-path "/transaction-log"))
                                              #_(sorted-map-transaction-log/create))))

(defn create-in-memory-db []
  (db-common/create :indexes {:eatcv {:index (sorted-set-index/create)
                                      :eatcv-to-datoms db-common/eatcv-to-eatcv-datoms}
                              :avtec {:index (sorted-set-index/create)
                                      :eatcv-to-datoms db-common/eatcv-to-avtec-datoms}
                              :full-text {:index (sorted-set-index/create)
                                          :eatcv-to-datoms eatcv-to-full-text-avtec}}
                    :transaction-log (sorted-map-transaction-log/create)))

(def imdb-schema
  {:genres {:multivalued? true}
   :directors {:multivalued? true
               :reference? true}
   :knownForTitles {:multivalued? true
                    :reference? true}
   :primaryProfession {:multivalued? true}})

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
                                                                 imdb-schema
                                                                 id-key))))
                      conj
                      []))

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
        title-ids (into #{} (db-common/entities db :type :title))]

    (db-common/transact db
                        (crew-as-transaction (comp (filter (fn [title]
                                                             (contains? title-ids
                                                                        (:tconst title))))
                                                   (take count))))))

(defn add-directors [db]
  (let [person-ids (->> (db-common/entities db :type :title)
                        (map (fn [entity-id]
                               (db-common/->Entity db imdb-schema entity-id)))
                        (mapcat :directors)
                        (map :entity/id)
                        (into #{}))]
    (db-common/transact db
                        (persons-as-transaction (comp (filter (fn [person]
                                                                (contains? person-ids
                                                                           (:nconst person))))
                                                      #_(take 5))))))

(comment
  (csv/transduce-lines "data/imdb/title.basics.tsv"
                       (take 10)
                       conj
                       [])


  (csv/transduce-maps "data/imdb/title.basics.tsv"
                      {:separator #"\t"}
                      (take 10)
                      conj
                      [])

  (titles-as-transaction (take 10))


  (def db (-> (create-in-memory-db)
              (add-titles 10)))

  (db-common/value db "tt0000002" :primaryTitle)

  (db-common/values db "tt0000002" :genres)

  (db-common/entities db :genres "Short")

  (db-common/->Entity db imdb-schema "tt0000002")

  (:primaryTitle (db-common/->Entity db imdb-schema "tt0000002"))

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
                (db-common/->Entity db imdb-schema entity-id)))))

  (def disk-db-directory "data/temp/imdbdb")

  (do (do (fs/delete-dir (str disk-db-directory "/avtec"))
          (fs/delete-dir (str disk-db-directory "/eatcv"))
          (fs/delete-dir (str disk-db-directory "/full-text"))
          (fs/delete-dir (str disk-db-directory "/transaction-log")))
      (let [db (create-directory-btree-db disk-db-directory)]
        (try
          (-> db
              #_(add-titles 10)
              (btree-db/store-index-roots-after-maximum-number-of-transactions 0)
              (db-common/value "tt0000002" :primaryTitle))
          (finally (btree-db/close! db)))))

  (-> (directory-storage/create (str disk-db-directory "/avtec/nodes"))
      (storage/get-edn-from-storage! "5F260E53A7526D0DCDDDAE26965D0F3207DE37F62FF8C784FC47D1EFACC6F5DF"))

  (-> (directory-storage/create (str disk-db-directory "/eatcv/nodes"))
      (storage/get-edn-from-storage! "FBDC7869347D39568AEBB7D25D768E792965CA3A8C047DA2BD98C24DF95D8000"))

  (-> (directory-storage/create (str disk-db-directory "/full-text/nodes"))
      (storage/get-edn-from-storage! "98CB6A20C5B5587691B202AA4E38DE64E12EF9AB1A74D2A6366E3BC49E970004"
                                     #_"FBDC7869347D39568AEBB7D25D768E792965CA3A8C047DA2BD98C24DF95D8000"))
  (fs/mkdirs "data/temp/storage"))



(defn start-server [directory port]
  (server/start-server (cor-api/app (server-api/create-state (-> (create-directory-btree-db directory)
                                                                 (add-titles 10)))
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
                                  (start-server "data/crud" 4010))))))


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
                               (db-common/->Entity db imdb-schema entity-id)))
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
                (db-common/->Entity db imdb-schema entity-id)))
         #_(map :knownForTitles))
    #_(->> (db-common/entities db :type :title)
           (map (fn [entity-id]
                  (db-common/->Entity db imdb-schema entity-id)))
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


