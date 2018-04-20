(ns crud-server
  (:gen-class)
  (:require [argumentica
             [berkeley-db-transaction-log :as berkeley-db-transaction-log]
             [btree-index :as btree-index]
             [csv :as csv]
             [sorted-map-transaction-log :as sorted-map-transaction-log]
             [sorted-set-index :as sorted-set-index]]
            [argumentica.db
             [common :as db-common]
             [server-api :as server-api]]
            [clojure
             [string :as string]
             [test :as t]]
            [cor
             [api :as cor-api]
             [server :as server]]))

(defn create-directory-btree-db [base-path]
  (db-common/update-indexes (db-common/create :indexes {:eatcv {:index (btree-index/create-directory-btree-index base-path)
                                                                :eatcv-to-datoms db-common/eatcv-to-eatcv-datoms}
                                                        :avtec {:index (btree-index/create-directory-btree-index base-path)
                                                                :eatcv-to-datoms db-common/eatcv-to-avtec-datoms}}

                                              :transaction-log (berkeley-db-transaction-log/create (str base-path "/transaction-log")))))

(defn start-server [directory port]
  (server/start-server (cor-api/app (server-api/create-state (create-directory-btree-db directory))
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


(comment

  (imbdb-file-to-transaction "data/imdb/name.basics.tsv"
                             :nconst
                             (comp (take 10)
                                   (map (fn [title]
                                          (assoc title :type :person)))))

  (let [db (-> (db-common/create :indexes {:eatcv {:index (sorted-set-index/create)
                                                   :eatcv-to-datoms db-common/eatcv-to-eatcv-datoms}
                                           :avtec {:index (sorted-set-index/create)
                                                   :eatcv-to-datoms db-common/eatcv-to-avtec-datoms}}
                                 :transaction-log (sorted-map-transaction-log/create))
               (db-common/transact (titles-as-transaction (take 10)))
               (db-common/transact (crew-as-transaction (take 10))))]
    (->> (db-common/entities db :type :title)
         (map (fn [entity-id]
                (db-common/->Entity db imdb-schema entity-id)))
         (map :directors)))

  (into {} (seq {:a :b}))
  (keys (type (first (seq {:a :b}))))
  (csv/transduce-maps "data/imdb/title.basics.tsv"
                      {:separator #"\t"}
                      (comp (take 2)
                            #_(map postprocess-entity))
                      conj
                      [])

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
                                   [])

  )
