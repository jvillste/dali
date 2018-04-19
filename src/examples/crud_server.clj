(ns crud-server
  (:require [cor.server :as server]
            [cor.api :as cor-api]
            (argumentica.db [client :as client]
                            [common :as db-common]
                            [server-api :as server-api]
                            [server-btree-db :as server-btree-db]
                            [client-db :as client-db])
            (argumentica [btree-db :as btree-db]
                         [berkeley-db-transaction-log :as berkeley-db-transaction-log]
                         [btree-index :as btree-index]
                         [csv :as csv])
            [clojure.string :as string]
            [clojure.test :as t])
  (:gen-class))

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
               :reference true}})

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

(mapcat (fn [[a b]]
          [a b])
        (dissoc {:a :b :c :d}
                :a))

(comment
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
                      (comp (filter (fn [value]
                                      (= "nm0005690"
                                         (:nconst value))))
                            (take 1))
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
