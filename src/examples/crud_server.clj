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
                         [btree-index :as btree-index]))
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
