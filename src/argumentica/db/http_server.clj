(ns argumentica.db.http-server
  (:require (argumentica [btree-db :as btree-db])
            (argumentica.db [server-api :as server-api])
            (cor [server :as server]
                 [api :as api]))
  (:use clojure.test))

(defn start []
  (server/start-server (api/app (server-api/create-state (btree-db/create-memory-btree-db))
                                'argumentica.db.server-api)
                       9999))


