(ns argumentica.server-btree-db-test
  (:require (argumentica [btree-db :as btree-db])
            (argumentica.db [client :as client]
                            [server-api :as server-api]
                            [server-btree-db :as server-btree-db]))
  (:use clojure.test))

(deftest test-server-btree-db
  (let [server-state-atom (atom (server-api/create-state (btree-db/create-memory-btree-db)))
        client (client/->InProcessClient server-state-atom)
        server-btree-db (server-btree-db/create client)
        entity-id  #uuid "adcba48b-b9a9-4c28-b1e3-3a97cb10cffb"]
    (server-api/transact server-state-atom
                         [[entity-id :name :set "Foo"]])
    (server-api/transact server-state-atom
                         [[entity-id :name :set "Bar"]])

    (is (= "Bar"
           (btree-db/value server-btree-db
                           entity-id
                           :name)))))
