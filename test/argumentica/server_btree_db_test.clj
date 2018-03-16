(ns argumentica.server-btree-db-test
  (:require (argumentica [btree-db :as btree-db]
                         [index :as index])
            (argumentica.db [client :as client]
                            [common :as common]
                            [server-api :as server-api]
                            [server-btree-db :as server-btree-db]
                            [server-transaction-log :as server-transaction-log]))
  (:use clojure.test))


(deftest test2-server-btree-db
  (let [server-state-atom (atom (server-api/create-state (btree-db/create-memory-btree-db)))
        client (client/->InProcessClient server-state-atom)
        server-btree-db (server-btree-db/create client)
        entity-id  #uuid "adcba48b-b9a9-4c28-b1e3-3a97cb10cffb"]
    (server-api/transact server-state-atom
                         [[entity-id :name :set "Foo"]])
    (server-api/transact server-state-atom
                         [[entity-id :name :set "Bar"]])

    (swap! server-state-atom
           update :db
           btree-db/store-index-roots)
    
    (is (= nil
           (server-btree-db/value server-btree-db
                                  entity-id
                                  :name)))

    (let [server-btree-db-2 (server-btree-db/update server-btree-db)]
      (is (= '([#uuid "adcba48b-b9a9-4c28-b1e3-3a97cb10cffb" :name 0 :set "Foo"]
               [#uuid "adcba48b-b9a9-4c28-b1e3-3a97cb10cffb" :name 1 :set "Bar"])
             (server-btree-db/datoms server-btree-db-2
                                     entity-id
                                     :name)))

      (is (= "Bar"
             (server-btree-db/value server-btree-db-2
                                    entity-id
                                    :name)))

      (server-api/transact server-state-atom
                           [[entity-id :name :set "Baz"]])

      
      (let [server-btree-db-3 (server-btree-db/update server-btree-db)]
        
        (is (= '([#uuid "adcba48b-b9a9-4c28-b1e3-3a97cb10cffb" :name 0 :set "Foo"]
                 [#uuid "adcba48b-b9a9-4c28-b1e3-3a97cb10cffb" :name 1 :set "Bar"]
                 [#uuid "adcba48b-b9a9-4c28-b1e3-3a97cb10cffb" :name 2 :set "Baz"])
               (server-btree-db/datoms server-btree-db-3
                                       entity-id
                                       :name)))
        (is (= "Baz"
               (server-btree-db/value server-btree-db-3
                                      entity-id
                                      :name)))))))
