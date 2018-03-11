(ns argumentica.server-btree-db-test
  (:require (argumentica [btree-db :as btree-db]
                         [index :as index])
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

    (is (= '([0 [[#uuid "adcba48b-b9a9-4c28-b1e3-3a97cb10cffb" :name :set "Foo"]]]
             [1 [[#uuid "adcba48b-b9a9-4c28-b1e3-3a97cb10cffb" :name :set "Bar"]]])
           (client/transaction-log-subseq client 0)))

    (swap! server-state-atom
           update :db
           btree-db/store-index-roots)
    
    (is (= nil
           (index/inclusive-subsequence (-> server-btree-db :indexes :eatcv :index)
                                        nil)))

    (let [server-btree-db (server-btree-db/create client)]
      (is (= '([#uuid "adcba48b-b9a9-4c28-b1e3-3a97cb10cffb" :name 0 :set "Foo"]
               [#uuid "adcba48b-b9a9-4c28-b1e3-3a97cb10cffb" :name 1 :set "Bar"])
             (index/inclusive-subsequence (-> server-btree-db :indexes :eatcv :index)
                                          nil)))

      (is (= "Bar"
           (btree-db/value server-btree-db
                           entity-id
                           :name))))))
