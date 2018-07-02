(ns argumentica.db.server-connection-test
  (:require [argumentica.db.server-connection :as server-connection]
            [clojure.string :as string]
            [argumentica.db.common :as db-common]
            [argumentica.btree-db :as btree-db]
            [argumentica.btree-index :as btree-index]
            [argumentica.sorted-map-transaction-log :as sorted-map-transaction-log]
            [argumentica.db.server-api :as server-api]
            [argumentica.db.client :as client]
            [argumentica.index :as index])
  (:use [clojure.test]))

(deftest test
  (let [index-definition {:eatcv db-common/eatcv-to-eatcv-datoms
                          :full-text (partial db-common/eatcv-to-full-text-avtec (fn [string]
                                                                                   (->> (string/split string #" ")
                                                                                        (map string/lower-case))))}
        transaction-log (sorted-map-transaction-log/create)
        
        
        server-db (-> (db-common/db-from-index-definition index-definition
                                                          (fn [_index-name] (btree-index/create-memory-btree-index 21))
                                                          (sorted-map-transaction-log/create))
                      (db-common/transact [[:entity-1 :name :set "Name 1"]])
                      (btree-db/store-index-roots-after-maximum-number-of-transactions 0)
                      (db-common/transact [[:entity-1 :name :set "Name 2"]])
                      (db-common/transact [[:entity-1 :name :set "Name 3"]]))
        server-state-atom (atom (server-api/create-state server-db))
        client (client/->InProcessClient server-state-atom)
        server-connection (server-connection/create client
                                                    index-definition)
        db @server-connection]

    (is (= nil
           db #_(index/inclusive-subsequence (get-in db [:indexes :eatcv])
                                        nil)))

    ))
