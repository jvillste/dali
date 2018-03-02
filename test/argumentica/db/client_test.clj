(ns argumentica.db.client-test
  (:require (argumentica [sorted-set-db :as sorted-set-db]
                         [btree-db :as btree-db]
                         [btree-index :as btree-index]
                         [transaction-log :as transaction-log]
                         [btree :as btree]
                         [encode :as encode])
            (argumentica.db [common :as common]
                            [server-api :as server-api]
                            [in-process-client :as in-process-client]
                            [client :as client])
            (cor [server :as server]
                 [api :as api]))
  (:use clojure.test))

(deftest test
  (let [state-atom (atom (server-api/create-state (btree-db/create-memory-btree-db)))
        client (client/->InProcessClient state-atom)
        entity-id  #uuid "adcba48b-b9a9-4c28-b1e3-3a97cb10cffb"]
    (client/transact client
                     [[entity-id :name :set "Foo"]])

    (is (= '([0 [[#uuid "adcba48b-b9a9-4c28-b1e3-3a97cb10cffb"
                  :name
                  :set
                  "Foo"]]])
           (client/transaction-log-subseq client
                                          0)))
    
    (btree-db/store-index-roots (-> client :state-atom deref :db))
    
    (is (= 0
           (-> (client/latest-root client :eatcv)
               :metadata
               :last-transaction-number)))

    (is (= {:values
            #{[#uuid "adcba48b-b9a9-4c28-b1e3-3a97cb10cffb"
               :name
               0
               :set
               "Foo"]}}
           (->> (client/latest-root client :eatcv)
                :storage-key
                (client/get-from-node-storage client :eatcv)
                (encode/base-64-decode)
                (btree/bytes-to-node))))))
