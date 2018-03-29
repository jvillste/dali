(ns argumentica.db.server-api-test
  (:require (argumentica [sorted-set-db :as sorted-set-db]
                         [btree-db :as btree-db]
                         [btree-index :as btree-index]
                         [transaction-log :as transaction-log]
                         [entity :as entity]
                         [btree :as btree]
                         [storage :as storage]
                         [encode :as encode])
            (argumentica.db [common :as common]
                            [server-api :as server-api])
            (cor [server :as server]
                 [api :as api]))
  (:use clojure.test))


(deftest test
  (let [state-atom (atom (server-api/create-state (btree-db/create-memory-btree-db)))
        entity-id  #uuid "adcba48b-b9a9-4c28-b1e3-3a97cb10cffb"]
    (server-api/transact state-atom
                         [[entity-id :name :set "Foo"]])
    (is (= '([0 [[#uuid "adcba48b-b9a9-4c28-b1e3-3a97cb10cffb"
                  :name
                  :set
                  "Foo"]]])
           (server-api/transaction-log-subseq state-atom
                                              0)))
    
    (btree-db/store-index-roots (-> @state-atom :db))
    
    (is (= 0
           (-> (server-api/latest-root state-atom :eatcv)
               :metadata
               :last-transaction-number)))

    (is (= {:values
            #{[#uuid "adcba48b-b9a9-4c28-b1e3-3a97cb10cffb"
               :name
               0
               :set
               "Foo"]}}
           (->> (server-api/latest-root state-atom :eatcv)
                :storage-key
                (server-api/get-from-node-storage state-atom :eatcv)
                #_(encode/base-64-decode)
                (btree/bytes-to-node))))))
