(ns argumentica.db.client-test
  (:require [argumentica.btree :as btree]
            [argumentica.btree-db :as btree-db]
            [argumentica.db.client :as client]
            [argumentica.db.server-api :as server-api]
            [argumentica.encode :as encode]
            [clojure.test :refer :all]))

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
