(ns argumentica.db.server-btree-db-test
  (:require [argumentica.btree-db :as btree-db]
            [argumentica.btree-index :as btree-index]
            [argumentica.db.client :as client]
            [argumentica.db.common :as db-common]
            [argumentica.db.server-api :as server-api]
            [argumentica.db.server-btree-db :as server-btree-db]
            [argumentica.sorted-map-transaction-log :as sorted-map-transaction-log]
            [clojure.test :refer :all]
            [argumentica.index :as index]
            [argumentica.btree :as btree]))

(def index-definition {:eatcv db-common/eatcv-to-eatcv-datoms
                       :avtec db-common/eatcv-to-avtec-datoms})

(deftest test-create
  (let [server-db (-> (db-common/db-from-index-definition index-definition
                                                          (fn [_index-name] (btree-index/create-memory-btree-index 21))
                                                          (sorted-map-transaction-log/create))
                      (db-common/transact [[:entity-1 :name :set "Name 1"]])
                      (btree-db/store-index-roots-after-maximum-number-of-transactions 0)
                      (db-common/transact [[:entity-1 :name :set "Name 2"]]))
        server-state-atom (atom (server-api/create-state server-db))
        client (client/->InProcessClient server-state-atom)
        server-btree-db (server-btree-db/create client index-definition)]
    (is (= 1
           (-> server-db :indexes :eatcv :last-indexed-transaction-number)))

    (is (= '([:entity-1 :name 0 :set "Name 1"])
           (btree/inclusive-subsequence (-> server-btree-db :indexes :eatcv :remote-index :index :btree-index-atom)
                                        nil)))

    (is (= #{[:entity-1 :name 1 :set "Name 2"]}
           @(-> server-btree-db :indexes :eatcv :local-index :index :sorted-set-atom)))

    (is (= '([:entity-1 :name 0 :set "Name 1"]
             [:entity-1 :name 1 :set "Name 2"])
           (server-btree-db/inclusive-subsequence server-btree-db
                                                  :eatcv
                                                  nil)))
    (client/transact client
                     [[:entity-1 :name :set "Name 3"]])

    (is (= '([:entity-1 :name 0 :set "Name 1"]
             [:entity-1 :name 1 :set "Name 2"])
           (server-btree-db/inclusive-subsequence server-btree-db
                                                  :eatcv
                                                  nil)))

    (let [server-btree-db-2 (server-btree-db/update server-btree-db)]
      (is (= '([:entity-1 :name 0 :set "Name 1"]
               [:entity-1 :name 1 :set "Name 2"]
               [:entity-1 :name 2 :set "Name 3"])
             (server-btree-db/inclusive-subsequence server-btree-db-2
                                                    :eatcv
                                                    nil)))

      (is (= #{[:entity-1 :name 1 :set "Name 2"]
               [:entity-1 :name 2 :set "Name 3"]}
             @(-> server-btree-db-2 :indexes :eatcv :local-index :index :sorted-set-atom)))

      (swap! server-state-atom
             update :db
             btree-db/store-index-roots-after-maximum-number-of-transactions 0)

      (let [server-btree-db-3 (server-btree-db/update server-btree-db)]
        (is (= #{}
               @(-> server-btree-db-3 :indexes :eatcv :local-index :index :sorted-set-atom)))

        (is (= (client/latest-root client :eatcv)
               (-> server-btree-db-3 :indexes :eatcv :remote-index :index :btree-index-atom deref :latest-root)))

        (is (= (:storage-key (client/latest-root client :eatcv))
               (-> server-btree-db-3 :indexes :eatcv :remote-index :index :btree-index-atom deref :root-id)))

        (is (= '([:entity-1 :name 0 :set "Name 1"]
                 [:entity-1 :name 1 :set "Name 2"]
                 [:entity-1 :name 2 :set "Name 3"])
               (btree/inclusive-subsequence (-> server-btree-db-3 :indexes :eatcv :remote-index :index :btree-index-atom)
                                            nil)))
        
        (is (= '([:entity-1 :name 0 :set "Name 1"]
                 [:entity-1 :name 1 :set "Name 2"]
                 [:entity-1 :name 2 :set "Name 3"])
               (server-btree-db/inclusive-subsequence server-btree-db-3 :eatcv nil)))))))

(deftest test2-server-btree-db
    (let [server-state-atom (atom (server-api/create-state (btree-db/create-memory-btree-db)))
          client (client/->InProcessClient server-state-atom)
          server-btree-db (server-btree-db/create client
                                                  index-definition)
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
