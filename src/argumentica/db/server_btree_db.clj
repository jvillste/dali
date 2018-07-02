(ns argumentica.db.server-btree-db
  "server-btree-db holds server-btree-index and corresponding sorted-set-index for transactions that are not yet flushed to
  the server-btree-index."
  (:require (argumentica [btree-db :as btree-db]
                         [sorted-set-index :as sorted-set-index])
            (argumentica.db [common :as common]
                            [client :as client]
                            [server-btree-index :as server-btree-index]
                            [server-transaction-log :as server-transaction-log])
            [argumentica.index :as index]
            [flatland.useful.map :as map]
            [argumentica.transaction-log :as transaction-log]
            [argumentica.db.db :as db]
            [argumentica.db.peer-index :as peer-index]))

(defn update-index-roots [server-btree-db]
  (update server-btree-db
          :indexes
          map/map-vals
          peer-index/update-root))

(defn update-indexes [server-btree-db]
  (-> server-btree-db
      (update-index-roots)
      (common/update-indexes)))

(defn inclusive-subsequence [server-btree-db index-key first-datom]
  (index/inclusive-subsequence (get-in server-btree-db [:indexes index-key :index])
                               first-datom))

(defn index-definition-to-peer-indexes [index-definition client]
  (into {}
        (map (fn [[key eatcv-to-datoms]]
               [key
                (peer-index/create client key eatcv-to-datoms)])
             index-definition)))

(defrecord ServerBtreeDb [client transaction-log indexes])

(extend ServerBtreeDb
  db/ReadableDB
  {:inclusive-subsequence inclusive-subsequence})

(defn create [client index-definition]
  (update-indexes (map->ServerBtreeDb {:client client
                                       :transaction-log (server-transaction-log/->ServerTransactionLog client)
                                       :indexes (index-definition-to-peer-indexes index-definition
                                                                                  client)})))

(defn datoms [server-btree-db entity-id attribute]
  (concat (common/eat-datoms-from-eatcv (get-in server-btree-db [:indexes :eatcv :remote-index :index])
                                        entity-id
                                        attribute
                                        nil #_(:last-indexed-transaction-number server-btree-db))

          (common/eat-datoms-from-eatcv (get-in server-btree-db [:indexes :eatcv :local-index :index])
                                        entity-id
                                        attribute
                                        nil #_(:last-indexed-transaction-number server-btree-db))))


(defn value [server-btree-db entity-id attribute]
  (first (common/values-from-eatcv-datoms (datoms server-btree-db entity-id attribute))))

(defn avtec-datoms [server-btree-db attribute value]
  (concat (common/avtec-datoms-from-avtec (get-in server-btree-db [:indexes :avtec :remote-index :index])
                                          attribute
                                          value
                                          (fn [other-value]
                                            (= value other-value))
                                          (:last-indexed-transaction-number server-btree-db))

          (common/avtec-datoms-from-avtec (get-in server-btree-db [:indexes :avtec :local-index :index])
                                          attribute
                                          value
                                          (fn [other-value]
                                            (= value other-value))
                                          (:last-indexed-transaction-number server-btree-db))))

(defn entities [server-btree-db attribute value]
  (first (common/entities-from-avtec-datoms (avtec-datoms server-btree-db attribute value))))
