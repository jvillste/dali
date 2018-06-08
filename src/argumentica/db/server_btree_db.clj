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
            [argumentica.db.db :as db]))

(defrecord PeerIndex [local-index
                      remote-index])

(defmethod index/add!
  PeerIndex
  [this first-datom]
  (index/add! (get-in this [:local-index])
              first-datom))

(defmethod index/inclusive-subsequence
  PeerIndex
  [this first-datom]
  (concat (index/inclusive-subsequence (get-in this [:remote-index])
                                       first-datom)
          (index/inclusive-subsequence (get-in this [:local-index])
                                       first-datom)))

(defn create-peer-index [client index-key eatcv-to-datoms]
  (let [latest-root (client/latest-root client
                                        index-key)]
    {:client client
     :index-key index-key
     :eatcv-to-datoms eatcv-to-datoms
     :index (map->PeerIndex {:local-index (sorted-set-index/create)
                             :remote-index (server-btree-index/create client
                                                                      index-key
                                                                      latest-root)})
     :last-indexed-transaction-number (-> latest-root :metadata :last-transaction-number)}))

#_(defn create-remote-and-local-indexes [client index-key eatcv-to-datoms]
  (let [latest-root (client/latest-root client
                                        index-key)]
    {:remote-index {:index (server-btree-index/create client
                                                      index-key
                                                      latest-root)}
     :local-index (create-new-local-index latest-root
                                          client
                                          eatcv-to-datoms)}))

#_(defn index-definition-to-remote-and-local-indexes [index-definition client]
  (into {}
        (map (fn [[key eatcv-to-datoms]]
               [key
                (create-remote-and-local-indexes client key eatcv-to-datoms)])
             index-definition)))



(defn update-peer-index-root [peer-index]
  (let [latest-root (client/latest-root (:client peer-index)
                                        (:index-key peer-index))
        previous-root (-> peer-index :index :remote-index :btree-index-atom deref :latest-root)]

    (if (= latest-root previous-root)
      peer-index
      (-> peer-index
          (update-in [:index :remote-index]
                     (fn [remote-btree-index]
                       (server-btree-index/set-root remote-btree-index
                                                    latest-root)))
          (assoc-in [:index :local-index]
                    (sorted-set-index/create))
          (assoc :last-indexed-transaction-number (-> latest-root :metadata :last-transaction-number))))))

#_(defn update-root [server-btree-db index-key]
  (let [latest-root (client/latest-root (:client server-btree-db)
                                        index-key)
        previous-root (-> server-btree-db :indexes index-key :remote-index :index :btree-index-atom deref :latest-root)]
    (if (= latest-root previous-root)
      server-btree-db
      (-> server-btree-db
          (update-in [:indexes index-key :remote-index :index]
                     (fn [remote-btree-index]
                       (server-btree-index/set-root remote-btree-index
                                                    latest-root)))
          (assoc-in [:indexes index-key :local-index]
                    (create-new-local-index latest-root
                                            (:client server-btree-db)
                                            (-> server-btree-db :indexes index-key :local-index :eatcv-to-datoms)))))))



#_(defn first-unindexed-transaction-number [server-btree-db]
  (->> server-btree-db
       :indexes
       vals
       (map :local-index)
       (map :last-indexed-transaction-number)
       (map (fn [transaction-number]
              (if transaction-number
                (inc transaction-number)
                0)))
       (apply min)))

(defn update-index-roots [server-btree-db]
  (update server-btree-db
          :indexes
          map/map-vals
          update-peer-index-root))

(defn update-indexes [server-btree-db]
  (-> server-btree-db
      (update-index-roots)
      (common/update-indexes))
  
  #_(let [server-btree-db ]
      (reduce (fn [server-btree-db [transaction-number statements]]
                (update server-btree-db
                        :indexes
                        map/map-vals
                        (fn [index]
                          (update index :local-index
                                  common/add-transaction-to-index
                                  server-btree-db
                                  transaction-number
                                  statements))))
              server-btree-db
              (client/transaction-log-subseq (:client server-btree-db)
                                             (first-unindexed-transaction-number server-btree-db)))))

(defn inclusive-subsequence [server-btree-db index-key first-datom]
  (index/inclusive-subsequence (get-in server-btree-db [:indexes index-key :index])
                               first-datom))


(defn index-definition-to-peer-indexes [index-definition client]
  (into {}
        (map (fn [[key eatcv-to-datoms]]
               [key
                (create-peer-index client key eatcv-to-datoms)])
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
  (first (common/values-from-eatcv-statements (datoms server-btree-db entity-id attribute))))

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
