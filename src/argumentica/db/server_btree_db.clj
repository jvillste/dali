(ns argumentica.db.server-btree-db
  "server-btree-db holds server-btree-index and corresponding sorted-set-index for transactions that are not yet flushed to
  the server-btree-index."
  (:require (argumentica [btree-db :as btree-db]
                         [sorted-set-index :as sorted-set-index])
            (argumentica.db [common :as common]
                            [client :as client]
                            [server-btree-index :as server-btree-index]
                            [server-transaction-log :as server-transaction-log])))

(defn create-new-local-index [latest-root client eatcv-to-datoms]
  (common/update-index {:index (sorted-set-index/create)
                        :last-indexed-transaction-number (-> latest-root :metadata :last-transaction-number)
                        :eatcv-to-datoms eatcv-to-datoms}
                       (server-transaction-log/->ServerTransactionLog client)))

(defn create-index [client index-key eatcv-to-datoms]
  (let [latest-root (client/latest-root client
                                        index-key)]
    {:remote-index {:index (server-btree-index/create client
                                                      index-key
                                                      latest-root)}
     :local-index (create-new-local-index latest-root
                                          client
                                          eatcv-to-datoms)}))

(defn create [client]
  {:client client
   :last-indexed-transaction-number (client/last-transaction-number client)
   :indexes {:eatcv (create-index client :eatcv common/eatcv-to-eatcv-datoms)
             :avtec (create-index client :avtec common/eatcv-to-avtec-datoms)}})

(defn update-index [server-btree-db index-key]
  (let [latest-root (client/latest-root (:client server-btree-db)
                                        index-key)
        previous-root (get-in server-btree-db [:indexes index-key :remote-index])]
    (if (= latest-root previous-root)
      (-> server-btree-db
          (update-in [:indexes index-key :local-index]
                     (fn [local-index]
                       (common/update-index local-index
                                            (server-transaction-log/->ServerTransactionLog (:client server-btree-db))))))
      (-> server-btree-db
          (update-in [:indexes index-key :remote-index :index]
                     (fn [remote-btree-index]
                       (server-btree-index/set-root remote-btree-index
                                                    latest-root)))
          (assoc-in [:indexes index-key :local-index]
                    (create-new-local-index latest-root
                                            (:client server-btree-db)
                                            (-> server-btree-db :indexes index-key :local-index :eatcv-to-datoms)))))))

(defn update [server-btree-db]
  (reduce (fn [server-btree-db index-key]
            (update-index server-btree-db
                          index-key))
          (assoc server-btree-db
                 :last-indexed-transaction-number (client/last-transaction-number (:client server-btree-db)))
          (keys (:indexes server-btree-db))))

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
