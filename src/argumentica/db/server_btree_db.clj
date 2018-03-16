(ns argumentica.db.server-btree-db
  (:require (argumentica [btree-db :as btree-db]
                         [sorted-set-index :as sorted-set-index])
            (argumentica.db [common :as common]
                            [client :as client]
                            [server-btree-index :as server-btree-index]
                            [server-transaction-log :as server-transaction-log])))

#_(defn create [client]
  (assoc (common/create :indexes {:eatcv {:index (server-btree-index/create client
                                                                            :eatcv
                                                                            (client/latest-root client
                                                                                                :eatcv))}})
         :last-indexed-transaction-number (-> (client/latest-root client :eatcv)
                                              :metadata
                                              :last-transaction-number)))

(defn create-new-local-index [latest-root client]
  (common/update-index {:index (sorted-set-index/create)
                        :last-indexed-transaction-number (-> latest-root :metadata :last-transaction-number)
                        :eatcv-to-datoms common/eatcv-to-eatcv-datoms} 
                       (server-transaction-log/->ServerTransactionLog client)))

(defn create [client]
  {:client client
   :last-indexed-transaction-number (client/last-transaction-number client)
   :indexes {:eatcv (let [latest-root (client/latest-root client
                                                          :eatcv)]
                        
                      {:remote-index {:index (server-btree-index/create client
                                                                        :eatcv
                                                                        latest-root)} 
                         
                       :local-index (create-new-local-index latest-root
                                                            client)})}})

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
                                            (:client server-btree-db)))))))

(defn update [server-btree-db]
  (-> server-btree-db
      (assoc :last-indexed-transaction-number (client/last-transaction-number (:client server-btree-db)))
      (update-index :eatcv)))

(defn datoms [server-btree-db entity-id attribute]
  (concat (common/eat-datoms-from-eatcv (get-in server-btree-db [:indexes :eatcv :remote-index :index])
                                        entity-id
                                        attribute
                                        (:last-indexed-transaction-number server-btree-db))
          
          (common/eat-datoms-from-eatcv (get-in server-btree-db [:indexes :eatcv :local-index :index])
                                        entity-id
                                        attribute
                                        (:last-indexed-transaction-number server-btree-db))))


(defn value [server-btree-db entity-id attribute]
  (first (common/values-from-eatcv-statements (datoms server-btree-db entity-id attribute))))
