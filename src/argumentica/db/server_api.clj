(ns argumentica.db.server-api
  (:require (argumentica [btree-db :as btree-db]
                         [btree-index :as btree-index]
                         [transaction-log :as transaction-log]
                         [entity :as entity]
                         [btree :as btree]
                         [storage :as storage]
                         [encode :as encode])))

(defn create-state [db]
  {:db db})

(defn ^:cor/api transact [state-atom statements]
  ;; TODO: can not use swap! for transactions because transacting is not idempotent, use for example a go block
  (swap! state-atom
         update :db
         btree-db/transact statements)

  #_(swap! state-atom
           update :db
           btree-db/store-index-roots-after-maximum-number-of-transactions 0)
  nil)

(defn ^:cor/api transaction-log-subseq [state-atom first-transaction-number]
  (transaction-log/subseq (-> @state-atom :db :transaction-log)
                          first-transaction-number))

(defn ^:cor/api last-transaction-number [state-atom]
  (transaction-log/last-transaction-number (-> @state-atom :db :transaction-log)))

(defn btree [state-atom index-key]
  (-> @state-atom
      :db
      :indexes
      index-key
      :index
      btree-index/btree))

(defn ^:cor/api latest-root [state-atom index-key]
  (-> (btree state-atom index-key)
      btree/get-latest-root))

(defn ^:cor/api get-from-node-storage [state-atom index-key storage-key]
  (-> (btree state-atom
             index-key)
      :node-storage
      (storage/get-from-storage! storage-key)
      #_(encode/base-64-encode)))
