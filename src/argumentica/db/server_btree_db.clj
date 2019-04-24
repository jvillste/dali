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
            [argumentica.db.index-atom :as index-atom]
            [argumentica.db.peer-index :as peer-index]
            [argumentica.db.sorted-datom-set-branch :as sorted-datom-set-branch]
            [argumentica.branch-transaction-log :as branch-transaction-log]
            [argumentica.sorted-map-transaction-log :as sorted-map-transaction-log]))

(defn- update-index-roots! [server-btree-db]
  (update server-btree-db
          :indexes
          map/map-vals
          (fn [index-map]
            (let [latest-root (client/latest-root (:client server-btree-db)
                                                  (:key index-map))]
              (if (not= (-> latest-root :metadata :last-transaction-number)
                        (-> index-map :index :index-atom deref :base-transaction-number))
                (index-atom/swap-index! (:index index-map)
                                        (fn [sorted-datom-set-branch]
                                          (-> (sorted-datom-set-branch/create (server-btree-index/set-root (:base-sorted-datom-set sorted-datom-set-branch)
                                                                                                           latest-root)
                                                                              (-> latest-root :metadata :last-transaction-number)
                                                                              (:datom-transaction-number-index index-map)
                                                                              (sorted-set-index/create))
                                              (assoc :last-indexed-transaction-number (-> latest-root :metadata :last-transaction-number)))))
                index-map)))))

(defn last-indexed-transaction-number-for-index-map [index-map]
 (-> index-map :index :index-atom deref :last-indexed-transaction-number))

(defn first-unindexed-transacion-number-for-index-map [index-map]
  (if-let [last-indexed-transaction-number (last-indexed-transaction-number-for-index-map index-map)]
    (inc last-indexed-transaction-number)
    0))

(defn first-unindexed-transaction [server-btree-db]
  (->> server-btree-db
       :indexes
       vals
       (map first-unindexed-transacion-number-for-index-map)
       (apply min)))

(defn add-transaction-to-index-map! [index-map indexes transaction-number statements]
  (when (< (last-indexed-transaction-number-for-index-map index-map)
           transaction-number)
    (do (common/add-transaction-to-index! index-map indexes transaction-number statements)
        (index-atom/swap-index! (:index index-map)
                                assoc
                                :last-indexed-transaction-number
                                transaction-number))))

(defn add-transactions-to-indexes! [indexes transactions]
  (doseq [[transaction-number statements] transactions]
    (doseq [index-map (vals indexes)]
      (add-transaction-to-index-map! index-map
                                     indexes
                                     transaction-number
                                     statements))))

(defn- update-indexes [server-btree-db]
  (let [first-unindexed-transaction (first-unindexed-transaction server-btree-db)
        transactions (transaction-log/subseq (:transaction-log server-btree-db)
                                             first-unindexed-transaction)
        last-transaction-number (first (last transactions))]
    (add-transactions-to-indexes! (:indexes server-btree-db)
                                  transactions)
    (assoc server-btree-db :last-transaction-number (or last-transaction-number
                                                        (dec first-unindexed-transaction)))))

(defn- deref-db [server-btree-db]
  (update-index-roots! server-btree-db)
  (update-indexes server-btree-db))

(defn inclusive-subsequence [server-btree-db index-key first-datom]
  (index/inclusive-subsequence (get-in server-btree-db [:indexes index-key :index])
                               first-datom))


(defn index-definition-to-index-map [client index-definition]
  (let [latest-root (client/latest-root client
                                        (:key index-definition))]
    (merge index-definition
           {:index (index-atom/create (-> (sorted-datom-set-branch/create (server-btree-index/create client
                                                                                                     (:key index-definition)
                                                                                                     latest-root)
                                                                          (-> latest-root :metadata :last-transaction-number)
                                                                          (:datom-transaction-number-index index-definition)
                                                                          (sorted-set-index/create))
                                          (assoc :last-indexed-transaction-number (-> latest-root :metadata :last-transaction-number))))})))

(defn index-definitions-to-indexes [index-definitions client]
  (reduce (fn [index-map index-definition]
            (assoc index-map
                   (:key index-definition)
                   (index-definition-to-index-map client index-definition)))
          {}
          index-definitions))

(defrecord ServerBtreeDb [client transaction-log indexes]
  clojure.lang.IDeref
  (deref [this] (deref-db this)))

(extend ServerBtreeDb
  db/ReadableDB
  {:inclusive-subsequence inclusive-subsequence})


(defn create [client index-definitions]
  (map->ServerBtreeDb (update-indexes {:client client
                                       :transaction-log (server-transaction-log/->ServerTransactionLog client)
                                       :indexes (index-definitions-to-indexes index-definitions
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
