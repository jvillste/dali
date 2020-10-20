(ns argumentica.btree-db
  (:require [argumentica.btree :as btree]
            [argumentica.btree-collection :as btree-collection]
            [argumentica.btree-index :as btree-index]
            [argumentica.db.common :as common]
            [argumentica.db.file-transaction-log :as file-transaction-log]
            [argumentica.sorted-map-transaction-log :as sorted-map-transaction-log]
            [argumentica.transaction-log :as transaction-log]
            [clojure.test :refer :all]))

(defn store-index-root-after-maximum-number-of-transactions [index last-transaction-number maximum-number-of-transactions-after-previous-flush]
  (when (<= maximum-number-of-transactions-after-previous-flush
            (- last-transaction-number
               (or (-> (btree/get-latest-root (btree-collection/btree (:collection index)))
                       :metadata
                       :last-transaction-number)
                   0)))
    (btree-collection/locking-apply-to-btree! (:collection index)
                                              btree/store-root-2
                                              {:last-transaction-number last-transaction-number}))
  index)

(defn store-index-roots-after-maximum-number-of-transactions [db maximum-number-of-transactions-after-previous-root]
  (if-let [last-transaction-number (transaction-log/last-transaction-number (:transaction-log db))]
    (common/apply-to-indexes db
                             store-index-root-after-maximum-number-of-transactions
                             last-transaction-number
                             maximum-number-of-transactions-after-previous-root)
    db))

(defn- apply-to-btrees [db function & arguments]
  (common/apply-to-indexes db
                           (fn [index]
                             (apply btree-collection/locking-apply-to-btree!
                                    (:collection index)
                                    function
                                    arguments)
                             index)))

(defn remove-old-roots [db]
  (apply-to-btrees db
                   btree/remove-old-roots-2))

(defn unload-nodes [db maximum-loaded-node-count]
  (apply-to-btrees db
                   btree/unload-excess-nodes
                   maximum-loaded-node-count))

(defn collect-storage-garbage [db]
  (apply-to-btrees db
                   btree/collect-storage-garbage))

(defn store-index-roots [db]
  (store-index-roots-after-maximum-number-of-transactions db 0))

(defn transact [db statements]
  (-> db
      (common/transact statements)
      (store-index-roots-after-maximum-number-of-transactions 100)))


(defn set [db entity attribute value]
  (transact db
            [[entity attribute :set value]]))


(defn value [db entity-id attribute]
  (common/value db
                entity-id
                attribute
                (common/last-transaction-number db)))



(defn create-directory-btree-db [base-path]
  (common/update-indexes (common/create :indexes {:eatcv {:collection (btree-index/create-directory-btree-index base-path)
                                                          :eatcv-to-datoms common/eatcv-to-eatcv-datoms}}
                                        :transaction-log (file-transaction-log/create (str base-path "/transaction-log")))))

(defn create-memory-btree-db []
  (common/update-indexes (common/create :indexes {:eatcv {:collection (btree-index/create-memory-btree-index 10001)
                                                          :eatcv-to-datoms common/eatcv-to-eatcv-datoms}
                                                  :avtec {:collection (btree-index/create-memory-btree-index 10001)
                                                          :eatcv-to-datoms common/eatcv-to-avtec-datoms}}
                                        :transaction-log  (sorted-map-transaction-log/create))))

(defn create-memory-btree-db-from-transaction-log [transaction-log first-transaction-number]
  (-> (common/create :indexes {:eatcv {:collection (btree-index/create-memory-btree-index 10001)
                                       :eatcv-to-datoms common/eatcv-to-eatcv-datoms
                                       :last-indexed-transaction-number (dec first-transaction-number)}}
                     :transaction-log transaction-log)
      (common/update-indexes)))

(defn create-memory-btree-db-from-reference [db-reference]
  (common/update-indexes (common/create :indexes {:eatcv {:collection (btree-index/create-memory-btree-index-from-btree-index (-> db-reference
                                                                                                                             :indexes
                                                                                                                             :eatcv
                                                                                                                             :index))
                                                          :eatcv-to-datoms common/eatcv-to-eatcv-datoms}}
                                        :transaction-log  (:transaction-log db-reference))))

(deftest test-create-memory-btree-db-from-reference
  (is (= "Foo"
         (-> (create-memory-btree-db)
             (set :entity-1 :name "Foo")
             (create-memory-btree-db-from-reference)
             (value :entity-1 :name)))))

(defn close! [disk-db]
  (transaction-log/close! (:transaction-log disk-db)))


;; test runs
(def disk-db-directory "data/imdbdb")

(defn- write-disk-db [statements]
  (let [db (create-directory-btree-db disk-db-directory)]
    (try
      (transact db statements)
      #_(store-index-roots-after-maximum-number-of-transactions db 0)
      (finally
        (close! db)))))

(defn- read-disk-db []
  (let [db (create-directory-btree-db disk-db-directory)]
    (try
      (value db :entity-1 :name)
      (finally
        (close! db)))))

(defn- flush-disk-db []
  (let [db (create-directory-btree-db disk-db-directory)]
    (try
      (store-index-roots-after-maximum-number-of-transactions db 0)
      (finally
        (close! db)))))


(defn- start []
  #_(fs/delete-dir "data/db")
  #_(fs/mkdir "data/db")

  #_(write-disk-db [(common/set-statement :enitty-1 :name "Bar")])
  #_(flush-disk-db)
  #_(-> (directory-storage/create "data/db/nodes")
      (storage/get-edn-from-storage! "151B32735139E469E921267A6A9C9B9B08EC6E726C90A778A0D854207E7454B3"))
  (read-disk-db))
