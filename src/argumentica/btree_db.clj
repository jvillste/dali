(ns argumentica.btree-db
  (:require [me.raynes.fs :as fs]
            [clojure.string :as string]
            (argumentica.db [common :as common])
            (argumentica [transaction-log :as transaction-log]
                         [entity :as entity]
                         [storage :as storage]
                         [hash-map-storage :as hash-map-storage]
                         [sorted-map-transaction-log :as sorted-map-transaction-log]
                         [berkeley-db-transaction-log :as berkeley-db-transaction-log]
                         [comparator :as comparator]
                         [index :as index]
                         [btree :as btree]
                         [btree-index :as btree-index]
                         [directory-storage :as directory-storage]))

  (:use clojure.test))


(defn store-index-root-after-maximum-number-of-transactions [index last-transaction-number maximum-number-of-transactions-after-previous-flush]
  (when (<= maximum-number-of-transactions-after-previous-flush
            (- last-transaction-number
               (or (-> (btree/get-latest-root (btree-index/btree (:index index)))
                       :metadata
                       :last-transaction-number)
                   0)))
    (btree-index/swap-btree! (:index index)
                             btree/store-root
                             {:last-transaction-number last-transaction-number}))
  index)

(defn store-index-roots-after-maximum-number-of-transactions [db maximum-number-of-transactions-after-previous-root]
  (if-let [last-transaction-number (transaction-log/last-transaction-number (:transaction-log db))]
    (-> db
        (common/apply-to-indexes store-index-root-after-maximum-number-of-transactions
                                 last-transaction-number
                                 maximum-number-of-transactions-after-previous-root)
        (update :transaction-log
                transaction-log/truncate!
                last-transaction-number))
    db))

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
  (common/update-indexes (common/create :indexes {:eatcv {:index (btree-index/create-directory-btree-index base-path)
                                                          :eatcv-to-datoms common/eatcv-to-eatcv-datoms}}
                                        :transaction-log (berkeley-db-transaction-log/create (str base-path "/transaction-log")))))

(defn create-memory-btree-db []
  (common/update-indexes (common/create :indexes {:eatcv {:index (btree-index/create-memory-btree-index)
                                                          :eatcv-to-datoms common/eatcv-to-eatcv-datoms}
                                                  :avtec {:index (btree-index/create-memory-btree-index)
                                                          :eatcv-to-datoms common/eatcv-to-avtec-datoms}}
                                        :transaction-log  (sorted-map-transaction-log/create))))

(defn create-memory-btree-db-from-transaction-log [transaction-log first-transaction-number]
  (-> (common/create :indexes {:eatcv {:index (btree-index/create-memory-btree-index)
                                       :eatcv-to-datoms common/eatcv-to-eatcv-datoms
                                       :last-indexed-transaction-number (dec first-transaction-number)}}
                     :transaction-log transaction-log)
      (common/update-indexes)))

(defn create-memory-btree-db-from-reference [db-reference]
  (common/update-indexes (common/create :indexes {:eatcv {:index (btree-index/create-memory-btree-index-from-btree-index (-> db-reference
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
