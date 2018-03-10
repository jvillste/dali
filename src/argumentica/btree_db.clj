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
               (or (-> (btree/get-latest-root (:index index))
                       :metadata
                       :last-transaction-number)
                   0)))
    (btree-index/swap-btree! (:index index)
                             btree/store-root
                             {:last-transaction-number last-transaction-number}))
  index)

(defn store-index-roots-after-maximum-number-of-transactions [db maximum-number-of-transactions-after-previous-root]
  (common/apply-to-indexes db
                           store-index-root-after-maximum-number-of-transactions
                           (transaction-log/last-transaction-number (:transaction-log db))
                           maximum-number-of-transactions-after-previous-root))

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
                                                          :eatcv-to-datoms common/eatcv-to-eatcv-datoms}}
                                        :transaction-log  (sorted-map-transaction-log/create))))

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
  (berkeley-db-transaction-log/close! (:transaction-log disk-db)))


;; test runs

(defn- write-disk-db [statements]
  (let [db (create-directory-btree-db "data/db")]
    (try
      (transact db statements)
      (finally
        (close! db)))))

(defn- read-disk-db []
  (let [db (create-directory-btree-db "data/db")]
    (try
      (value db :entity-1 :name)
      (finally
        (close! db)))))

(defn- flush-disk-db []
  (let [db (create-directory-btree-db "data/db")]
    (try
      (store-index-roots-after-maximum-number-of-transactions db 0)
      (finally
        (close! db)))))

(comment
  (import [java.nio.file Files Paths])
  (storage/byte-array-to-edn (Files/readAllBytes (Paths/get "data/db/metadata/roots"
                                                            #_"data/db/metadata/2F765674B333B1C14B03B28CCE08706C338F9C945B2E1CC5622E72BD6F3F8413"
                                                            #_"data/db/nodes/2F765674B333B1C14B03B28CCE08706C338F9C945B2E1CC5622E72BD6F3F8413"
                                                            (into-array String [])))))

(defn- start []
  #_(fs/delete-dir "data/db")
  #_(fs/mkdir "data/db")

  #_(write-disk-db [(common/set-statement :enitty-1 :name "Bar")])
  #_(flush-disk-db)
  (read-disk-db))
