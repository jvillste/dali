(ns hour-track
  (:require [argumentica.btree :as btree]
            [argumentica.btree-db :as btree-db]
            [argumentica.btree-index :as btree-index]
            [argumentica.csv :as csv]
            [argumentica.db.common :as db-common]
            [argumentica.db.db :as db]
            [argumentica.db.file-transaction-log :as file-transaction-log]
            [argumentica.db.server-api :as server-api]
            [argumentica.directory-storage :as directory-storage]
            [argumentica.sorted-map-transaction-log :as sorted-map-transaction-log]
            [argumentica.sorted-set-index :as sorted-set-index]
            [argumentica.storage :as storage]
            [argumentica.transaction-log :as transaction-log]
            [clojure.set :as set]
            [clojure.string :as string]
            [clojure.test :as t :refer :all]
            [cor.api :as cor-api]
            [cor.server :as server]
            [kixi.stats.core :as stats]
            [me.raynes.fs :as fs]
            [net.cgrand.xforms :as xforms]))

(defn create-directory-btree-db [base-path]
  (db-common/update-indexes-2! (db-common/db-from-index-definitions db-common/base-index-definitions
                                                                    (fn [index-name]
                                                                      (btree-index/create-directory-btree-index (str base-path "/" index-name)
                                                                                                                1001))
                                                                    (file-transaction-log/create (str base-path "/transaction-log")))))

(def disk-db-directory "data/temp/hour-track")

(defn store-index-roots! [db]
  (let [last-transaction-number (transaction-log/last-transaction-number (:transaction-log db))]
    (doseq [index (vals (:indexes db))]
      (btree-index/store-root! (:index index)
                                 last-transaction-number))))

(comment
  (fs/mkdirs disk-db-directory)

  (let [db (create-directory-btree-db disk-db-directory)]
    (store-index-roots! db))

  (let [db (create-directory-btree-db disk-db-directory)]
    (db-common/transact! db #{[1 :name :set "Baz"]}))

  (let [db (create-directory-btree-db disk-db-directory)]
    (db-common/values-from-eatcv-datoms (db-common/datoms-from-index (-> db :indexes :eatcv :index)
                                                                     [1 :name])))

  (do (fs/delete-dir (str disk-db-directory "/avtec"))
      (fs/delete-dir (str disk-db-directory "/eatcv"))
      (fs/delete-dir (str disk-db-directory "/transaction-log"))))
