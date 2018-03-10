(ns argumentica.db.server-btree-db
  (:require [me.raynes.fs :as fs]
            [clojure.string :as string]
            (argumentica.db [common :as common]
                            [client :as client]
                            [server-transaction-log :as server-transaction-log]
                            [server-btree-index :as server-btree-index])
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

(defn create [client]
  (common/update-indexes (common/create :indexes {:eatcv {:index (server-btree-index/create client
                                                                                            :eatcv
                                                                                            (client/latest-root client
                                                                                                                :eatcv))
                                                          :eatcv-to-datoms common/eatcv-to-eatcv-datoms}}
                                        :transaction-log  (server-transaction-log/->ServerTransactionLog client))))


