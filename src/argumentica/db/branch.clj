(ns argumentica.db.branch
  (:require [clojure.test :as t]
            [argumentica.db.common :as common]
            [argumentica.sorted-set-index :as sorted-set-index]
            [argumentica.sorted-map-transaction-log :as sorted-map-transaction-log]
            [argumentica.db.sorted-datom-set-branch :as sorted-datom-set-branch]
            [argumentica.branch-transaction-log :as branch-transaction-log]
            [argumentica.transaction-log :as transaction-log]
            [argumentica.db.db :as db]))

(defrecord Branch []
  clojure.lang.IDeref
  (deref [this] (assoc this :last-transaction-number (transaction-log/last-transaction-number (:transaction-log this))))
  db/WriteableDB
  (transact [this statements]
    (common/transact! this statements)))

(defn create [base-database-value]
  (map->Branch (common/db-from-index-definitions (map common/index-to-index-definition (vals (:indexes base-database-value)))
                                                 (fn [key]
                                                   (sorted-datom-set-branch/create (-> base-database-value :indexes key :index)
                                                                                   (:last-transaction-number base-database-value)
                                                                                   (-> base-database-value :indexes key :datom-transaction-number-index)
                                                                                   (sorted-set-index/create)))
                                                 (branch-transaction-log/create (:transaction-log base-database-value)
                                                                                (:last-transaction-number base-database-value)
                                                                                (sorted-map-transaction-log/create)))))


(defn squash [branch-value]
  (common/squash-transaction-log (-> branch-value :transaction-log :branch-transaction-log)
                                 (:last-transaction-number branch-value)))
