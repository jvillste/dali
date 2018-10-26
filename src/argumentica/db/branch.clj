(ns argumentica.db.branch
  (:require  [clojure.test :as t]
             [argumentica.db.common :as common]
             [argumentica.sorted-set-index :as sorted-set-index]
             [argumentica.sorted-map-transaction-log :as sorted-map-transaction-log]
             [argumentica.db.sorted-datom-set-branch :as sorted-datom-set-branch]))


(defn create [base-database-value]
  (-> (common/db-from-index-definitions (map common/index-to-index-definition (vals (:indexes base-database-value)))
                                        (fn [key]
                                          (sorted-datom-set-branch/create (-> base-database-value :indexes key :index)
                                                                          (:last-transaction-number base-database-value)
                                                                          (-> base-database-value :indexes key :datom-transaction-number)
                                                                          (sorted-set-index/create)))
                                        (sorted-map-transaction-log/create))
      (assoc :base-database base-database-value)))
