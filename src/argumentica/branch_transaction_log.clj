(ns argumentica.branch-transaction-log
  (:require [argumentica.util :as util]
            [argumentica.transaction-log :as transaction-log]
            [argumentica.db.common :as common]))

(defrecord BranchTransactionLog [base-transaction-log
                                 last-base-transaction-number
                                 branch-transaction-log])

(util/defn-alias create ->BranchTransactionLog)

(defmethod transaction-log/last-transaction-number BranchTransactionLog
  [this]
  (or (transaction-log/last-transaction-number (:branch-transaction-log this))
      (:last-base-transaction-number this)))

(defmethod transaction-log/add!-method BranchTransactionLog
  [this transaction-number statements]
  (transaction-log/add!-method (:branch-transaction-log this)
                               transaction-number
                               statements))

(defmethod transaction-log/subseq BranchTransactionLog
  [this first-transaction-number]
  (concat (take-while (fn [[transaction-number statements_]]
                        (<= transaction-number (:last-base-transaction-number this)))
                      (transaction-log/subseq (:base-transaction-log this)
                                              first-transaction-number))
          (transaction-log/subseq (:branch-transaction-log this)
                                  first-transaction-number)))
