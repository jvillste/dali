(ns argumentica.db.server-transaction-log
  (:require (argumentica.db [client :as client])
            (argumentica [transaction-log :as transaction-log]
                         [storage :as storage])))

(defrecord ServerTransactionLog [client])

(defmethod transaction-log/subseq ServerTransactionLog
  [this first-transaction-number]
  (client/transaction-log-subseq (:client this)
                                 first-transaction-number))

(defmethod transaction-log/last-transaction-number ServerTransactionLog
  [this]
  (client/last-transaction-number (:client this)))



