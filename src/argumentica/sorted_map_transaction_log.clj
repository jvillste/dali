(ns argumentica.sorted-map-transaction-log
  (:require (argumentica [transaction-log :as transaction-log])))

(defrecord SortedMapTransactionLog [sorted-map-atom])

(defn create []
  (->SortedMapTransactionLog (atom (sorted-map))))

(defmethod transaction-log/add! SortedMapTransactionLog
  [this transaction-number statements]
  (swap! (:sorted-map-atom this)
         assoc
         transaction-number
         statements))

(defmethod transaction-log/subseq SortedMapTransactionLog
  [this first-transaction-number]
  (subseq @(:sorted-map-atom this)
          >=
          first-transaction-number))

(defmethod transaction-log/last-transaction-number SortedMapTransactionLog
  [this]
  (first (last @(:sorted-map-atom this))))
