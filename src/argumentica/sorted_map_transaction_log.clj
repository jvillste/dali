(ns argumentica.sorted-map-transaction-log
  (:require (argumentica [transaction-log :as transaction-log])
            [argumentica.util :as util]))

(defrecord SortedMapTransactionLog [sorted-map-atom])

(defn create []
  (->SortedMapTransactionLog (atom (sorted-map))))

(defmethod transaction-log/last-transaction-number SortedMapTransactionLog
  [this]
  (first (last @(:sorted-map-atom this))))

(defmethod transaction-log/add!-method SortedMapTransactionLog
  [this transaction-number statements]
  (swap! (:sorted-map-atom this)
         assoc
         transaction-number
         statements)
  this)

(defmethod transaction-log/subseq SortedMapTransactionLog
  [this first-transaction-number]
  (subseq @(:sorted-map-atom this)
          >=
          first-transaction-number))

(defmethod transaction-log/truncate! SortedMapTransactionLog
  [this first-preserved-transaction-number]
  (swap! (:sorted-map-atom this)
         util/filter-sorted-map-keys
         (fn [transaction-number]
           (<= first-preserved-transaction-number
               transaction-number)))
  this)




(defmethod transaction-log/close! SortedMapTransactionLog
  [this]
  this)

(defmethod transaction-log/make-transient! SortedMapTransactionLog
  [this]
  this)

(defmethod transaction-log/make-persistent! SortedMapTransactionLog
  [this]
  this)
