(ns argumentica.sorted-map-transaction-log
  (:require [argumentica.transaction-log :as transaction-log]
            [argumentica.util :as util]
            [argumentica.reduction :as reduction]))

(defrecord SortedMapTransactionLog [state-atom])

(defn create []
  (->SortedMapTransactionLog (atom {:sorted-map (sorted-map)})))

(defmethod transaction-log/last-transaction-number SortedMapTransactionLog
  [this]
  (:last-transaction-number @(:state-atom this)))

(defmethod transaction-log/add! SortedMapTransactionLog
  [this statements]
  (let [transaction-number (inc (or (transaction-log/last-transaction-number this)
                                    -1))]
    (swap! (:state-atom this)
           (fn [state]
             (-> state
                 (update :sorted-map
                         assoc
                         transaction-number
                         statements)
                 (assoc :last-transaction-number transaction-number)))))
  this)

(defmethod transaction-log/subreducible SortedMapTransactionLog
  [this first-transaction-number]
  (reduction/reducible (fn [reducing-function initial-value]
                         (reduce reducing-function
                                 initial-value
                                 (subseq (:sorted-map @(:state-atom this))
                                         >=
                                         first-transaction-number)))))

(defmethod transaction-log/truncate! SortedMapTransactionLog
  [this first-preserved-transaction-number]
  (swap! (:state-atom this)
         update
         :sorted-map
         util/filter-sorted-map-keys
         (fn [transaction-number]
           (<= first-preserved-transaction-number
               transaction-number)))
  this)

(defmethod transaction-log/close! SortedMapTransactionLog
  [this]
  this)
