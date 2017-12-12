(ns argumentica.sorted-map-transaction-log
  (:require (argumentica [transaction-log :as transaction-log])))

(defn create []
  (sorted-map))

(defmethod transaction-log/add (type (sorted-map))
  [log transaction-number statements]
  (assoc log transaction-number statements))

(defmethod transaction-log/get (type (sorted-map))
  [log first-transaction-number]
  (subseq log
          >=
          first-transaction-number))
