(ns argumentica.db.multifile-transaction-log
  (:require [argumentica.db.multifile-numbered-sequence :as multifile-numbered-sequence]
            [argumentica.reducible :as reducible]
            [argumentica.transaction-log :as transaction-log]))

(defrecord MultifileTransactionLog [multifile-numbered-sequence-atom]
  java.io.Closeable
  (close [this]
    (multifile-numbered-sequence/close! @multifile-numbered-sequence-atom)))

(defn create [directory-path]
  (->MultifileTransactionLog (atom (multifile-numbered-sequence/open directory-path 500))))

(defmethod transaction-log/last-transaction-number MultifileTransactionLog
  [this]
  (multifile-numbered-sequence/last-number @(:multifile-numbered-sequence-atom this)))

(defmethod transaction-log/add! MultifileTransactionLog
  [this statements]
  (locking (:multifile-numbered-sequence-atom this)
    (swap! (:multifile-numbered-sequence-atom this)
           multifile-numbered-sequence/add-value
           statements))
  this)

(defmethod transaction-log/close! MultifileTransactionLog
  [this]
  (multifile-numbered-sequence/close! @(:multifile-numbered-sequence-atom this)))

(defmethod transaction-log/subreducible MultifileTransactionLog
  [this first-transaction-number]
  (reducible/reducible (fn [reducing-function initial-value]
                         (multifile-numbered-sequence/reduce-values @(:multifile-numbered-sequence-atom this)
                                                                    reducing-function
                                                                    initial-value
                                                                    first-transaction-number))))
