(ns argumentica.db.multifile-transaction-log
  (:require [argumentica.db.multifile-numbered-sequence :as multifile-numbered-sequence]
            [argumentica.transaction-log :as transaction-log]
            [argumentica.reduction :as reduction]))

(defrecord MultifileTransactionLog [multifile-numbered-sequence-atom]
  java.io.Closeable
  (close [this]
    (multifile-numbered-sequence/close! @multifile-numbered-sequence-atom)))

(defn open [directory-path]
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
  (reduction/reducible (fn [reducing-function initial-value]
                         (multifile-numbered-sequence/reduce-values @(:multifile-numbered-sequence-atom this)
                                                                    reducing-function
                                                                    initial-value
                                                                    first-transaction-number))))

(defmethod transaction-log/subseq MultifileTransactionLog
  [this first-transaction-number]
  (into [] (transaction-log/subreducible this first-transaction-number)))
