(ns argumentica.branch-transaction-log
  (:require [argumentica.util :as util]
            [argumentica.transaction-log :as transaction-log]
            [argumentica.db.common :as common]
            [argumentica.contents :as contents]))

(defn take-subseq [base-transaction-log last-base-transaction-number branch-transaction-log first-transaction-number]
  (concat (map second
               (take-while (fn [[transaction-number _changes]]
                             (<= transaction-number last-base-transaction-number))
                           (map-indexed vector
                                        (transaction-log/subseq base-transaction-log
                                                                first-transaction-number))))
          (transaction-log/subseq branch-transaction-log
                                  first-transaction-number)))

(deftype BranchTransactionLog [base-transaction-log
                               last-base-transaction-number
                               branch-transaction-log]

  contents/Protocol
  (contents [this]
    {:base-transaction-log base-transaction-log
     :last-base-transaction-number last-base-transaction-number
     :branch-transaction-log branch-transaction-log})

  clojure.lang.Seqable
  (seq [this]
    (take-subseq base-transaction-log
                 last-base-transaction-number
                 branch-transaction-log
                 0)))

#_(util/defn-alias create ->BranchTransactionLog)

(defn create [base-transaction-log
              last-base-transaction-number
              branch-transaction-log]
  (assert (and (int? last-base-transaction-number)
               (<= 0 last-base-transaction-number)))
  (->BranchTransactionLog base-transaction-log
                          last-base-transaction-number
                          branch-transaction-log))

(defmethod transaction-log/last-transaction-number BranchTransactionLog
  [this]
  (let [contents (contents/contents this)]
    (+ (:last-base-transaction-number contents)
       (if-let [last-branch-transaction-number (transaction-log/last-transaction-number (:branch-transaction-log contents))]
         (inc last-branch-transaction-number)
         0))))

(defmethod transaction-log/add! BranchTransactionLog
  [this statements]
  (transaction-log/add! (:branch-transaction-log (contents/contents this))
                        statements))

(defmethod transaction-log/subseq BranchTransactionLog
  [this first-transaction-number]
  (let [contents (contents/contents this)]
    (take-subseq (:base-transaction-log contents)
                 (:last-base-transaction-number contents)
                 (:branch-transaction-log contents)
                 first-transaction-number))
#_  (concat (map second
               (take-while (fn [[transaction-number _changes]]
                             (<= transaction-number (:last-base-transaction-number this)))
                           (map-indexed vector
                                        (transaction-log/subseq (:base-transaction-log this)
                                                                first-transaction-number))))
          (transaction-log/subseq (:branch-transaction-log this)
                                  first-transaction-number)))

(defmethod print-method BranchTransactionLog [this ^java.io.Writer writer]
  (.write writer (with-out-str (clojure.pprint/pprint (seq this)))))
