(ns argumentica.transaction-log
  (:require [schema.core :as s]
            [dali.core :as dali]))

(defmulti add! (fn [log statements]
                 (type log)))

(defmulti truncate! (fn [log first-transaction-number-to-preserve]
                      (type log)))

(defmulti close! (fn [log]
                   (type log)))

(defmulti subseq (fn [log first-transaction-number]
                   (type log)))

(defmulti subreducible (fn [log first-transaction-number]
                         (type log)))

(defmulti last-transaction-number (fn [log]
                                    (type log)))

(defmulti make-transient! (fn [log]
                            (type log)))

(defmulti make-persistent! (fn [log]
                             (type log)))

#_(defn add! [transaction-log transaction]
  (dali/validate-transaction transaction)

  (let [transaction-number (if-let [last-transaction-number (last-transaction-number transaction-log)]
                             (inc last-transaction-number)
                             0)]
    (add!-method transaction-log
                 transaction-number
                 transaction)
    transaction-number))
