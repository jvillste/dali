(ns argumentica.transaction-log
  (:require [schema.core :as s]
            [dali.core :as dali]))

(defmulti add!-method (fn [log transaction-number statements]
                        (type log)))

(defmulti truncate! (fn [log first-transaction-number-to-preserve]
                      (type log)))

(defmulti close! (fn [log]
                  (type log)))

(defmulti subseq (fn [log first-transaction-number]
                   (type log)))

(defmulti last-transaction-number (fn [log]
                                    (type log)))

(defmulti make-transient! (fn [log]
                            (type log)))

(defmulti make-persistent! (fn [log]
                             (type log)))


(defn add! [transaction-log transaction]
  (dali/validate-transaction transaction)

  (add!-method transaction-log
               (if-let [last-transaction-number (last-transaction-number transaction-log)]
                 (inc last-transaction-number)
                 0)
               transaction))
