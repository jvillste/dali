(ns argumentica.transaction-log)

(defmulti add (fn [log transaction-number statements]
                (type log)))

(defmulti get (fn [log first-transaction-number]
                (type log)))

