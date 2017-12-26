(ns argumentica.transaction-log)

(defmulti add! (fn [log transaction-number statements]
                 (type log)))

(defmulti subseq (fn [log first-transaction-number]
                   (type log)))

(defmulti last-transaction-number (fn [log]
                                    (type log)))
