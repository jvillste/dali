(ns argumentica.transaction-log)

(defmulti add! (fn [log statements]
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
