(ns argumentica.index)

(defmulti add! (fn [index value]
                 (type index)))


(defmulti flush! (fn [index metadata]
                   (type index)))

(defmulti inclusive-subsequence
  (fn [index key]
    (type index)))

(defmulti inclusive-reverse-subsequence
  (fn [index key]
    (type index)))

(defmulti last-stored-transaction-number
  (fn [index]
    (type index)))

(defmethod last-stored-transaction-number
  Object
  [this]
  nil)
