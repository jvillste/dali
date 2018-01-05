(ns argumentica.index)

(defmulti unload-index (fn [index]
                         (type index)))

(defmulti add-to-index (fn [index & values]
                         (type index)))

(defmethod add-to-index
  (type (sorted-set))
  [this & values]
  (apply conj
         this
         values))

(defmulti inclusive-subsequence
  (fn [coll key]
    (type coll)))

(defmethod inclusive-subsequence
  (type (sorted-set))
  [coll key]
  (subseq coll
          >=
          key))
