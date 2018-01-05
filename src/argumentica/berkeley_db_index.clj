(ns argumentica.berkeley-db-index
  (:require (argumentica [index :as index]
                         [berkeley-db :as berkeley-db]
                         [comparator :as comparator])))

(defrecord BerkeleyDbIndex [state])

(defn create [directory-path database-name]
  (->BerkeleyDbIndex (-> (berkeley-db/create directory-path)
                           (berkeley-db/open-database database-name))))

(defmethod index/add-to-index
  BerkeleyDbIndex
  [this & values]
  (berkeley-db/put-to-database (database this)
                               (string-entry-bytes key)
                               value-bytes))

(defmethod index/inclusive-subsequence
  BerkeleyDbIndex
  [coll key]
  (subseq coll
          >=
          key))
