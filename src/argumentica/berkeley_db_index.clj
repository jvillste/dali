(ns argumentica.berkeley-db-index
  (:require (argumentica [index :as index]
                         [berkeley-db :as berkeley-db]
                         [comparator :as comparator])))

(defrecord BerkeleyDbIndex [state])

(defn create [directory-path database-name]
  (->BerkeleyDbIndex (-> (berkeley-db/create directory-path)
                         (berkeley-db/open-database database-name))))

(defn database [this]
  (first (vals (:databases (:state this)))))

(defmethod index/add-to-index
  BerkeleyDbIndex
  [this value]
  (berkeley-db/put-to-database! (database this)
                                (berkeley-db/string-entry-bytes key)
                                value-bytes))

(defmethod index/inclusive-subsequence
  BerkeleyDbIndex
  [coll key]
  (subseq coll
          >=
          key))
