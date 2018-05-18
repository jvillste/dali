(ns argumentica.db.db)

(defprotocol DB
  (transact [this statements])
  (inclusive-subsequence [this index-key first-datom]))
