(ns argumentica.db.db)

(defprotocol WriteableDB
  (transact [this statements]))

(defprotocol ReadableDB
  (inclusive-subsequence [this index-key first-datom]))
