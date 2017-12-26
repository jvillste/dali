(ns argumentica.berkeley-db-transaction-log
  (:require (argumentica [transaction-log :as transaction-log]
                         [berkeley-db :as berkeley-db]
                         [storage :as storage])
            [me.raynes.fs :as fs])
  (:import [com.sleepycat.bind.tuple BigIntegerBinding]
           [com.sleepycat.je DatabaseEntry]))

(defrecord BerkeleyDbTransactionLog [environment-atom])

(def database-name "log")

(defn create [directory-path]
  (fs/mkdir directory-path)
  (->BerkeleyDbTransactionLog (atom (-> (berkeley-db/open-environment directory-path)
                                        (berkeley-db/open-database database-name)))))

(defn close! [berkeley-db-transaction-log]
  (berkeley-db/close @(:environment-atom berkeley-db-transaction-log)))

(defmethod transaction-log/add! BerkeleyDbTransactionLog
    [this transaction-number statements]
    (let [key-database-entry (DatabaseEntry.)]
      (BigIntegerBinding/bigIntegerToEntry transaction-number
                                           key-database-entry)
      (swap! (:environment-atom this)
             berkeley-db/put
             database-name
             key-database-entry
             (storage/edn-to-byte-array statements))
      nil))

(comment 
  

  (defmethod transaction-log/subseq SortedMapTransactionLog
    [this first-transaction-number]
    (berkeley-db/)
    (subseq @(:sorted-map-atom this)
            >=
            first-transaction-number))

  (defmethod transaction-log/last-transaction-number SortedMapTransactionLog
    [this]
    (first (last @(:sorted-map-atom this)))))
