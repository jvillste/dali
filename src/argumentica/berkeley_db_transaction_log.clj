(ns argumentica.berkeley-db-transaction-log
  (:require (argumentica [transaction-log :as transaction-log]
                         [berkeley-db :as berkeley-db]
                         [storage :as storage])
            [me.raynes.fs :as fs])
  (:import [com.sleepycat.je DatabaseEntry]))

(defrecord BerkeleyDbTransactionLog [state-atom])

(def database-name "log")

(defn create [directory-path]
  (fs/mkdir directory-path)
  (->BerkeleyDbTransactionLog (atom (-> (berkeley-db/create directory-path)
                                        (berkeley-db/open-database database-name)))))

(defn close! [berkeley-db-transaction-log]
  (berkeley-db/close @(:state-atom berkeley-db-transaction-log)))

(defmethod transaction-log/add! BerkeleyDbTransactionLog
  [this transaction-number statements]
  (berkeley-db/put! @(:state-atom this)
                    database-name
                    (berkeley-db/big-integer-entry-bytes (BigInteger/valueOf transaction-number))
                    (storage/edn-to-bytes statements)))

(defmethod transaction-log/subseq BerkeleyDbTransactionLog
  [this first-transaction-number]

  ;; TODO: return a lazy sequence that opens a new cursor each time more values need to be generated

  (berkeley-db/transduce-keyvalues @(:state-atom this)
                                   database-name
                                   :key-bytes (berkeley-db/big-integer-entry-bytes (BigInteger/valueOf first-transaction-number))
                                   :get-type :gte
                                   :transducer (map (fn [[key-bytes value-bytes]]
                                                      [(berkeley-db/entry-bytes-to-big-integer key-bytes)
                                                       (storage/bytes-to-edn value-bytes)]))
                                   :reducer conj))

(defmethod transaction-log/last-transaction-number BerkeleyDbTransactionLog
  [this]
  (first (berkeley-db/transduce-keyvalues @(:state-atom this)
                                    database-name
                                    :get-type :last
                                    :transducer (map (fn [[key-bytes value-bytes]]
                                                       (berkeley-db/entry-bytes-to-big-integer key-bytes)))
                                    :reducer conj)))

(comment

  (fs/delete-dir "data/log")
  (fs/mkdir "data/log")

)

(defn start []


  (let [state (-> (create "data/log"))]
    (try
      #_(transaction-log/subseq state
                              1)
      (transaction-log/last-transaction-number state)

      #_(transaction-log/add! state
                              1
                              [[1 :friend 1 :add 2]])

      #_(transaction-log/add! state
                              2
                              [[2 :friend 1 :add 2]])

      (finally (close! state)))))
