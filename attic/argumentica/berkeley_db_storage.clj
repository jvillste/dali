(ns argumentica.berkeley-db-storage
  (:require (argumentica [storage :as storage]
                         [berkeley-db :as berkeley-db])
            [me.raynes.fs :as fs])
  (:import [com.sleepycat.bind.tuple StringBinding]
           [com.sleepycat.je DatabaseEntry]))
(comment
  (defrecord BerkeleyDbStorage [state])

  (defn create [directory-path database-name]
    (->BerkeleyDbStorage (-> (berkeley-db/create directory-path)
                             (berkeley-db/open-database database-name))))

  (defn close! [this]
    (berkeley-db/close (:state this)))



  (defn database [this]
    (first (vals (:databases (:state this)))))

  (defmethod storage/get-from-storage!
    BerkeleyDbStorage
    [this key]
    (berkeley-db/get-from-database (database this)
                                   (string-entry-bytes key)))

  (defmethod storage/put-to-storage!
    BerkeleyDbStorage
    [this key value-bytes]
    (berkeley-db/put-to-database (database this)
                                 (string-entry-bytes key)
                                 value-bytes))

  (defmethod storage/storage-keys!
    BerkeleyDbStorage
    [this]
    (let [cursor (berkeley-db/open-cursor-for-database (database this))
          _ (berkeley-db/move-cursor-to-the-first-entry cursor)]

      (berkeley-db/transduce-cursor cursor
                                    (map (fn [[key-bytes value-bytes]]

                                           (entry-bytes-to-string key-bytes)))
                                    conj
                                    [])))

  (defmethod storage/remove-from-storage!
    BerkeleyDbStorage
    [this key]
    (berkeley-db/delete-from-database (database this)
                                      (string-entry-bytes key)))

  (defn start []
  
    (let [db-dir "data/temp/berkeley-db-storage-test"]
      (fs/delete-dir db-dir)
      (fs/mkdir db-dir)

      (let [state (create db-dir
                          "db")]
        (try

          (storage/put-to-storage! state
                                   "foo2"
                                   (storage/edn-to-byte-array "bar2"))

          (storage/put-to-storage! state
                                   "foo"
                                   (storage/edn-to-byte-array "baz"))

          (storage/remove-from-storage! state
                                        "foo")

          (storage/byte-array-to-edn (storage/get-from-storage! state
                                                                "foo2"))
        
          #_(storage/storage-keys! state)

          (finally (close! state))))))
  )
