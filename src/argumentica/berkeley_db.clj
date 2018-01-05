(ns argumentica.berkeley-db
  (:require [me.raynes.fs :as fs])
  (:import [com.sleepycat.je Cursor
            Database
            DatabaseConfig
            DatabaseEntry 
            DatabaseException
            Environment      
            EnvironmentConfig
            LockMode         
            OperationStatus
            Transaction
            Get]
           [java.io File]))

(defn create [directory-path]
  {:environment (Environment. (File. directory-path)
                              (doto (EnvironmentConfig.)
                                (.setTransactional true)
                                (.setAllowCreate true)))})


(defn open-database
  ([state database-name]
   (open-database state database-name nil))
  
  ([state database-name key-comparator]
   (assoc-in state
             [:databases database-name]
             (.openDatabase (:environment state)
                            nil
                            database-name
                            (let [database-config (doto (DatabaseConfig.)
                                                    (.setTransactional true)
                                                    (.setAllowCreate true))]
                              (if key-comparator
                                (.setBtreeComparator database-config
                                                     key-comparator)
                                database-config))))))

(defn close [state]
  (doseq [databse (vals (:databases state))]
    (.close databse))
  (.close (:environment state)))

(defn put-to-database [database key-bytes value-bytes]
  (.put database
        nil
        (DatabaseEntry. key-bytes)
        (DatabaseEntry. value-bytes)))

(defn put [state database-name key-bytes value-bytes]
  (put-to-database (get-in state [:databases database-name])
                   key-bytes
                   value-bytes))


(defn get-from-database [database key-bytes]
  (let [value-database-entry (DatabaseEntry.)]
    (.get database
          nil
          (DatabaseEntry. key-bytes)
          value-database-entry
          nil)
    
    (.getData value-database-entry)))

(defn get [state database-name key-bytes]
  (get-from-database (get-in state [:databases database-name])
                     key-bytes))

(defn transduce-cursor
  ([cursor transducer]
   (transduce-cursor cursor transducer (constantly nil) nil))
  
  ([cursor transducer reducer]
   (transduce-cursor cursor transducer reducer (reducer)))
  
  ([cursor transducer reducer initial-value]
   (let [key-database-entry (DatabaseEntry.)
         value-database-entry (DatabaseEntry.)
         reducing-function (transducer reducer)]


     (loop [operation-result (.get cursor
                                   key-database-entry
                                   value-database-entry
                                   Get/CURRENT
                                   nil)
            value initial-value]

       (if operation-result
         (let [result (reducing-function value
                                         [(.getData key-database-entry)
                                          (.getData value-database-entry)])]
           (if (reduced? result)
             (do (.close cursor)
                 (reducing-function @result))
             (recur (.get cursor
                          key-database-entry
                          value-database-entry
                          Get/NEXT
                          nil)
                    result)))
         (do (.close cursor)
             (reducing-function value)))))))

(defn open-cursor-for-database [database]
  (.openCursor database
               nil
               nil))

(defn move-cursor [cursor get-type key-bytes]
  (let [key-database-entry (if key-bytes
                             (DatabaseEntry. key-bytes)
                             (DatabaseEntry.))
        value-database-entry (DatabaseEntry.)]
    (.get cursor
          key-database-entry
          value-database-entry
          get-type
          nil)))

(defn move-cursor-gte [cursor key-bytes]
  (move-cursor cursor
               Get/SEARCH_GTE
               key-bytes))

(defn move-cursor-to-the-first-entry [cursor]
  (move-cursor cursor
               Get/FIRST
               nil))

(defn transduce-keyvalues
  ([state database-name key-bytes transducer]
   (transduce-keyvalues state database-name key-bytes transducer (constantly nil) nil))
  
  ([state database-name key-bytes transducer reducer]
   (transduce-keyvalues state database-name key-bytes transducer reducer (reducer)))
  
  ([state database-name key-bytes transducer reducer initial-value]

   (let [cursor (.openCursor (get-in state [:databases database-name])
                             nil
                             nil)
         key-database-entry (DatabaseEntry. key-bytes)
         value-database-entry (DatabaseEntry.)
         reducing-function (transducer reducer)]


     (transduce-cursor cursor
                       )


     (loop [operation-result (.get cursor
                                   key-database-entry
                                   value-database-entry
                                   Get/SEARCH_GTE
                                   nil)
            value initial-value]

       (if operation-result
         (let [result (reducing-function value
                                         [(.getData key-database-entry)
                                          (.getData value-database-entry)])]
           (if (reduced? result)
             (do (.close cursor)
                 (reducing-function @result))
             (recur (.get cursor
                          key-database-entry
                          value-database-entry
                          Get/NEXT
                          nil)
                    result)))
         (do (.close cursor)
             (reducing-function value)))))))

(defn delete-from-database [database key-bytes]
  (.delete database
           nil
           (DatabaseEntry. key-bytes)))


(defn start []
  #_(transduce (comp (map inc))
               conj
               []
               [1 2 3])
  
  (let [state (-> (create "data/berkeley2")
                  (open-database "db"))
        cursor (open-cursor-for-database (get-in state [:databases "db"]))
        ;; _ (move-cursor cursor Get/FIRST nil)
        _ (move-cursor-gte cursor
                           (.getBytes "fo"
                                      "UTF-8"))
        values (transduce-cursor cursor
                                 (comp #_(take 1)
                                       #_(take-while (fn [[key value]]
                                                       (.startsWith (String. key "UTF-8") "fo")))
                                       (map (fn [[key value]]
                                              [(String. key "UTF-8")
                                               (String. value "UTF-8")]))
                                       (map println)))]
    (close state)
    values))

(comment
  
  (fs/mkdir "data/berkeley2")
  (fs/delete-dir "data/berkeley")

  (let [state (-> (create "data/berkeley2")
                        (open-database "db"))
        value-bytes (get state
                         "db"
                         (.getBytes "foo2"
                                    "UTF-8"))
        value (if value-bytes
                (String. value-bytes
                         "UTF-8")
                nil)]
    (close state)
    value)

  (let [state (-> (create "data/berkeley2")
                        (open-database "db"))]

    (put state
         "db"
         (.getBytes "foo2"
                    "UTF-8")
         
         (.getBytes "bar"
                    "UTF-8"))
    
    (close state))
  
  (let [db-directory (File. "data/berkeley")
        env (Environment. db-directory
                          (doto (EnvironmentConfig.)
                            (.setTransactional true)
                            (.setAllowCreate true)))
        db (.openDatabase env
                          nil
                          "db"
                          (doto (DatabaseConfig.)
                            (.setTransactional true)
                            (.setAllowCreate true)))]
    (.put db
          nil
          (DatabaseEntry. (.getBytes "baz"
                                     "UTF-8"))
          (DatabaseEntry. (.getBytes "bar"
                                     "UTF-8")))
    (.close db)
    (.close env)))

;; EnvironmentConfig envConfig = new EnvironmentConfig();
;;        envConfig.setTransactional(true);
;;        envConfig.setAllowCreate(true);
;;        Environment exampleEnv = new Environment(envDir, envConfig) ;
