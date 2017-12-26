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

(defn open-environment [directory-path]
  {:environment (Environment. (File. directory-path)
                              (doto (EnvironmentConfig.)
                                (.setTransactional true)
                                (.setAllowCreate true)))})


(defn open-database [environment database-name]
  (assoc-in environment
            [:databases database-name]
            (.openDatabase (:environment environment)
                           nil
                           database-name
                           (doto (DatabaseConfig.)
                             (.setTransactional true)
                             (.setAllowCreate true)))))

(defn close [environment]
  (doseq [databse (vals (:databases environment))]
    (.close databse))
  (.close (:environment environment)))


(defn put [environment database-name key-bytes value-bytes]
  (.put (get-in environment [:databases database-name])
        nil
        (DatabaseEntry. key-bytes)
        (DatabaseEntry. value-bytes)))


(defn get [environment database-name key-bytes]
  (let [value-database-entry (DatabaseEntry.)]
    
    (.get (get-in environment [:databases database-name])
          nil
          (DatabaseEntry. key-bytes)
          value-database-entry
          nil)
    
    (.getData value-database-entry)))

(defn transduce-keyvalues
  ([environment database-name key-bytes transducer]
   (transduce-keyvalues environment database-name key-bytes transducer (constantly nil) nil))
  
  ([environment database-name key-bytes transducer reducer]
   (transduce-keyvalues environment database-name key-bytes transducer reducer (reducer)))
  
  ([environment database-name key-bytes transducer reducer initial-value]
   (let [cursor (.openCursor (get-in environment [:databases database-name])
                             nil
                             nil)
         key-database-entry (DatabaseEntry. key-bytes)
         value-database-entry (DatabaseEntry.)
         reducing-function (transducer reducer)]


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


(defn start []
  #_(transduce (comp (map inc))
               conj
               []
               [1 2 3])
  
  (let [environment (-> (open-environment "data/berkeley2")
                        (open-database "db"))
        values (transduce-keyvalues environment
                                    "db"
                                    (.getBytes "fo"
                                               "UTF-8")
                                    (comp #_(take 1)
                                          #_(take-while (fn [[key value]]
                                                        (.startsWith (String. key "UTF-8") "fo")))
                                          (map (fn [[key value]]
                                                 [(String. key "UTF-8")
                                                  (String. value "UTF-8")]))
                                          (map println)))]
    (close environment)
    values))

(comment
  (.startsWith "fo" "foo")
  (into )
  (((map inc) conj) [] 1)
  ((comp (take 1)
         (completing conj))
   []
   1)
  
  (fs/mkdir "data/berkeley2")
  (fs/delete-dir "data/berkeley")

  (let [environment (-> (open-environment "data/berkeley")
                        (open-database "db"))
        value (String. (get environment
                            "db"
                            (.getBytes "foo"
                                       "UTF-8"))
                       "UTF-8")]
    (close environment)
    value)

  (let [environment (-> (open-environment "data/berkeley2")
                        (open-database "db"))]

    (put environment
         "db"
         (.getBytes "foo2"
                    "UTF-8")
         
         (.getBytes "bar"
                    "UTF-8"))
    
    (close environment))
  
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
