(ns argumentica.directory-db
  (:require (argumentica [index :as index]
                         [db :as db]
                         [directory-storage :as directory-storage])
            [iota :as iota])
  (:import [java.util UUID]
           [java.nio.file Files Paths OpenOption]
           [java.nio.file.attribute FileAttribute])
  (:use clojure.test))

(defrecord Index [index-atom])

(defn create-directory-index

  ([directory-path]
   (create-directory-index directory-path
                           nil))

  ([directory-path root-id]
   (directory-storage/create-directories directory-path)
   (->Index (atom (index/create
                   (index/full-after-maximum-number-of-values 101)
                   (directory-storage/->DirectoryStorage directory-path)
                   root-id)))))

(defn create-hash-map-index []
  (->Index (atom (index/create
                  (index/full-after-maximum-number-of-values 3)
                  {}))))

(defmethod db/inclusive-subsequence
  Index
  [this value]
  (index/inclusive-subsequence (:index-atom this)
                               value))

(defmethod db/add-to-index
  Index
  [this value]
  (index/add-to-atom (:index-atom this)
                     value))

(defmethod db/unload-index
  Index
  [this]
  (swap! (:index-atom this)
         index/unload-index))


(defn create [path]
  (db/create create-directory-index))


(deftest test-transact-statements-over
  (let [db (-> (db/create create-hash-map-index)
               (db/transact-statements-over [:master] [[1] :friend :add "friend 1"])
               (db/transact-statements-over [:master] [[1] :friend :add "friend 2"]))]
    
    (is (= #{"friend 2" "friend 1"}
           (db/get-value db
                         (db/get-reference db
                                           :master)
                         [1]
                         :friend)))
    
    (let [db (-> db
                 (db/transact-statements-over [:master] [[1] :friend :retract "friend 1"]))]
      
      (is (= #{"friend 2"}
             (db/get-value db
                           (db/get-reference db
                                             :master)
                           [1]
                           :friend)))
      (let [db (-> db
                   (db/transact-statements-over [:master] [[1] :friend :set "only friend"]))]
        
        (is (= #{"only friend"}
               (db/get-value db
                             (db/get-reference db
                                               :master)
                             [1]
                             :friend)))))))
