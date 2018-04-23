(ns argumentica.db-test
  (:require (argumentica [db :as db]
                         [btree-index :as btree-index]))
  (:use clojure.test))


(deftest test-transact-statements-over
  (let [db (-> (db/create (btree-index/create-memory-btree-index))
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

