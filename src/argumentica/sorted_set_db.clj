(ns argumentica.sorted-set-db
  (:require [argumentica.db.common :as common]
            (argumentica [sorted-map-transaction-log :as sorted-map-transaction-log]
                         [index :as index]
                         [sorted-set-index :as sorted-set-index]
                         [transaction-log :as transaction-log]))

  (:use clojure.test))

(defn create []
  (common/create :indexes {:eatcv {:index (sorted-set-index/create)
                                   :eatcv-to-datoms common/eatcv-to-eatcv-datoms}}
                 :transaction-log (sorted-map-transaction-log/create)))

(def value common/value)

(def set common/set)

(def transact common/transact)




(deftest test-memory-db
  (is (= [[1 :friend -1 :set 3]
          [1 :friend 0 :set 2]
          [2 :friend 0 :set 1]]
         (let [db (-> (create)
                      (transact [[1 :friend :set 2]
                                 [2 :friend :set 1]])
                      (transact [[1 :friend :set 3]]))]
           (index/inclusive-subsequence (-> db :indexes :eatcv :index)
                                        [1 :friend nil nil nil]))))


  (let [db (-> (create)
               (transact [[1 :friend :set 2]
                          [2 :friend :set 1]])
               (transact [[1 :friend :set 3]]))]
    (is (= 3
           (value db 1 :friend 2)))

    (is (= 3
           (value db 1 :friend)))

    (is (= 2
           (value db 1 :friend 0)))))

