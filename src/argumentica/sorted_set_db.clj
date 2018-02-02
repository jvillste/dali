(ns argumentica.sorted-set-db
  (:require [argumentica.db.common :as common]
            (argumentica [sorted-map-transaction-log :as sorted-map-transaction-log]
                         [index :as index]
                         [sorted-set-index :as sorted-set-index]))

  (:use clojure.test))

(defn create []
  (common/create :indexes {:eatcv {:index (sorted-set-index/create)
                                   :eatcv-to-datoms common/eatcv-to-eatcv-datoms}}
                 :transaction-log (sorted-map-transaction-log/create)))

(defn value [db entity-id attribute transaction-id]
  (common/value (-> db :indexes :eatcv :index)
                entity-id attribute transaction-id))

(def set common/set)

(def transact common/transact)

(deftest test-memory-db
  (is (= [[1 :friend 0 :set 2]
          [1 :friend 1 :set 3]
          [2 :friend 0 :set 1]]
         (let [db (-> (create)
                      (transact [[1 :friend :set 2]
                                 [2 :friend :set 1]])
                      (transact [[1 :friend :set 3]]))]
           (index/inclusive-subsequence (-> db :indexes :eatcv :index)
                                        [1 :friend nil nil nil]))))


  (is (= 3
         (let [db (-> (create)
                      (transact [[1 :friend :set 2]
                                        [2 :friend :set 1]])
                      (transact [[1 :friend :set 3]]))]
           (value db 1 :friend 2)))))

