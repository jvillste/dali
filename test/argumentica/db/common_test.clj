(ns argumentica.db.common-test
  (:require [argumentica.db.common :as common]
            (argumentica [hash-map-storage :as hash-map-storage]
                         [sorted-set-db :as sorted-set-db]
                         [btree :as btree]
                         [index :as index]
                         [sorted-map-transaction-log :as sorted-map-transaction-log])
            [argumentica.btree-index :as btree-index]
            [argumentica.sorted-set-index :as sorted-set-index])
  (:use clojure.test))


(deftest test-transact
  (is (= '([1 :friend 0 :set 2]
           [1 :friend 1 :set 3]
           [2 :friend 0 :set 1])
         (let [db (-> (sorted-set-db/create)
                      (common/transact [[1 :friend :set 2]
                                        [2 :friend :set 1]])
                      (common/transact [[1 :friend :set 3]]))]
           (index/inclusive-subsequence (-> db :indexes :eatcv :index)
                                        [1 :friend nil nil nil])))))

(deftest test-eat-datoms-from-eatcv
  (let [db (-> (sorted-set-db/create)
               (sorted-set-db/transact [[1 :friend :set 2]
                                        [2 :friend :set 1]])
               (sorted-set-db/transact [[1 :friend :set 3]]))]

    (is (= [[1 :friend 0 :set 2]]
           (common/eat-datoms-from-eatcv (-> db :indexes :eatcv :index)
                                         1
                                         :friend
                                         0)))

    (is (= [[1 :friend 0 :set 2]
            [1 :friend 1 :set 3]]
           (common/eat-datoms-from-eatcv (-> db :indexes :eatcv :index)
                                         1
                                         :friend
                                         1)))))

(defn create-db-with-composite-index []
  (common/db-from-index-definitions [common/eatcv-index-definition
                                     (common/composite-index-definition :composite :attribute-1 :attribute-2)]
                                    (fn [index-key]
                                      #_(btree-index/create-memory-btree-index 101)
                                      (sorted-set-index/create))
                                    (sorted-map-transaction-log/create)))

(defn test-composite-index [& transactions]
  (-> (reduce common/transact
              (create-db-with-composite-index)
              transactions)
      (common/datoms-from :composite [])))

(deftest create-in-memory-db
  (is (= nil
         (test-composite-index #{[:entity-1 :attribute-1 :add :value-1]})))

  (is (= nil
         (test-composite-index #{[:entity-1 :attribute-2 :add :value-1]})))

  (is (= '([:value-1 :value-2 0 :add])
         (test-composite-index #{[:entity-1 :attribute-1 :add :value-1]
                                 [:entity-1 :attribute-2 :add :value-2]})))

  (is (= '([:value-1 :value-2 1 :add])
         (test-composite-index #{[:entity-1 :attribute-1 :add :value-1]}
                               #{[:entity-1 :attribute-2 :add :value-2]})))

  (is (= '([:value-1 :value-2 0 :add]
           [:value-1 :value-2 1 :remove])
         (test-composite-index #{[:entity-1 :attribute-1 :add :value-1]
                                 [:entity-1 :attribute-2 :add :value-2]}
                               #{[:entity-1 :attribute-2 :remove :value-2]}))))
(comment
  (test-composite-index #{[:entity-1 :attribute-1 :add :value-1]})

  (do (println 1)
      4)

  ) ;; TODO: remove-me

(deftest test-composite-index
  )

#_(deftest read-only-index-test
  (let [metadata-storage (hash-map-storage/create)
        node-storage (hash-map-storage/create)
        transaction-log (sorted-map-transaction-log/create)
        transactor-db (-> (create-db :indexes {:eatcv {:index-atom (atom (btree/create-from-options :metadata-storage metadata-storage
                                                                                                    :node-storage node-storage))
                                                       :eatcv-to-datoms eatcv-to-eatcv-datoms}}
                                     :transaction-log transaction-log)
                          (transact [[1 :friend :set 2]
                                     [2 :friend :set 1]])
                          (flush-indexes-after-maximum-number-of-transactions 0)
                          (common/transact [[1 :friend :set 3]]))

        read-only-db (-> (create-db :indexes {:eatcv {:index-atom (atom (btree/create-from-options :metadata-storage metadata-storage
                                                                                                   :node-storage node-storage))
                                                      :eatcv-to-datoms eatcv-to-eatcv-datoms
                                                      :last-transaction-number (or (-> (btree/latest-root (btree/roots-from-metadata-storage metadata-storage))
                                                                                       :metadata
                                                                                       :last-transaction-number)
                                                                                   0)}}
                                    :transaction-log transaction-log)
                         (update-indexes))]

    (is (= '([1 :friend 0 :set 2]
             [1 :friend 1 :set 3]
             [2 :friend 0 :set 1])
           (btree/inclusive-subsequence (-> transactor-db :indexes :eatcv :index-atom)
                                        [1 :friend nil nil nil])
           (btree/inclusive-subsequence (-> read-only-db :indexes :eatcv :index-atom)
                                        [1 :friend nil nil nil])))))

#_(deftest test-db-reload
  (let [metadata-storage (hash-map-storage/create)
        node-storage (hash-map-storage/create)
        transaction-log (sorted-map-transaction-log/create)
        db1 (-> (create-db :indexes {:eatcv {:index-atom (atom (btree/create-from-options :metadata-storage metadata-storage
                                                                                          :node-storage node-storage))
                                             :eatcv-to-datoms eatcv-to-eatcv-datoms}}
                           :transaction-log transaction-log)
                (transact [[1 :friend :set 2]
                           [2 :friend :set 1]])
                (flush-indexes-after-maximum-number-of-transactions 0)
                (transact [[1 :friend :set 3]]))

        db2 (-> (create-db :indexes {:eatcv {:index-atom (atom (btree/create-from-options :metadata-storage metadata-storage
                                                                                          :node-storage node-storage))
                                             :eatcv-to-datoms eatcv-to-eatcv-datoms
                                             :last-transaction-number (last-transaction-number metadata-storage)}}
                           :transaction-log transaction-log)
                (transact [[1 :friend :set 4]]))]

    (is (= '([1 :friend 0 :set 2]
             [1 :friend 1 :set 3]
             [2 :friend 0 :set 1])
           (btree/inclusive-subsequence (-> db1 :indexes :eatcv :index-atom)
                                        [1 :friend nil nil nil])))

    (is (= '([1 :friend 0 :set 2]
             [1 :friend 1 :set 3]
             [1 :friend 2 :set 4]
             [2 :friend 0 :set 1])
           (btree/inclusive-subsequence (-> db2 :indexes :eatcv :index-atom)
                                        [1 :friend nil nil nil])))))
