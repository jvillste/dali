(ns argumentica.db.common-test
  (:require [argumentica.db.common :as common]
            (argumentica [hash-map-storage :as hash-map-storage]
                         [sorted-set-db :as sorted-set-db]
                         [btree :as btree]
                         [index :as index]
                         [comparator :as comparator]
                         [sorted-map-transaction-log :as sorted-map-transaction-log])
            [argumentica.btree-index :as btree-index]
            [argumentica.sorted-set-index :as sorted-set-index])
  (:use clojure.test))

(defn create-eav-db [& transactions]
  (reduce common/transact
          (common/db-from-index-definitions [common/eav-index-definition]
                                            (fn [index-key] (sorted-set-index/create))
                                            (sorted-map-transaction-log/create))
          transactions))

(deftest test-transact
  (is (= '([1 :friend 2 0 :add]
           [1 :friend 2 1 :remove]
           [1 :friend 3 1 :add]
           [2 :friend 1 0 :add])
         (let [db (create-eav-db #{[1 :friend :set 2]
                                   [2 :friend :set 1]}
                                 #{[1 :friend :set 3]})]
           (index/inclusive-subsequence (-> db :indexes :eav :index)
                                        [1 :friend nil nil nil])))))

(deftest test-datoms-from-index
  (let [db (-> (create-eav-db #{[:entity-1 :attribute-1 :set :value-1]
                                [:entity-1 :attribute-2 :set :value-2]}
                              #{[:entity-1 :attribute-1 :set :value-3]}))]

    (is (= '([:entity-1 :attribute-1 :value-1 0 :add]
             [:entity-1 :attribute-1 :value-1 1 :remove]
             [:entity-1 :attribute-1 :value-3 1 :add]
             [:entity-1 :attribute-2 :value-2 0 :add])
           (common/datoms-from-index (-> db :indexes :eav)
                                     [])))

    (is (= '([:entity-1 :attribute-1 :value-1 0 :add]
             [:entity-1 :attribute-2 :value-2 0 :add])
           (common/datoms-from-index (-> db :indexes :eav)
                                     []
                                     0)))

    (is (= '([:entity-1 :attribute-1 :value-1 0 :add]
             [:entity-1 :attribute-2 :value-2 0 :add])
           (common/datoms-from-index (-> db :indexes :eav)
                                     [:entity-1 :attribute-1]
                                     0)))

    (is (= '([:entity-1 :attribute-2 :value-2 0 :add])
           (common/datoms-from-index (-> db :indexes :eav)
                                     [:entity-1 :attribute-2]
                                     0)))))

(deftest test-datoms-starting-from-index
  (let [db (-> (create-eav-db #{[:entity-1 :attribute-1 :add 1]
                                [:entity-1 :attribute-1 :add 2]
                                [:entity-1 :attribute-1 :add 3]}))]

    (is (= '([:entity-1 :attribute-1 1 0 :add]
             [:entity-1 :attribute-1 2 0 :add]
             [:entity-1 :attribute-1 3 0 :add])
           (common/datoms-starting-from-index (-> db :indexes :eav)
                                              [])))

    (is (= '([:entity-1 :attribute-1 2 0 :add]
             [:entity-1 :attribute-1 3 0 :add])
           (common/datoms-starting-from-index (-> db :indexes :eav)
                                              [:entity-1 :attribute-1 2])))

    (is (= '([:entity-1 :attribute-1 3 0 :add]
             [:entity-1 :attribute-1 2 0 :add]
             [:entity-1 :attribute-1 1 0 :add])
           (common/datoms-starting-from-index (-> db :indexes :eav)
                                              []
                                              {:reverse? true})))

    (is (= '([:entity-1 :attribute-1 2 0 :add]
             [:entity-1 :attribute-1 1 0 :add])
           (common/datoms-starting-from-index (-> db :indexes :eav)
                                              [:entity-1 :attribute-1 2]
                                              {:reverse? true})))))


(deftest test-matching-datoms-from-index
  (let [db (-> (create-eav-db #{[:entity-1 :attribute-1 :set :value-1]
                                [:entity-1 :attribute-2 :set :value-2]}
                              #{[:entity-1 :attribute-1 :set :value-3]}))]

    (is (= '([:entity-1 :attribute-1 :value-1 0 :add]
             [:entity-1 :attribute-1 :value-1 1 :remove]
             [:entity-1 :attribute-1 :value-3 1 :add]
             [:entity-1 :attribute-2 :value-2 0 :add])
           (common/matching-datoms-from-index (-> db :indexes :eav)
                                              [])))

    (is (= '([:entity-1 :attribute-1 :value-1 0 :add]
             [:entity-1 :attribute-2 :value-2 0 :add])
           (common/matching-datoms-from-index (-> db :indexes :eav)
                                              []
                                              0)))

    (is (= '([:entity-1 :attribute-1 :value-1 0 :add])
           (common/matching-datoms-from-index (-> db :indexes :eav)
                                              [:entity-1 :attribute-1]
                                              0)))

    (is (= '([:entity-1 :attribute-1 :value-1 0 :add]
             [:entity-1 :attribute-1 :value-1 1 :remove]
             [:entity-1 :attribute-1 :value-3 1 :add])
           (common/matching-datoms-from-index (-> db :indexes :eav)
                                              [:entity-1 :attribute-1]
                                              1)))

    (is (= '([:entity-1 :attribute-2 :value-2 0 :add])
           (common/matching-datoms-from-index (-> db :indexes :eav)
                                              [:entity-1 :attribute-2]
                                              0)))

    (is (= '([:entity-1 :attribute-2 :value-2 0 :add])
           (common/matching-datoms-from-index (-> db :indexes :eav)
                                              [:entity-1 :attribute-2]
                                              1)))))

(deftest test-values-from-eav
  (let [db (-> (create-eav-db #{[1 :friend :set 2]
                                [2 :friend :set 1]}
                              #{[1 :friend :set 3]}))]

    (is (= '(2)
           (common/values-from-eav (-> db :indexes :eav)
                                   1
                                   :friend
                                   0)))

    (is (= '(3)
           (common/values-from-eav (-> db :indexes :eav)
                                   1
                                   :friend
                                   1))))

  (let [db (-> (create-eav-db #{[:entity-1 :attribute-1 :set :value-1]
                                [:entity-1 :attribute-2 :set :value-2]}))]

    (is (= '(:value-1)
           (common/values-from-eav (-> db :indexes :eav)
                                   :entity-1
                                   :attribute-1)))

    (is (= '(:value-2)
           (common/values-from-eav (-> db :indexes :eav)
                                   :entity-1
                                   :attribute-2)))))

(defn create-db-with-composite-index []
  (common/db-from-index-definitions [common/eav-index-definition
                                     (common/composite-index-definition :composite :attribute-1 :attribute-2)]
                                    (fn [index-key]
                                      #_(btree-index/create-memory-btree-index 101)
                                      (sorted-set-index/create))
                                    (sorted-map-transaction-log/create)))

(defn datoms-from-composite-index [& transactions]
  (-> (reduce common/transact
              (create-db-with-composite-index)
              transactions)
      (common/datoms-from :composite [])))

(deftest test-composite-index
  (is (= []
         (datoms-from-composite-index #{[:entity-1 :attribute-1 :add :value-1]})))

  (is (= []
         (datoms-from-composite-index #{[:entity-1 :attribute-2 :add :value-1]})))

  (is (= '([:value-1 :value-2 :entity-1 0 :add])
         (datoms-from-composite-index #{[:entity-1 :attribute-1 :add :value-1]
                                        [:entity-1 :attribute-2 :add :value-2]})))

  (is (= '([:value-1 :value-2 :entity-1 1 :add])
         (datoms-from-composite-index #{[:entity-1 :attribute-1 :add :value-1]}
                                      #{[:entity-1 :attribute-2 :add :value-2]})))

  (is (= '([:value-1 :value-2 :entity-1 0 :add]
           [:value-1 :value-2 :entity-1 1 :remove])
         (datoms-from-composite-index #{[:entity-1 :attribute-1 :add :value-1]
                                        [:entity-1 :attribute-2 :add :value-2]}
                                      #{[:entity-1 :attribute-2 :remove :value-2]}))))

(defn propositions-from-composite-index [last-transaction-number pattern & transactions]
  (let [db (reduce common/transact
                   (create-db-with-composite-index)
                   transactions)]
    (common/propositions-from-index (common/index db :composite)
                                    pattern
                                    last-transaction-number)))

(deftest test-propositions-from-index
  (is (= '()
         (propositions-from-composite-index nil [] #{})))

  (is (= '([:value-1 :value-2 :entity-1])
         (propositions-from-composite-index nil []
                                            #{[:entity-1 :attribute-1 :add :value-1]
                                              [:entity-1 :attribute-2 :add :value-2]})))

  (is (= '([:value-3 :value-2 :entity-1])
         (propositions-from-composite-index nil []
                                            #{[:entity-1 :attribute-1 :add :value-1]
                                              [:entity-1 :attribute-2 :add :value-2]}
                                            #{[:entity-1 :attribute-1 :set :value-3]})))

  (is (= '([:value-1 :value-2 :entity-1])
         (propositions-from-composite-index 0 []
                                            #{[:entity-1 :attribute-1 :add :value-1]
                                              [:entity-1 :attribute-2 :add :value-2]}
                                            #{[:entity-1 :attribute-1 :set :value-3]})))

  (testing "pattern"
    (is (= '([:value-1 :value-2 :entity-1]
             [:value-2 :value-2 :entity-1])
           (propositions-from-composite-index nil []
                                              #{[:entity-1 :attribute-1 :add :value-1]
                                                [:entity-1 :attribute-1 :add :value-2]
                                                [:entity-1 :attribute-2 :add :value-2]})))

    (is (= '([:value-2 :value-2 :entity-1])
           (propositions-from-composite-index nil [:value-2]
                                              #{[:entity-1 :attribute-1 :add :value-1]
                                                [:entity-1 :attribute-1 :add :value-2]
                                                [:entity-1 :attribute-2 :add :value-2]})))))

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
