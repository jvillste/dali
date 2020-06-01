(ns argumentica.kiss-test
  (:require [argumentica.db.branch :as branch]
            [argumentica.db.common :as common]
            [argumentica.index :as index]
            [argumentica.sorted-map-transaction-log :as sorted-map-transaction-log]
            [argumentica.sorted-set-index :as sorted-set-index]
            [argumentica.transaction-log :as transaction-log]
            [clojure.test :refer :all]
            [argumentica.comparator :as comparator]
            [argumentica.btree-db :as btree-db]
            [argumentica.btree-index :as btree-index]
            [argumentica.db.server-api :as server-api]
            [argumentica.db.client :as client]
            [argumentica.db.server-btree-db :as server-btree-db]
            [argumentica.btree :as btree]
            [argumentica.db.db :as db]))

(defn transact! [transaction-log indexes-atom statements]
  (transaction-log/add! transaction-log
                        statements)

  (swap! indexes-atom
         common/update-indexes!
         transaction-log))

(deftest test-local-indexes
  (let [transaction-log (sorted-map-transaction-log/create)
        indexes-atom (atom (common/index-definitions-to-indexes sorted-set-index/creator
                                                                common/base-index-definitions))]

    (transaction-log/add! transaction-log
                          #{[1 :name :set "foo"]})

    (is (= '([0 #{[1 :name :set "foo"]}])
           (transaction-log/subseq transaction-log
                                   0)))

    (swap! indexes-atom
           common/update-indexes!
           transaction-log)

    (is (= '([:name "foo" 0 1 :set])
           (index/inclusive-subsequence (-> @indexes-atom :avtec :index)
                                        nil)))

    (is (= '([1 :name 0 :set "foo"])
           (common/matching-datoms-from-index (-> @indexes-atom :eatcv)
                                     [1 :name])))

    (is (= #{"foo"}
           (common/values-from-eatcv (-> @indexes-atom :eatcv)
                                     1
                                     :name)))

    (is (= 0 (transaction-log/last-transaction-number transaction-log)))

    (transact! transaction-log
               indexes-atom
               #{[1 :name :add "bar"]})

    (is (= 1 (transaction-log/last-transaction-number transaction-log)))

    (is (= 2 (common/first-unindexed-transacion-number-for-index-map @indexes-atom)))

    (is (= '([1 :name 0 :set "foo"]
             [1 :name 1 :add "bar"])
           (index/inclusive-subsequence (-> @indexes-atom :eatcv :index)
                                        nil)))

    (is (= #{"foo" "bar"}
           (common/values-from-eatcv (-> @indexes-atom :eatcv)
                                     1
                                     :name)))))

(deftest test-branch
  (do (let [db (common/db-from-index-definitions common/base-index-definitions
                                                 sorted-set-index/creator
                                                 (sorted-map-transaction-log/create))]

        (testing "Transacting two statements in to the database"
          (db/transact db #{[1 :name :set "1 name 1 in base"]
                            [1 :type :set :person]})

          (db/transact db #{[2 :name :set "2 name 1 in base"]
                            [2 :type :set :person]})

          (is (= '([1 :name 0 :set "1 name 1 in base"])
                 (common/datoms db :eatcv [1 :name])))

          (is (= '([2 :name 1 :set "2 name 1 in base"])
                 (common/datoms db :eatcv [2 :name]))))

        (is (= '([:type :person 0 1 :set]
                 [:type :person 1 2 :set])
               (index/inclusive-subsequence (-> db :indexes :avtec :index)
                                            [:type :person 0 ::comparator/min ::comparator/min])))

        (is (= '([:type :person 0 1 :set]
                 [:type :person 1 2 :set])
               (index/inclusive-subsequence (-> db :indexes :avtec :index)
                                            [:type :person 0])))

        (let [db-value @db
              branch (branch/create db-value)]

          (is (= 1 (transaction-log/last-transaction-number (:transaction-log branch))))
          (is (= '([:name "1 name 1 in base" 0 1 :set]
                   [:name "2 name 1 in base" 1 2 :set]
                   [:type :person 0 1 :set]
                   [:type :person 1 2 :set])
                 (index/inclusive-subsequence (-> branch :indexes :avtec :index)
                                              [::comparator/min
                                               ::comparator/min
                                               ::comparator/min
                                               ::comparator/min
                                               ::comparator/min])))

          (is (= '([:type :person 0 1 :set]
                   [:type :person 1 2 :set])
                 (index/inclusive-subsequence (-> branch :indexes :avtec :index)
                                              [:type :person 0 ::comparator/min ::comparator/min])))

          (is (= 1 (transaction-log/last-transaction-number (:transaction-log branch))))

          (is (= #{1 2} (common/entities db :type :person)))
          (is (= #{1 2} (common/entities branch :type :person)))

          (is (= 1 (:last-transaction-number db-value)))

          (testing "transacting a branch"
            (db/transact branch #{[1 :name :set "1 name 1 in branch"]})

            (is (= 2 (transaction-log/last-transaction-number (:transaction-log branch))))

            (is (= '([1 :name 0 :set "1 name 1 in base"]
                     [1 :name 2 :set "1 name 1 in branch"])
                   (common/datoms branch :eatcv [1 :name])))

            (is (= #{"1 name 1 in branch"}
                   (common/values-from-eatcv-datoms (common/matching-datoms-from-index  (-> branch :indexes :eatcv)
                                                                               [1 :name])))))

          (testing "transacting base database after branching"
            (db/transact db #{[2 :name :set "2 name 2 in base"]})

            (is (= #{"2 name 1 in base"}
                   (common/values-from-eatcv-datoms (common/datoms branch :eatcv [2 :name]))))

            (is (= #{"2 name 1 in base"}
                   (common/values-from-eatcv-datoms (common/datoms db-value :eatcv [2 :name]))))

            (is (= #{"2 name 2 in base"}
                   (common/values-from-eatcv-datoms (common/datoms db :eatcv [2 :name])))))

          (db/transact branch #{[1 :name :set "1 name 2 in branch"]})


          (is (= '([0 #{[1 :name :set "1 name 1 in base"] [1 :type :set :person]}]
                   [1 #{[2 :name :set "2 name 1 in base"] [2 :type :set :person]}]
                   [2 #{[1 :name :set "1 name 1 in branch"]}]
                   [3 #{[1 :name :set "1 name 2 in branch"]}])
                 (transaction-log/subseq (:transaction-log branch)
                                         0)))

          (is (= #{[1 :name :set "1 name 2 in branch"]}
                 (branch/squash @branch)))))))

(deftest test-client-and-server
  (let [index-definitions common/base-index-definitions
        server-db (-> (common/db-from-index-definitions index-definitions
                                                        (fn [_index-name] (btree-index/create-memory-btree-index 21))
                                                        (sorted-map-transaction-log/create))
                      (common/transact #{[:entity-1 :name :set "Name in first root"]})
                      (btree-db/store-index-roots-after-maximum-number-of-transactions 0)
                      (common/transact #{[:entity-1 :name :set "Name after first root"]}))
        server-state-atom (atom (server-api/create-state server-db))
        client (client/->InProcessClient server-state-atom)
        server-btree-db (server-btree-db/create client index-definitions)
        db-value-1 (deref server-btree-db)]

    (is (= '([:entity-1 :name 0 :set "Name in first root"]
             [:entity-1 :name 1 :set "Name after first root"])
           (common/datoms db-value-1 :eatcv [])))

    (db/transact server-btree-db
                 #{[:entity-1 :name :set "Name after creation"]})

    (is (= '([:entity-1 :name 0 :set "Name in first root"]
             [:entity-1 :name 1 :set "Name after first root"])
           (common/datoms db-value-1 :eatcv [])))

    (let [db-value-2 @server-btree-db]

      (is (= '([:entity-1 :name 0 :set "Name in first root"]
               [:entity-1 :name 1 :set "Name after first root"]
               [:entity-1 :name 2 :set "Name after creation"])
             (common/datoms db-value-2 :eatcv [])))

      (is (= #{[:entity-1 :name 1 :set "Name after first root"]
               [:entity-1 :name 2 :set "Name after creation"]}
             (-> server-btree-db :indexes :eatcv :index :index-atom deref :branch-datom-set :sorted-set-atom deref)))

      (swap! server-state-atom
             update
             :db
             btree-db/store-index-roots-after-maximum-number-of-transactions
             0)

      (let [db-value-3 @server-btree-db]
        (is (= '([:entity-1 :name 0 :set "Name in first root"]
                 [:entity-1 :name 1 :set "Name after first root"]
                 [:entity-1 :name 2 :set "Name after creation"])
               (common/datoms db-value-3 :eatcv [])))

        (is (= '([:entity-1 :name 0 :set "Name in first root"]
                 [:entity-1 :name 1 :set "Name after first root"]
                 [:entity-1 :name 2 :set "Name after creation"])
               (btree/inclusive-subsequence (-> server-btree-db :indexes :eatcv :index :index-atom deref :base-sorted-datom-set :btree-index-atom)
                                            ::comparator/min)))

        (is (= 2 (:last-transaction-number @server-btree-db)))

        (let [client-branch (branch/create @server-btree-db)
              client-branch-value-1 @client-branch]
          (is (= '([:entity-1 :name 0 :set "Name in first root"]
                   [:entity-1 :name 1 :set "Name after first root"]
                   [:entity-1 :name 2 :set "Name after creation"])
                 (common/datoms client-branch-value-1 :eatcv [])))

          (common/transact! client-branch
                            #{[:entity-1 :name :set "Name 1 in client"]})

          (is (= '([:entity-1 :name 0 :set "Name in first root"]
                   [:entity-1 :name 1 :set "Name after first root"]
                   [:entity-1 :name 2 :set "Name after creation"]
                   [:entity-1 :name 3 :set "Name 1 in client"])
                 (common/datoms client-branch :eatcv [:entity-1])))

          (common/transact! client-branch
                            #{[:entity-1 :name :set "Name 2 in client"]})

          (is (= #{[:entity-1 :name :set "Name 2 in client"]}
                 (branch/squash @client-branch)))

          (db/transact server-btree-db
                       (set (branch/squash @client-branch)))

          (is (= '([:entity-1 :name 0 :set "Name in first root"]
                   [:entity-1 :name 1 :set "Name after first root"]
                   [:entity-1 :name 2 :set "Name after creation"])
                 (common/datoms db-value-3 :eatcv [:entity-1 :name])))

          (is (= '([:entity-1 :name 0 :set "Name in first root"]
                   [:entity-1 :name 1 :set "Name after first root"]
                   [:entity-1 :name 2 :set "Name after creation"]
                   [:entity-1 :name 3 :set "Name 2 in client"])
                 (common/datoms @server-btree-db :eatcv [:entity-1 :name]))))))))

(deftest test-client-and-server-2
  (let [db (common/db-from-index-definitions common/base-index-definitions
                                             sorted-set-index/creator
                                             (sorted-map-transaction-log/create))]
    (db/transact db #{[:entity-1 :name :set "Name 1 in db"]})

    (is (= "Name 1 in db" (common/value @db :entity-1 :name)))

    (let [branch (branch/create @db)]
      (is (= "Name 1 in db" (common/value @branch :entity-1 :name)))

      (db/transact branch #{[:entity-1 :name :set "Name 2 in branch"]})

      (is (= "Name 2 in branch" (common/value @branch :entity-1 :name)))
      (is (= "Name 1 in db" (common/value @db :entity-1 :name)))

      (db/transact db (branch/squash @branch))

      (is (= "Name 2 in branch" (common/value @db :entity-1 :name)))

      (let [branch (branch/create @db)]
        (is (= "Name 2 in branch" (common/value @branch :entity-1 :name)))

        (db/transact branch #{[:entity-1 :name :set "Name 3 in branch"]})

        (is (= "Name 3 in branch" (common/value @branch :entity-1 :name)))
        (is (= "Name 2 in branch" (common/value @db :entity-1 :name)))))))
