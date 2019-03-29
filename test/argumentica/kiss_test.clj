(ns argumentica.kiss-test
  (:require [argumentica.db.branch :as branch]
            [argumentica.db.common :as common]
            [argumentica.index :as index]
            [argumentica.sorted-map-transaction-log :as sorted-map-transaction-log]
            [argumentica.sorted-set-index :as sorted-set-index]
            [argumentica.transaction-log :as transaction-log]
            [clojure.test :refer :all]
            [argumentica.comparator :as comparator]))

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
           (common/datoms-from-index (-> @indexes-atom :eatcv)
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
          (common/transact! db #{[1 :name :set "1 name 1 in base"]
                                 [1 :type :set :person]})

          (common/transact! db #{[2 :name :set "2 name 1 in base"]
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

        (let [db-value (common/deref db)
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
            (common/transact! branch #{[1 :name :set "1 name 1 in branch"]})

            (is (= '([1 :name 0 :set "1 name 1 in base"]
                     [1 :name 2 :set "1 name 1 in branch"])
                   (common/datoms branch :eatcv [1 :name])))

            (is (= #{"1 name 1 in branch"}
                   (common/values-from-eatcv-datoms (common/datoms-from-index  (-> branch :indexes :eatcv)
                                                                               [1 :name])))))

          (testing "transacting base database after branching"
            (common/transact! db #{[2 :name :set "2 name 2 in base"]})

            (is (= #{"2 name 1 in base"}
                   (common/values-from-eatcv-datoms (common/datoms branch :eatcv [2 :name]))))

            (is (= #{"2 name 1 in base"}
                   (common/values-from-eatcv-datoms (common/datoms db-value :eatcv [2 :name]))))

            (is (= #{"2 name 2 in base"}
                   (common/values-from-eatcv-datoms (common/datoms db :eatcv [2 :name])))))

          (common/transact! branch #{[1 :name :set "1 name 2 in branch"]})


          (is (= '([0 #{[1 :name :set "1 name 1 in base"] [1 :type :set :person]}]
                   [1 #{[2 :name :set "2 name 1 in base"] [2 :type :set :person]}]
                   [2 #{[1 :name :set "1 name 1 in branch"]}]
                   [3 #{[1 :name :set "1 name 2 in branch"]}])
                 (transaction-log/subseq (:transaction-log branch)
                                         0)))

          (is (= '([1 :name :set "1 name 2 in branch"])
                 (branch/squash branch)))))))
