(ns argumentica.kiss-test
  (:require [argumentica.db.common :as common]
            [argumentica.db.branch :as branch]
            [argumentica.index :as index]
            [argumentica.sorted-map-transaction-log :as sorted-map-transaction-log]
            [argumentica.sorted-set-index :as sorted-set-index]
            [argumentica.transaction-log :as transaction-log]
            [clojure.test :refer :all]
            [flatland.useful.map :as map]
            [clojure.tools.trace :as trace]
            [flow-gl.tools.trace :as fun-trace]
            [clojure.walk :as walk]
            [clojure.string :as string]))

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
           (common/datoms-from-index  (-> @indexes-atom :eatcv :index)
                                      [1 :name nil nil nil])))

    (is (= #{"foo"}
           (common/values-from-eatcv (-> @indexes-atom :eatcv :index)
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
           (common/values-from-eatcv (-> @indexes-atom :eatcv :index)
                                     1
                                     :name)))))


(comment
  (fun-trace/show-value (branch/create (common/deref (common/transact! {:transaction-log (sorted-map-transaction-log/create)
                                                                        :indexes (common/index-definitions-to-indexes sorted-set-index/creator
                                                                                                                      common/base-index-definitions)}
                                                                       #{[1 :name :set "1 name 1 in base"]}))))

  (fun-trace/show-value (common/transact! {:transaction-log (sorted-map-transaction-log/create)
                                           :indexes (common/index-definitions-to-indexes sorted-set-index/creator
                                                                                         common/base-index-definitions)}
                                          #{[1 :name :set "1 name 1 in base"]}))
  )

(deftest test-branch
  (do #_fun-trace/with-trace
      (fun-trace/untrace-ns 'argumentica.db.common)
      (fun-trace/untrace-ns 'argumentica.sorted-map-transaction-log)
      (fun-trace/untrace-ns 'argumentica.sorted-set-index)
      (let [db (common/db-from-index-definitions common/base-index-definitions
                                                 sorted-set-index/creator
                                                 (sorted-map-transaction-log/create))]

        (testing "Transacting two statements in to the database"
          (common/transact! db #{[1 :name :set "1 name 1 in base"]})

          (common/transact! db #{[2 :name :set "2 name 1 in base"]})


          (is (= '([1 :name 0 :set "1 name 1 in base"])
                 (common/datoms db :eatcv [1 :name])))

          (is (= '([2 :name 1 :set "2 name 1 in base"])
                 (common/datoms db :eatcv [2 :name]))))

        (let [db-value (common/deref db)
              branch (branch/create db-value)]

          (is (= 1 (:last-transaction-number db-value)))

          (testing "transacting a branch"
            (common/transact! branch #{[1 :name :set "1 name 1 in branch"]})

            (is (= '([1 :name 0 :set "1 name 1 in base"]
                     [1 :name 0 :set "1 name 1 in branch"])
                   (common/datoms branch :eatcv [1 :name])))

            (is (= '([2 :name 1 :set "2 name 1 in base"])
                   (common/datoms branch :eatcv [2 :name])))

            (is (= #{"1 name 1 in branch"}
                   (common/values-from-eatcv-datoms (common/datoms-from-index  (-> branch :indexes :eatcv :index)
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

          (is (= '([1 :name :set "1 name 2 in branch"])
                 (common/squash-transaction-log (:transaction-log branch))))))))
