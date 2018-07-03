(ns argumentica.kiss-test
  (:require [argumentica.db.common :as common]
            [argumentica.index :as index]
            [argumentica.sorted-map-transaction-log :as sorted-map-transaction-log]
            [argumentica.sorted-set-index :as sorted-set-index]
            [argumentica.transaction-log :as transaction-log]
            [clojure.test :refer :all]))

(defn transact! [transaction-log indexes statements]
  (transaction-log/add! transaction-log
                        statements)

  (common/update-indexes! transaction-log
                          indexes))

(deftest test-local-indexes
  (let [transaction-log (sorted-map-transaction-log/create)
        indexes (common/index-definitions-to-indexes sorted-set-index/creator
                                                     common/base-index-definitions)]

    (transaction-log/add! transaction-log
                          [[1 :name :set "foo"]])

    (is (= '([0 [[1 :name :set "foo"]]])
           (transaction-log/subseq transaction-log
                                   0)))

    (common/update-indexes! transaction-log
                            indexes)

    (is (= '([:name "foo" 0 1 :set])
           (index/inclusive-subsequence (-> indexes :avtec :index)
                                        nil)))

    (is (= '([1 :name 0 :set "foo"])
           (common/datoms-from-index  (-> indexes :eatcv :index)
                                      [1 :name nil nil nil])))

    (is (= #{"foo"}
           (common/values-from-eatcv (-> indexes :eatcv :index)
                                     1
                                     :name)))

    (is (= 0 (transaction-log/last-transaction-number transaction-log)))

    (transact! transaction-log
               indexes
               [[1 :name :add "bar"]])

    (is (= 1 (transaction-log/last-transaction-number transaction-log)))

    (is (= '([1 :name 0 :set "foo"]
             [1 :name 1 :add "bar"])
           (index/inclusive-subsequence (-> indexes :eatcv :index)
                                        nil)))

    (is (= #{"foo" "bar"}
           (common/values-from-eatcv (-> indexes :eatcv :index)
                                     1
                                     :name)))))


