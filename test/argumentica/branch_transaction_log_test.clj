(ns argumentica.branch-transaction-log-test
  (:require [argumentica.branch-transaction-log :as branch-transaction-log]
            [clojure.test :refer :all]
            [argumentica.sorted-map-transaction-log :as sorted-map-transaction-log]
            [argumentica.transaction-log :as transaction-log]))

(deftest test
  (let [base-transaction-log (sorted-map-transaction-log/create)]
    (transaction-log/add! base-transaction-log #{[1 :name :set "base name 1"]})
    (transaction-log/add! base-transaction-log #{[1 :name :set "base name 2"]})
    (is (= 1 (transaction-log/last-transaction-number base-transaction-log)))

    (let [branch-transaction-log (branch-transaction-log/create base-transaction-log
                                                                0
                                                                (sorted-map-transaction-log/create))]
      (is (= 0 (transaction-log/last-transaction-number branch-transaction-log)))
      (transaction-log/add! branch-transaction-log #{[1 :name :set "branch name 3"]})
      (is (= 1 (transaction-log/last-transaction-number branch-transaction-log)))

      (is (= '([0 #{[1 :name :set "base name 1"]}]
               [1 #{[1 :name :set "branch name 3"]}])
             (transaction-log/subseq branch-transaction-log
                                     0)))

      (is (= '([0 #{[1 :name :set "base name 1"]}]
               [1 #{[1 :name :set "base name 2"]}])
             (transaction-log/subseq base-transaction-log
                                     0))))))
