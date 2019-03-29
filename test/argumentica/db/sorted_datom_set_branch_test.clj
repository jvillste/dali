(ns argumentica.db.sorted-datom-set-branch-test
  (:require [argumentica.db.sorted-datom-set-branch :as sorted-datom-set-branch]
            [clojure.test :refer :all]
            [argumentica.sorted-set-index :as sorted-set-index]
            [argumentica.index :as index]
            [argumentica.index :as index]
            [argumentica.db.common :as common]
            [argumentica.comparator :as comparator]))

(deftest test
  (let [base-datom-set (sorted-set-index/create)]
    (index/add! base-datom-set [1 :name 0 :set "base name 0"])
    (index/add! base-datom-set [1 :name 1 :set "base name 1"])
    (index/add! base-datom-set [1 :age 1 :set 30])
    (index/add! base-datom-set [1 :age 2 :set 32])
    (index/add! base-datom-set [1 :name 2 :set "base name 2"])

    (is (= '([1 :name 0 :set "base name 0"]
             [1 :name 1 :set "base name 1"]
             [1 :name 2 :set "base name 2"])
           (index/inclusive-subsequence base-datom-set
                                        [1 :name 0 ::comparator/min ::comparator/min])))

    (is (= '([1 :age 1 :set 30]
             [1 :age 2 :set 32]
             [1 :name 0 :set "base name 0"]
             [1 :name 1 :set "base name 1"]
             [1 :name 2 :set "base name 2"])
           (index/inclusive-subsequence base-datom-set
                                        [1 :age 0 ::comparator/min ::comparator/min])))

    (is (= ' ([1 :age 1 :set 30]
              [1 :age 2 :set 32]
              [1 :name 0 :set "base name 0"]
              [1 :name 1 :set "base name 1"]
              [1 :name 2 :set "base name 2"])
             (index/inclusive-subsequence base-datom-set
                                          [::comparator/min ::comparator/min ::comparator/min ::comparator/min ::comparator/min])))

    (let [branch-datom-set (sorted-datom-set-branch/create base-datom-set
                                                           1
                                                           2
                                                           (sorted-set-index/create))]

      (is (= '([1 :name 0 :set "base name 0"]
               [1 :name 1 :set "base name 1"])
             (index/inclusive-subsequence branch-datom-set
                                          [1 :name 0 ::comparator/min ::comparator/min])))


      (index/add! branch-datom-set [1 :name 3 :set "branch name 3"])
      (index/add! branch-datom-set [1 :age 4 :set 34])

      (is (= '([1 :name 0 :set "base name 0"]
               [1 :name 1 :set "base name 1"]
               [1 :name 3 :set "branch name 3"])
             (index/inclusive-subsequence branch-datom-set
                                          [1 :name 0 ::comparator/min ::comparator/min])))

      (is (= '([1 :age 1 :set 30]
               [1 :age 4 :set 34]
               [1 :name 0 :set "base name 0"]
               [1 :name 1 :set "base name 1"]
               [1 :name 3 :set "branch name 3"])
             (index/inclusive-subsequence branch-datom-set
                                          [1 :age 0 ::comparator/min ::comparator/min])))

      (is (= '([1 :age 1 :set 30]
               [1 :age 4 :set 34]
               [1 :name 0 :set "base name 0"]
               [1 :name 1 :set "base name 1"]
               [1 :name 3 :set "branch name 3"])
             (index/inclusive-subsequence branch-datom-set
                                          [::comparator/min ::comparator/min ::comparator/min ::comparator/min ::comparator/min]))))))
