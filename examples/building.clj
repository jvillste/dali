(ns building
  (:require [argumentica.db.common :as common]
            [argumentica.db.file-transaction-log :as file-transaction-log]
            [argumentica.sorted-map-transaction-log :as sorted-map-transaction-log]
            [argumentica.sorted-set-index :as sorted-set-index]
            [argumentica.transaction-log :as transaction-log])
  (:use [clojure.test]))

(def transaction-log (file-transaction-log/create "data/temp/log"))

(deftest test
  (let [transaction-log (sorted-map-transaction-log/create)
        indexes (common/index-definition-to-indexes common/base-index-definition
                                                    sorted-set-index/creator)]

    (transaction-log/add! transaction-log
                          [1 :name :set "foo"])

    (common/update-indexes! transaction-log
                            indexes)

    (is (= nil
           (common/values-from-eatcv (:eatcv indexes)
                                   1
                                   :name)))))
