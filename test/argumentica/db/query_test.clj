(ns argumentica.db.query-test
  (:require [argumentica.db.common :as common]
            (argumentica [hash-map-storage :as hash-map-storage]
                         [sorted-set-db :as sorted-set-db]
                         [btree :as btree]
                         [index :as index]
                         [comparator :as comparator]
                         [sorted-map-transaction-log :as sorted-map-transaction-log])
            [argumentica.btree-index :as btree-index]
            [argumentica.sorted-set-index :as sorted-set-index]
            [medley.core :as medley]
            [argumentica.db.query :as sut]
            [clojure.test :as t]
            [argumentica.db.query :as query])
  (:use clojure.test))

(defn create-eav-db [& transactions]
  (reduce common/transact
          (common/db-from-index-definitions [common/eav-index-definition]
                                            (fn [index-key] (sorted-set-index/create))
                                            (sorted-map-transaction-log/create))
          transactions))

(defn create-index [& datoms]
  (let [index (sorted-set-index/create)]
    (doseq [datom datoms]
      (index/add! index datom))
    index))

(deftest test-datoms
  (let [index (-> (create-index [:entity-1 :attribute-1 :add 1]
                                [:entity-1 :attribute-1 :add 2]
                                [:entity-1 :attribute-1 :add 3]))]

    (is (= '([:entity-1 :attribute-1 1 0 :add]
             [:entity-1 :attribute-1 2 0 :add]
             [:entity-1 :attribute-1 3 0 :add])
           (query/datoms index
                         [])))

    (is (= '([:entity-1 :attribute-1 2 0 :add]
             [:entity-1 :attribute-1 3 0 :add])
           (query/datoms index
                         [:entity-1 :attribute-1 2])))

    (is (= '([:entity-1 :attribute-1 3 0 :add]
             [:entity-1 :attribute-1 2 0 :add]
             [:entity-1 :attribute-1 1 0 :add])
           (query/datoms index
                         []
                         {:reverse? true})))

    (is (= '([:entity-1 :attribute-1 2 0 :add]
             [:entity-1 :attribute-1 1 0 :add])
           (query/datoms index
                         [:entity-1 :attribute-1 2]
                         {:reverse? true})))

    (is (= '([:entity-1 :attribute-1 2 0 :add])
           (query/datoms index
                         [:entity-1 nil 2]
                         {:reverse? true})))))



(deftest test-index-substitutions
  (is (= nil (query/index-substitutions (create-index [:a :b]
                                                      [:a :c])
                                        [:a :b?])))
  )
