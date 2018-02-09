(ns argumentica.merge-test
  
  (:require [argumentica.db.common :as common]
            (argumentica [btree-db :as btree-db]
                         [index :as index]
                         [sorted-set-db :as sorted-set-db]))
  (:use clojure.test))

(defn create-branch [origin-reference origin-transaction-number]
  {:origin-btree-db (btree-db/create-memory-btree-db-from-reference origin-reference)
   :origin-transaction-number origin-transaction-number
   :branch-sorted-set-db (sorted-set-db/create)})

(defn set [branch entity-id attribute value]
  (update branch
          :branch-sorted-set-db
          sorted-set-db/set
          entity-id
          attribute
          value))


(defn datoms [branch entity-id attribute]
  (concat #_(common/eat-datoms (:branch-sorted-set-db branch)
                               entity-id
                               attribute
                               (common/last-transaction-number (:branch-sorted-set-db branch)))
          (common/eat-datoms (:origin-btree-db branch)
                             entity-id
                             attribute
                             :origin-transaction-number)))

(deftest test
  (let [server-btree-db (-> (btree-db/create-memory-btree-db)
                            (btree-db/set :entity-1 :name "Foo")
                            (btree-db/store-index-roots))
        branch (create-branch server-btree-db
                              (common/last-transaction-number server-btree-db))]
    
    (is (= "Foo"
           (datoms branch :entity-1 :name)))))


