(ns argumentica.db.branch
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
  (concat (common/eat-datoms (:origin-btree-db branch)
                             entity-id
                             attribute
                             (:origin-transaction-number branch)
                             false)
          (common/eat-datoms (:branch-sorted-set-db branch)
                             entity-id
                             attribute
                             (common/last-transaction-number (:branch-sorted-set-db branch))
                             false)))

(defn transaction [branch]
  (map common/datom-to-eacv-statemnt
       (common/squash-datoms (common/datoms (:branch-sorted-set-db branch)))))

(defn values [branch entity-id attribute]
  (common/values-from-eatcv-statements (datoms branch entity-id attribute)))


(deftest test
  (let [server-btree-db (-> (btree-db/create-memory-btree-db)
                            (btree-db/set :entity-1 :name "Foo")
                            (btree-db/store-index-roots))
        branch (create-branch server-btree-db
                              (common/last-transaction-number server-btree-db))]
    
    (is (= #{"Foo"}
           (values branch :entity-1 :name)))
    
    (let [branch (update branch :branch-sorted-set-db
                         sorted-set-db/set :entity-1 :name "Bar")]

      (is (= '([:entity-1 :name 0 :set "Foo"]
               [:entity-1 :name 0 :set "Bar"])
             (datoms branch :entity-1 :name)))
      
      (is (= #{"Bar"}
             (values branch :entity-1 :name)))

      (let [branch (update branch :branch-sorted-set-db
                           sorted-set-db/set :entity-1 :name "Baz")]

        (is (= #{"Baz"}
               (values branch :entity-1 :name)))

        (is (= [[:entity-1 :name :set "Baz"]]
               (transaction branch)))))))



