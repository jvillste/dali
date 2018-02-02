(ns argumentica.merge-test
  (:require (argumentica [btree-db :as btree-db]
                         [sorted-set-db :as sorted-set-db]))
  (:use clojure.test))


(deftest test
  (is (= nil
         (let [server-btree-db (-> (btree-db/create-memory-btree-index)
                                   (btree-db/set :entity-1 :name "Foo")
                                   (btree-db/store-index-roots))
               client-btree-db (btree-db/)]
           server-btree-db))))


