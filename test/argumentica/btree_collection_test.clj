(ns argumentica.btree-collection-test
  (:require [argumentica.btree-collection :as btree-collection]
            [argumentica.mutable-collection :as mutable-collection]
            [clojure.test :refer :all]
            [argumentica.btree :as btree]))

(defn create-collection [& datoms]
  (let [collection (btree-collection/create-in-memory {:node-size 3})]
    (doseq [datom datoms]
      (mutable-collection/add! collection datom))
    collection))

(deftest test-btree-collection
  (is (= '(4 5 6)
         (subseq (create-collection 1 2 3 4 5 6)
                 >=
                 4))))

(deftest test-unload
  (is (= '(4 5 6)
         (subseq (btree-collection/locking-apply-to-btree! (create-collection 1 2 3 4 5 6)
                                                           btree/unload-excess-nodes
                                                           0)
                 >=
                 4))))

