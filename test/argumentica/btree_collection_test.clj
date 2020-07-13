(ns argumentica.db.common-test
  (:require [argumentica.btree-collection :as btree-collection]
            [argumentica.mutable-collection :as mutable-collection]
            [clojure.test :refer :all]))

(defn create-collection [& datoms]
  (let [collection (btree-collection/create-memory-based {:node-size 3})]
    (doseq [datom datoms]
      (mutable-collection/add! collection datom))
    collection))

(deftest test-btree-collection
  (is (= '(4 5 6)
         (subseq (create-collection 1 2 3 4 5 6)
                 >=
                 4))))

