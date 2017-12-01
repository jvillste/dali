(ns argumentica.btree-test
  (:require (argumentica [btree :as btree])
            (clojure.test.check [clojure-test :refer [defspec]]
                                [generators :as gen]
                                [properties :as properties])
            [clojure.test.check :as check])
  (:use clojure.test))

(defn reached-nodes [btree]
  (into (sorted-set)
        (filter btree/loaded-node-id?
                (apply concat
                       (btree/all-cursors btree)))))

(deftest test-reached-nodes
  (is (= #{0 1 2 3 4 5 6 7}
         (reached-nodes (btree/create-test-btree 3 10))))

  (is (= #{0}
         (reached-nodes (btree/create-test-btree 3 0))))

  (is (= #{}
         (reached-nodes (btree/unload-least-used-node (btree/create-test-btree 3 0)))))

  (is (= #{}
         (reached-nodes (btree/unload-btree (btree/create-test-btree 3 10))))))


(defn all-nodes-reachable? [btree]
  (= (into #{} (keys (:nodes btree)))
     (reached-nodes btree)))

(defspec test-all-nodes-reachable?
  100
  (properties/for-all [size gen/pos-int]
                      (all-nodes-reachable? (btree/create-test-btree 3 size))))


(def command-generator (gen/frequency [[9 (gen/tuple (gen/return #'btree/unload-least-used-node))]
                                       [10 (gen/tuple (gen/return #'btree/add)
                                                      gen/pos-int)]]))

(defn apply-commands-to-new-btree [commands]
  (reduce (fn [btree [command & arguments]]
            (apply command
                   btree
                   arguments))
          (btree/create (btree/full-after-maximum-number-of-values 3))
          commands))


(defspec property-test-reached-nodes
   (properties/for-all [commands (gen/vector command-generator)]
                      (every? btree/loaded-node-id?
                              (reached-nodes (apply-commands-to-new-btree commands)))))


(defspec test-all-nodes-stay-reachable
  (properties/for-all [commands (gen/vector command-generator)]
                      (all-nodes-reachable? (apply-commands-to-new-btree commands))))

