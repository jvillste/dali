(ns argumentica.btree-test
  (:require (argumentica [btree :as btree])
            (clojure.test.check [clojure-test :refer [defspec]]
                                [generators :as gen]
                                [properties :as properties]))
  (:use clojure.test))

(defn keyword-to-var [keyword]
  (get (ns-publics (symbol (namespace keyword)))
       (symbol (name keyword))))

(defn reached-nodes [btree]
  (reduce (fn [reached-nodes cursor]
            (apply conj reached-nodes cursor))
          (sorted-set)
          (btree/cursors (btree/first-cursor btree)
                         btree)))

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
  (= (btree/loaded-node-count btree)
     (count (reached-nodes btree))))

(defspec test-all-nodes-reachable
  200
  (properties/for-all [size gen/pos-int]
                      (all-nodes-reachable? (btree/create-test-btree 3 size))))

(def add-generator (gen/tuple (gen/return ::btree/add)
                              gen/pos-int))

(def unload-least-used-node-generator (gen/tuple (gen/return ::btree/unload-least-used-node)))

(def command-generator (gen/frequency [[9 unload-least-used-node-generator]
                                       [10 add-generator]]))




(defn apply-commands-to-new-btree [commands]
  (reduce (fn [btree [command & arguments]]
            (apply (keyword-to-var command)
                   btree
                   arguments))
          (btree/create (btree/full-after-maximum-number-of-values 3))
          commands))

(comment
  (btree/least-used-cursor (apply-commands-to-new-btree [[:argumentica.btree/add 9]
                                 [:argumentica.btree/add 0]
                                 [:argumentica.btree/add 3]
                                 [:argumentica.btree/unload-least-used-node]
                                 [:argumentica.btree/add 1]
                                 [:argumentica.btree/add 8]
                                 [:argumentica.btree/add 8]
                                 [:argumentica.btree/unload-least-used-node]
                                 #_[:argumentica.btree/add 2]
                                 #_[:argumentica.btree/unload-least-used-node]])))

(defspec all-nodes-stay-reachable
  100
  (properties/for-all [commands (gen/vector command-generator)]
                      (all-nodes-reachable? (apply-commands-to-new-btree commands))))

(def test-btree (apply-commands-to-new-btree [[:argumentica.btree/add 9]
                                              [:argumentica.btree/add 0]
                                              [:argumentica.btree/add 3]
                                              [:argumentica.btree/unload-least-used-node]
                                              [:argumentica.btree/add 1]
                                              [:argumentica.btree/add 8]
                                              [:argumentica.btree/add 8]
                                              [:argumentica.btree/unload-least-used-node]
                                              #_[:argumentica.btree/add 2]
                                              #_[:argumentica.btree/unload-least-used-node]]))

(defn start []
  (clojure.pprint/pprint (select-keys test-btree
                                      [:nodes :root-id]))

  (select-keys (btree/add test-btree 2)
               [:nodes :root-id]))
