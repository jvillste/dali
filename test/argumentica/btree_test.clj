(ns argumentica.btree-test
  (:require (argumentica [btree :as btree])
            (clojure.test.check [clojure-test :refer [defspec]]
                                [generators :as gen]
                                [properties :as properties])
            [clojure.test.check :as check]
            [argumentica.util :as util])
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

(defn unload-excess-nodes [btree]
  (btree/unload-excess-nodes btree
                             5))

(def command-generator (gen/frequency [[1 (gen/tuple (gen/return #'btree/unload-least-used-node))]
                                       [1 (gen/tuple (gen/return #'btree/unload-btree))]
                                       [1 (gen/tuple (gen/return #'unload-excess-nodes))]
                                       [1 (gen/tuple (gen/return #'btree/store-root-2))]
                                       [1 (gen/tuple (gen/return #'btree/remove-old-roots-2))]
                                       [1 (gen/tuple (gen/return #'btree/collect-storage-garbage))]
                                       [8 (gen/tuple (gen/return #'btree/add-3)
                                                     gen/pos-int)]]))

(defn apply-commands-to-new-btree [commands]
  (reduce (fn [btree [command & arguments]]
            (apply command
                   btree
                   arguments))
          (btree/create-2 (btree/full-after-maximum-number-of-values 2))
          commands))

(defn apply-commands-to-new-sorted-set [commands]
  (reduce (fn [sorted-set [command & arguments]]
            (if-let [sorted-set-command (get {#'btree/add-3 conj}
                                             command)]
              (apply sorted-set-command
                     sorted-set
                     arguments)
              sorted-set))
          (sorted-set)
          commands))

(deftest test-apply-commands-to-new-sorted-set
  (is (= #{0 1}
         (apply-commands-to-new-sorted-set [[#'argumentica.btree/add 0]
                                            [#'argumentica.btree/unload-least-used-node]
                                            [#'argumentica.btree/add 1]]))))

(defspec property-test-reached-nodes
  (properties/for-all [commands (gen/vector command-generator)]
                      (every? btree/loaded-node-id?
                              (reached-nodes (apply-commands-to-new-btree commands)))))


(defspec test-all-nodes-stay-reachable
  (properties/for-all [commands (gen/vector command-generator)]
                      (all-nodes-reachable? (apply-commands-to-new-btree commands))))

(defn btree-result-for-parameters [[commands smallest]]
  (btree/reduce-btree conj
                      []
                      (atom (apply-commands-to-new-btree commands))
                      smallest
                      :forwards))

(defn sorted-set-result-for-parameters [[commands smallest]]
  (or (subseq (apply-commands-to-new-sorted-set commands)
           >=
           smallest)
      []))

(comment
  (def test-parameters [[[#'argumentica.btree/add-3 0]
                         [#'argumentica.btree/add-3 1]
                         [#'argumentica.btree/add-3 0]
                         [#'argumentica.btree/unload-least-used-node]
                         [#'argumentica.btree/collect-storage-garbage]]
                        0])

  (apply-commands-to-new-btree (first test-parameters))

  (sorted-set-result-for-parameters test-parameters)

  (btree-result-for-parameters test-parameters)
  )

(defspec property-test-btree 1000
  (properties/for-all [commands (gen/vector command-generator)
                       smallest gen/int]
                      (try
                        (util/cancel-after-timeout 1000
                                                   false
                                                   (= (sorted-set-result-for-parameters [commands smallest])
                                                      (btree-result-for-parameters [commands smallest])))
                        (catch Exception e
                          false))))

(defn property-test-transduce* [values first-value]
  (let [btree-atom (atom (reduce btree/add
                                 (btree/create (btree/full-after-maximum-number-of-values 3))
                                 values))
        forward-subsequence (or (subseq (apply btree/create-sorted-set
                                               values)
                                        >=
                                        first-value)
                                [])
        backward-subsequence (or (rsubseq (apply btree/create-sorted-set
                                                 values)
                                          <=
                                          first-value)
                                 [])]
    (and (= forward-subsequence
            (btree/transduce-btree btree-atom
                                   first-value
                                   {:reducer conj}))
         (= backward-subsequence
            (btree/transduce-btree btree-atom
                                   first-value
                                   {:reducer conj
                                    :direction :backwards})))))

(defspec property-test-transduce 1000
  (properties/for-all* [(gen/vector gen/int)
                        gen/int]
                       property-test-transduce*))


(defn create-btree [values]
  (reduce btree/add
          (btree/create (btree/full-after-maximum-number-of-values 3))
          values))

(deftest test-transduce
  (is [[:a :b]]
      (btree/transduce-btree (atom (create-btree [[:a :b]]))
                             [:a]
                             {:reducer conj}))

  (is [[:a :b :c]]
      (btree/transduce-btree (atom (create-btree [[:a :b :c]]))
                             [:a nil :c]
                             {:reducer conj})))

(comment

  ;; preformance testing

  (time (let [btree-atom (atom (btree/create-test-btree 11 1000))]
          (dotimes [i 10000]
            (doall (take 1 (btree/inclusive-subsequence btree-atom
                                                        (rand-int 1000)))))))

  (time (let [btree-atom (atom (btree/create-test-btree 11 1000))]
          (taoensso.tufte/profile {}
                                  (taoensso.tufte/p :total
                                                    (dotimes [i 10000]
                                                      (taoensso.tufte/p :transduce-btree
                                                                        (btree/transduce-btree btree-atom
                                                                                               (rand-int 1000)
                                                                                               :transducer (take 1)
                                                                                               :reducer conj)))))))

  )
