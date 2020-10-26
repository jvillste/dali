(ns argumentica.btree-test
  (:require (argumentica [btree :as btree])
            (clojure.test.check [clojure-test :refer [defspec]]
                                [generators :as gen]
                                [properties :as properties])
            [clojure.test.check :as check]
            [argumentica.util :as util])
  (:use clojure.test))


(def command-generator (gen/frequency [[8 (gen/tuple (gen/return #'btree/add-3)
                                                     gen/pos-int)]
                                       [1 (gen/tuple (gen/return #'btree/unload-least-used-node))]
                                       [1 (gen/tuple (gen/return #'btree/unload-btree))]
                                       [1 (gen/tuple (gen/return #'btree/unload-excess-nodes)
                                                     (gen/return 2))]
                                       [1 (gen/tuple (gen/return #'btree/store-root-2))]
                                       [1 (gen/tuple (gen/return #'btree/remove-old-roots-2))]
                                       [1 (gen/tuple (gen/return #'btree/collect-storage-garbage))]]))

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
         (apply-commands-to-new-sorted-set [[#'argumentica.btree/add-3 0]
                                            [#'argumentica.btree/unload-least-used-node]
                                            [#'argumentica.btree/add-3 1]]))))


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

(defspec property-test-btree 100
  (properties/for-all [commands (gen/vector command-generator)
                       start-value gen/int]
                      (try
                        (util/cancel-after-timeout 1000
                                                   false
                                                   (= (sorted-set-result-for-parameters [commands start-value])
                                                      (btree-result-for-parameters [commands start-value])))
                        (catch Exception e
                          false))))

(defn property-test-reduce* [values first-value]
  (let [btree-atom (atom (reduce btree/add-3
                                 (btree/create-2 (btree/full-after-maximum-number-of-values 3))
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
            (btree/reduce-btree conj
                                []
                                btree-atom
                                first-value
                                :forwards))
         (= backward-subsequence
            (btree/reduce-btree conj
                                []
                                btree-atom
                                first-value
                                :backwards)))))

(defspec property-test-reduce 100
  (properties/for-all* [(gen/vector gen/int)
                        gen/int]
                       property-test-reduce*))



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
