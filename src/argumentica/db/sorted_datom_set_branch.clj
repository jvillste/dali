(ns argumentica.db.sorted-datom-set-branch
  (:require [argumentica.index :as index]
            [argumentica.comparator :as comparator]
            [argumentica.util :as util]
            [argumentica.db.common :as common])
  (:use clojure.test))


(defrecord SortedDatomSetBranch [base-sorted-datom-set
                                 base-transaction-number
                                 datom-transaction-number
                                 branch-datom-set])

(util/defn-alias create ->SortedDatomSetBranch)

(defmethod index/add!
  SortedDatomSetBranch
  [this value]
  (println "adding " value)
  (index/add! (:branch-datom-set this)
              value))

(defmethod index/inclusive-subsequence
  SortedDatomSetBranch
  [this key]
  (concat (take-while (fn [datom]
                        (and (common/pattern-matches? key datom)
                             (<= ((:datom-transaction-number this) datom)
                                 (:base-transaction-number this))))
                      (index/inclusive-subsequence (:base-sorted-datom-set this)
                                                   key))
          (index/inclusive-subsequence (:branch-datom-set this)
                                       key)))
