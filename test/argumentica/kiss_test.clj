(ns argumentica.kiss-test
  (:require [argumentica.db.common :as common]
            [argumentica.db.branch :as branch]
            [argumentica.index :as index]
            [argumentica.sorted-map-transaction-log :as sorted-map-transaction-log]
            [argumentica.sorted-set-index :as sorted-set-index]
            [argumentica.transaction-log :as transaction-log]
            [clojure.test :refer :all]
            [flatland.useful.map :as map]
            [clojure.tools.trace :as trace]
            [flow-gl.tools.trace :as fun-trace]
            [clojure.walk :as walk]
            [clojure.string :as string]))

(defn transact! [transaction-log indexes-atom statements]
  (transaction-log/add! transaction-log
                        statements)

  (swap! indexes-atom
         common/update-indexes!
         transaction-log))

(deftest test-local-indexes
  (let [transaction-log (sorted-map-transaction-log/create)
        indexes-atom (atom (common/index-definitions-to-indexes sorted-set-index/creator
                                                                common/base-index-definitions))]

    (transaction-log/add! transaction-log
                          #{[1 :name :set "foo"]})

    (is (= '([0 #{[1 :name :set "foo"]}])
           (transaction-log/subseq transaction-log
                                   0)))

    (swap! indexes-atom
           common/update-indexes!
           transaction-log)

    (is (= '([:name "foo" 0 1 :set])
           (index/inclusive-subsequence (-> @indexes-atom :avtec :index)
                                        nil)))

    (is (= '([1 :name 0 :set "foo"])
           (common/datoms-from-index  (-> @indexes-atom :eatcv :index)
                                      [1 :name nil nil nil])))

    (is (= #{"foo"}
           (common/values-from-eatcv (-> @indexes-atom :eatcv :index)
                                     1
                                     :name)))

    (is (= 0 (transaction-log/last-transaction-number transaction-log)))

    (transact! transaction-log
               indexes-atom
               #{[1 :name :add "bar"]})

    (is (= 1 (transaction-log/last-transaction-number transaction-log)))

    (is (= 2 (common/first-unindexed-transacion-number-for-index-map @indexes-atom)))

    (is (= '([1 :name 0 :set "foo"]
             [1 :name 1 :add "bar"])
           (index/inclusive-subsequence (-> @indexes-atom :eatcv :index)
                                        nil)))

    (is (= #{"foo" "bar"}
           (common/values-from-eatcv (-> @indexes-atom :eatcv :index)
                                     1
                                     :name)))))

;; from https://dev.clojure.org/jira/browse/CLJ-1550
(defn package-name [^Class class]
  (let [class (.getName class)
	idx (clojure.string/last-index-of class \.)]
    (when (pos? idx)
      (subs class 0 idx))))

(defn class-to-ns-name [^Class class]
  (-> (package-name class)
      (string/replace #"_" "-")))

(defn class-to-map-constructor [^Class class]
  (find-var (symbol (str (class-to-ns-name class)
                         "/"
                         (str "map->" (.getSimpleName class))))))

(defn map-record-values [record function]
  ((class-to-map-constructor (type record))
   (map/map-vals record
                 function)))

(defn deep-deref [value]
  (cond (instance? clojure.lang.Atom value)
        (atom (deep-deref (deref value)))

        (record? value)
        (map-record-values value
                           deep-deref)
        (map? value)
        (map/map-vals value
                      deep-deref)

        (vector? value)
        (mapv deep-deref value)

        :default
        value))

(comment
  (map-record-values (sorted-map-transaction-log/->SortedMapTransactionLog (atom :foo))
                     deref)
  (map/map-vals (sorted-map-transaction-log/->SortedMapTransactionLog (atom :foo))
                deref)
  
  (map? (sorted-set-index/create))
  
  (map-record-values (sorted-map-transaction-log/create)
                     deref)

  (deep-deref {:foo (atom 2)})
  (deep-deref (atom {:transaction-log (sorted-map-transaction-log/create)
                     :indexes (common/index-definitions-to-indexes sorted-set-index/creator
                                                                   common/base-index-definitions)}))

  (def base-db-val (common/deref (common/transact! {:transaction-log (sorted-map-transaction-log/create)
                                                    :indexes (common/index-definitions-to-indexes sorted-set-index/creator
                                                                                                  common/base-index-definitions)}
                                                   #{[1 :name :set "1 name 1 in base"]})))

  (fun-trace/with-trace (branch/create base-db-val))

  (deep-deref {:foo (common/index-definitions-to-indexes sorted-set-index/creator
                                                         common/base-index-definitions)})
  
  (.lastIndexOf )
  (the-ns (symbol ( class-to-ns-name (type (sorted-map-transaction-log/create)))))
  (name (class-to-map-constructor (type (sorted-map-transaction-log/create))))
  (the-ns 'foo.bar)
  (the-ns 'argumentica.sorted-map-transaction-log.map->SortedMapTransactionLog)
  
  (type (type (sorted-map-transaction-log/create)))
  (.getCanonicalName (type (sorted-map-transaction-log/create)))
  (.getSimpleName (type (sorted-map-transaction-log/create)))
  (.getTypeName (type (sorted-map-transaction-log/create)))
  (ns-publics (symbol (string/replace "argumentica.sorted_map_transaction_log"
                                      #"_"
                                      "-")))

  (map/map-vals (sorted-map-transaction-log/create)
                deref)

  (deep-deref (atom {:transaction-log (sorted-map-transaction-log/create)
                     :indexes (common/index-definitions-to-indexes sorted-set-index/creator
                                                                   common/base-index-definitions)}))

  (fun-trace/untrace-ns 'argumentica.db.common)
  (fun-trace/untrace-ns 'argumentica.sorted-map-transaction-log))

(deftest test-branch
  (fun-trace/with-trace
    (fun-trace/untrace-ns 'argumentica.db.common)
    (fun-trace/untrace-ns 'argumentica.sorted-map-transaction-log)
    (fun-trace/untrace-ns 'argumentica.sorted-set-index)
    (let [db-atom (atom {:transaction-log (sorted-map-transaction-log/create)
                         :indexes (common/index-definitions-to-indexes sorted-set-index/creator
                                                                       common/base-index-definitions)})]

      (fun-trace/log (deep-deref db-atom))

      (swap! db-atom
             common/transact!
             #{[1 :name :set "1 name 1 in base"]})

      (fun-trace/log (deep-deref db-atom))

      (swap! db-atom
             common/transact!
             #{[2 :name :set "2 name 1 in base"]})

      (fun-trace/log (deep-deref db-atom))

      (is (= '([1 :name 0 :set "1 name 1 in base"])
             (common/datoms-from-index (-> @db-atom :indexes :eatcv :index)
                                       [1 :name])))

      (is (= '([2 :name 1 :set "2 name 1 in base"])
             (common/datoms-from-index (-> @db-atom :indexes :eatcv :index)
                                       [2 :name])))

      (is (= 2 (common/first-unindexed-transacion-number @db-atom)))

      (let [db-value (common/deref @db-atom)
            branch-atom (atom (branch/create db-value))]

        (fun-trace/log (deep-deref branch-atom))

        (is (= 1 (:last-transaction-number db-value)))

        (swap! branch-atom
               common/transact!
               #{[1 :name :set "1 name 1 in branch"]})

        (fun-trace/log (deep-deref branch-atom))

        (prn (-> @branch-atom :indexes :eatcv :index))

        (is (= '([1 :name 0 :set "1 name 1 in base"] 
                 [1 :name 0 :set "1 name 1 in branch"])
               (index/inclusive-subsequence (-> @branch-atom :indexes :eatcv :index)
                                            [1 :name])))

        (is (= '([1 :name 0 :set "1 name 1 in base"]
                 [1 :name 0 :set "1 name 1 in branch"])
               (common/datoms-from-index (-> @branch-atom :indexes :eatcv :index)
                                         [1 :name])))
        (comment
          (prn (identity #_keys (-> @branch-atom :indexes :eatcv :index :branch-datom-set :sorted-set-atom deref)))

          (swap! branch-atom
                 common/transact!
                 #{[1 :name :set "1 name 2"]})

          #_(prn (identity #_keys (-> @branch-atom :indexes :eatcv :index :branch-datom-set :sorted-set-atom deref)))
          (is (= '([1 :name 0 :set "foo"]
                   [1 :name 0 :set "bar"]
                   [1 :name 1 :set "baz"])
                 (common/datoms-from-index (-> @branch-atom :indexes :eatcv :index)
                                           [1 :name])))

          #_(prn (common/datoms-from-index  (-> @branch-atom :indexes :eatcv :index)
                                            []))
          (is (= #{"baz"}
                 (common/values-from-eatcv-datoms (common/datoms-from-index  (-> @branch-atom :indexes :eatcv :index)
                                                                             [1 :name]))))

          (is (= #{"baz"}
                 (common/values-from-eatcv (-> @branch-atom :indexes :eatcv :index)
                                           1
                                           :name)))

          (is (= #{"foo 2"}
                 (common/values-from-eatcv (-> @db-atom :indexes :eatcv :index)
                                           2
                                           :name)))

          (is (= #{"foo 2"}
                 (common/values-from-eatcv (-> @branch-atom :indexes :eatcv :index)
                                           2
                                           :name)))

          (is (= #{"foo in base"}
                 (common/values-from-eatcv (-> @db-atom :indexes :eatcv :index)
                                           1
                                           :name))))))))
