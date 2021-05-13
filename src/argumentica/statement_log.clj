(ns argumentica.statement-log
  (:require [argumentica.counter :as counter]
            [argumentica.sorted-map-transaction-log :as sorted-map-transaction-log]
            [clojure.string :as string]
            [clojure.test :refer [deftest is]]
            [argumentica.temporary-ids :as temporary-ids]
            [argumentica.file-atom :as file-atom]
            [clojure.java.io :as io]
            [argumentica.db.multifile-transaction-log :as multifile-transaction-log]
            [argumentica.transaction-log :as transaction-log]))

(defn in-memory []
  {:next-id-atom (atom 0)
   :transaction-log (sorted-map-transaction-log/create)})

(defn on-disk [directory-path]
  {:next-id-atom (file-atom/create (io/file directory-path "next-id")
                                   0)
   :transaction-log (let [transaction-log-directory (io/file directory-path "transaction-log")]
                      (io/make-parents transaction-log-directory "x")
                      (multifile-transaction-log/open transaction-log-directory))})

(defn write! [statement-log temporary-statements & [validate]]
  (let [{:keys [temporary-id-resolution statements]} (temporary-ids/temporary-statements-to-statements @(:next-id-atom statement-log)
                                                                                                       temporary-statements)]
    (when (or (nil? validate)
              (validate statements))
      (reset! (:next-id-atom statement-log)
              (temporary-ids/new-next-id temporary-id-resolution))
      (transaction-log/add! (:transaction-log statement-log)
                            statements)
      {:temporary-id-resolution temporary-id-resolution
       :statements statements})))


(comment
  (temporary-ids/temporary-statements-to-statements 0
                                                    [[:add :id/t1 :name "foo"]])

  (let [statement-log (on-disk "temp/test-statement-log")
        #_(in-memory)]
    ;; (write! statement-log [[:add :id/t1 :name "foo"]])
    ;; (write! statement-log [[:add :id/t1 :name "bar"]])
    (into [] (transaction-log/subreducible (:transaction-log statement-log)
                                           0)))
  )
