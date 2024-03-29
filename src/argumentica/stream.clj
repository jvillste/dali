(ns argumentica.stream
  (:require [argumentica.db.multifile-transaction-log :as multifile-transaction-log]
            [argumentica.file-atom :as file-atom]
            [argumentica.sorted-map-transaction-log :as sorted-map-transaction-log]
            [argumentica.temporary-ids :as temporary-ids]
            [argumentica.transaction-log :as transaction-log]
            [clojure.java.io :as io]))

(defn in-memory [& [{:keys [id next-id create-atom] :or {id (java.util.UUID/randomUUID)
                                                         next-id 0}}]]
  {:id id
   :next-id-atom (atom next-id)
   :transaction-log (sorted-map-transaction-log/create (when create-atom {:create-atom create-atom}))})

(defn on-disk [directory-path & [{:keys [id next-id] :or {id (java.util.UUID/randomUUID)
                                                          next-id 0}}]]
  (io/make-parents directory-path "x")
  {:id @(file-atom/create (str directory-path "/id")
                          id)
   :next-id-atom (file-atom/create (io/file directory-path "next-id")
                                   next-id)
   :transaction-log (let [transaction-log-directory (io/file directory-path "transaction-log")]
                      (io/make-parents transaction-log-directory "x")
                      (multifile-transaction-log/open transaction-log-directory))})

(defn write! [stream temporary-changes & [validate]]
  (let [temporary-id-resolution (temporary-ids/temporary-id-resolution @(:next-id-atom stream)
                                                                       temporary-changes)
        changes (temporary-ids/assign-temporary-ids temporary-id-resolution
                                                    temporary-changes)]
    (when (or (nil? validate)
              (validate changes))
      (reset! (:next-id-atom stream)
              (temporary-ids/new-next-id temporary-id-resolution))
      (transaction-log/add! (:transaction-log stream)
                            changes)
      {:temporary-id-resolution temporary-id-resolution
       :changes changes})))


(comment
  (let [stream (on-disk "temp/test-stream")
        #_(in-memory)]
    ;; (write! stream [[:add :id/t1 :name "foo"]])
    ;; (write! stream [[:add :id/t1 :name "bar2"]])
    #_(into [] (transaction-log/subreducible (:transaction-log stream)
                                             0))
    (:id stream))
  )
