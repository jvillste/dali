(ns argumentica.db.directory-transaction-log
  (:require (argumentica [transaction-log :as transaction-log])
            [me.raynes.fs :as fs]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.string :as string])
  (:import [java.nio.file Files Paths OpenOption LinkOption]
           [java.nio.file.attribute FileAttribute])
  (:use clojure.test))


(defrecord FileTransactionLog [log-file-path state-atom])

(defn log-to-string [log]
  (string/join "\n"
               (map pr-str
                    log)))

(defn reset-log-file! [log-file-path log]
  (let [temporary-log-file-path (str log-file-path ".new")]
    (spit temporary-log-file-path
          (log-to-string log))
    (fs/rename temporary-log-file-path
               log-file-path)))

(defn read-and-fix-log! [log-file-path]
  (with-open [reader (io/reader log-file-path)]
    (loop [lines (line-seq reader)
           log (sorted-map)]
      (if-let [line (first lines)]
        (if-let [[transaction-number statements] (try (edn/read-string line)
                                                      (catch Exception exception
                                                        (reset-log-file! log-file-path log)
                                                        nil))]
          (recur (rest lines)
                 (assoc log transaction-number statements))
          log)
        log))))

(defn create [log-file-path]
  (->FileTransactionLog log-file-path
                        (atom {:in-memory-log (if (fs/exists? log-file-path)
                                                (read-and-fix-log! log-file-path)
                                                (sorted-map))})))

(defn write-to-log-file! [log-file-path transaction-number statements]
  (spit log-file-path
        (prn-str [transaction-number statements])
        :append true))

(defn add-transaction! [state log-file-path transaction-number statements]
  (write-to-log-file! log-file-path
                      transaction-number
                      statements)
  (update state
          :in-memory-log
          assoc
          transaction-number
          statements))

(defn truncate-sorted-map [the-sorted-map first-preserved-key]
  (apply sorted-map (apply concat (filter (fn [[key _value]]
                                            (<= first-preserved-key
                                                key))
                                          the-sorted-map))))

(deftest test-truncate-sorted-map
  (is (= {2 :2
          3 :3}
         (truncate-sorted-map (sorted-map 1 :1 2 :2 3 :3)
                              2))))



(deftest test-log-to-string
  (is (= "[1 [[1 :name :set \"Foo 1\"] [2 :name :set \"Foo 2\"]]]\n[2 [[1 :name :set \"Bar 1\"] [2 :name :set \"Bar 2\"]]]"
         (log-to-string (sorted-map 1 [[1 :name :set "Foo 1"]
                                       [2 :name :set "Foo 2"]]

                                    2 [[1 :name :set "Bar 1"]
                                       [2 :name :set "Bar 2"]])))))


(defn truncate! [state log-file-path first-preserved-transaction-number]
  (let [truncated-log (truncate-sorted-map (:in-memory-log state)
                                           first-preserved-transaction-number)]

    (reset-log-file! log-file-path truncated-log)
    (assoc state :in-memory-log truncated-log)))

(defn synchronously-apply-to-state [file-transaction-log function & arguments]
  (locking (:state-atom file-transaction-log)
    (apply swap!
           (:state-atom file-transaction-log)
           function
           arguments))
  file-transaction-log)

(defmethod transaction-log/truncate! FileTransactionLog
  [this first-preserved-transaction-number]
  (synchronously-apply-to-state this
                                truncate!
                                (:log-file-path this)
                                first-preserved-transaction-number))

(defmethod transaction-log/add! FileTransactionLog
  [this transaction-number statements]
  (synchronously-apply-to-state this
                                add-transaction!
                                (:log-file-path this)
                                transaction-number
                                statements))

(defmethod transaction-log/subseq FileTransactionLog
  [this first-transaction-number]
  (subseq (:in-memory-log @(:state-atom this))
          >=
          first-transaction-number))

(defmethod transaction-log/last-transaction-number FileTransactionLog
  [this]
  (first (last (:in-memory-log @(:state-atom this)))))


(comment
  (write-to-log-file! "data/temp/log"
                      3
                      [[1 :name :set "Foo 4"]])

  (read-and-fix-log! "data/temp/log")


  (-> (create "data/temp/log")
      (transaction-log/truncate! 30)
      (transaction-log/add! 1 [[1 :name :set "Bar 1"]
                               [2 :name :set "Bar 2"]])
      (transaction-log/add! 2 [[1 :name :set "Baz 1"]])
      (transaction-log/subseq 2))

  )
