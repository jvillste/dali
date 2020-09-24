(ns argumentica.db.file-transaction-log
  (:require (argumentica [transaction-log :as transaction-log])
            [me.raynes.fs :as fs]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.string :as string]
            [argumentica.util :as util])
  (:import [java.nio.file Files Paths OpenOption LinkOption]
           [java.nio.file.attribute FileAttribute])
  (:use clojure.test))


(defrecord FileTransactionLog [log-file-path state-atom]
  java.io.Closeable
  (close [this]
    (.close (:output-stream @state-atom))))

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
                                                (sorted-map))
                               :output-stream (io/output-stream log-file-path
                                                                :append true)})))

(defn write-to-log-file! [output-stream transaction-number statements]
  (.write output-stream
          (.getBytes (prn-str [transaction-number statements])
                     "UTF-8"))
  (.flush output-stream))

(defn add-transaction! [state transaction-number statements]
  (when (not (:is-transient? state))
    (write-to-log-file! (:output-stream state)
                        transaction-number
                        statements))
  (update state
          :in-memory-log
          assoc
          transaction-number
          statements))


(deftest test-log-to-string
  (is (= "[1 [[1 :name :set \"Foo 1\"] [2 :name :set \"Foo 2\"]]]\n[2 [[1 :name :set \"Bar 1\"] [2 :name :set \"Bar 2\"]]]"
         (log-to-string (sorted-map 1 [[1 :name :set "Foo 1"]
                                       [2 :name :set "Foo 2"]]

                                    2 [[1 :name :set "Bar 1"]
                                       [2 :name :set "Bar 2"]])))))

(defn- last-transaction-number [state]
  (first (last (:in-memory-log state))))


(defn truncate! [state log-file-path first-preserved-transaction-number]
  (let [truncated-log (util/filter-sorted-map-keys (:in-memory-log state)
                                                   (fn [transaction-number]
                                                     (<= first-preserved-transaction-number
                                                         transaction-number)))]
    (when (not (:is-transient? state))
      (.close (:output-stream state))
      (reset-log-file! log-file-path truncated-log))

    (-> state
        (assoc :last-indexed-transaction-number-before-truncate (last-transaction-number state))
        (assoc :in-memory-log truncated-log)
        (cond-> (:is-transient? state)
          (assoc  :output-stream (io/output-stream log-file-path :append true))))))

(defn synchronously-apply-to-state! [file-transaction-log function & arguments]
  (locking (:state-atom file-transaction-log)
    (apply swap!
           (:state-atom file-transaction-log)
           function
           arguments))
  file-transaction-log)

(defmethod transaction-log/truncate! FileTransactionLog
  [this first-preserved-transaction-number]
  (synchronously-apply-to-state! this
                                 truncate!
                                 (:log-file-path this)
                                 first-preserved-transaction-number))


(defmethod transaction-log/last-transaction-number FileTransactionLog
  [this]
  (let [state @(:state-atom this)]
    (or (last-transaction-number state)
        (:last-indexed-transaction-number-before-truncate state))))

(defmethod transaction-log/add!-method FileTransactionLog
  [this transaction-number statements]
  (synchronously-apply-to-state! this
                                 add-transaction!
                                 transaction-number
                                 statements))

(defn transient? [file-transaction-log]
  (:is-transient? @(:state-atom file-transaction-log)))

(defn close! [file-transaction-log]
  (if (not (transient? file-transaction-log))
    (.close (:output-stream @(:state-atom file-transaction-log)))))

(defmethod transaction-log/close! FileTransactionLog
  [this]
  (close! this))

(defmethod transaction-log/subseq FileTransactionLog
  [this first-transaction-number]
  (subseq (:in-memory-log @(:state-atom this))
          >=
          first-transaction-number))



(defmethod transaction-log/make-transient! FileTransactionLog
  [file-transaction-log]
  (assert (not (transient? file-transaction-log)))

  (synchronously-apply-to-state! file-transaction-log
                                 (fn [state]
                                   (close! file-transaction-log)
                                   (fs/delete (:log-file-path file-transaction-log))
                                   (assoc state :is-transient? true))))

(defmethod transaction-log/make-persistent! FileTransactionLog
  [file-transaction-log]
  (assert (transient? file-transaction-log))

  (synchronously-apply-to-state! file-transaction-log
                                 (fn [state]
                                   (reset-log-file! (:log-file-path file-transaction-log)
                                                    (:in-memory-log state))
                                   (assoc state
                                          :is-transient? false
                                          :output-stream (io/output-stream (:log-file-path file-transaction-log)
                                                                           :append true)))))

(comment
  (write-to-log-file! "data/temp/log"
                      3
                      [[1 :name :set "Foo 4"]])

  (read-and-fix-log! "data/temp/log")


  (with-open [log (create "data/temp/log")]
    (doto log
      #_(transaction-log/make-transient!)
      (transaction-log/add! #{[1 :name :set "Bar 1"]
                              [2 :name :set "Bar 2"]})
      (transaction-log/add! #{[1 :name :set "Baz 1"]})
      #_(transaction-log/truncate! 2)
      (transaction-log/add! #{[1 :name :set "Foo 2"]})
      #_(transaction-log/make-persistent!))

    #_(prn (transaction-log/subseq log 2))
    #_(prn (transaction-log/last-transaction-number log))))
