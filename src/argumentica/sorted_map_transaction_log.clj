(ns argumentica.sorted-map-transaction-log
  (:require [argumentica.transaction-log :as transaction-log]
            [argumentica.util :as util]
            [argumentica.reduction :as reduction]
            [schema.core :as schema]))


(defprotocol SortedMapTransactionLogProtocol
  (state-atom [this]))

(deftype SortedMapTransactionLog [state-atom]
  SortedMapTransactionLogProtocol
  (state-atom [this]
    state-atom)

  clojure.lang.Seqable
  (seq [this]
    (map second (seq (:sorted-map @state-atom))))

  clojure.lang.Sorted
  (comparator [this]
    (.comparator (:sorted-map @state-atom)))

  (entryKey [this entry]
    entry)

  (seq [this ascending?]
    (map set (map second (.seq  (:sorted-map @state-atom)
                                ascending?))))
  (seqFrom [this value ascending?]
    (map set (map second (.seqFrom (:sorted-map @state-atom)
                                   value
                                   ascending?)))))

(defmethod print-method SortedMapTransactionLog [this ^java.io.Writer writer]
  (.write writer (with-out-str (clojure.pprint/pprint (seq this)))))

(def create-options {(schema/optional-key :create-atom) (schema/pred fn?)})

(def ^:private default-create-options {:create-atom atom})

(util/defno create [options :- create-options]
  (let [options (merge default-create-options
                       options)]
    (->SortedMapTransactionLog ((:create-atom options) {:sorted-map (sorted-map)}))))

(defmethod transaction-log/last-transaction-number SortedMapTransactionLog
  [this]
  (:last-transaction-number @(state-atom this)))

(defmethod transaction-log/add! SortedMapTransactionLog
  [this statements]
  (let [transaction-number (inc (or (transaction-log/last-transaction-number this)
                                    -1))]
    (swap! (state-atom this)
           (fn [state]
             (-> state
                 (update :sorted-map
                         assoc
                         transaction-number
                         statements)
                 (assoc :last-transaction-number transaction-number)))))
  this)

(defmethod transaction-log/subreducible SortedMapTransactionLog
  [this first-transaction-number]
  (reduction/reducible (fn [reducing-function initial-value]
                         (reduce reducing-function
                                 initial-value
                                 (map second (subseq (:sorted-map @(state-atom this))
                                                     >=
                                                     first-transaction-number))))))

(defmethod transaction-log/subseq SortedMapTransactionLog
  [this first-transaction-number]
  (into []
        (transaction-log/subreducible this
                                      first-transaction-number)))

(defmethod transaction-log/truncate! SortedMapTransactionLog
  [this first-preserved-transaction-number]
  (swap! (state-atom this)
         update
         :sorted-map
         util/filter-sorted-map-keys
         (fn [transaction-number]
           (<= first-preserved-transaction-number
               transaction-number)))
  this)

(defmethod transaction-log/close! SortedMapTransactionLog
  [this]
  this)
