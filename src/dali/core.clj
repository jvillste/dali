(ns dali.core
  (:require [schema.core :as s]))

(defprotocol TransactionLog
  (add-transaction! [this transaction-statements])
  (subseq-from [this first-transaction-number])
  (last-transaction-number [this])

  (truncate! [this first-transaction-number-to-preserve])
  (close! [this])
  (make-transient! [this])
  (make-persistent! [this]))

(defprotocol Index
  (add-datom! [this datom])
  (subseq-from [this first-datom])
  (last-indexed-transaction-number [this]))

(defprotocol Btree
  (store-root! [this metadata]))

(defprotocol Storage
  (get [this key])
  (put! [this key value])
  (delete! [this key])
  (keys [this])
  (contains? [this key]))

(defprotocol DatabaseReference
  (transact! [this statements])
  (dereference-database [this]))

(s/defschema DatabaseReference
  {:transaction-log (s/protocol TransactionLog)
   :indexes {s/Keyword (s/protocol Index)}})

(s/defschema DatabaseValue
  {:indexes {s/Keyword (s/protocol Index)}
   :last-transaction-number s/Int})

(s/defschema Command
  (s/enum :add :retract :set))

(s/defschema Statement
  [(s/one s/Any "entity")
   (s/one s/Keyword "attribute")
   (s/one Command "command")
   (s/one s/Any "value")])

(s/defschema Datom
  (s/both [s/Any]
          (s/pred (fn [datom] (some integer? datom))
                  "Contains a transaction number")))

(s/defschema StatementToDatom
  (s/=> Datom

        DatabaseValue
        s/Int ;; transaction number
        s/Any ;; entity
        s/Keyword ;; attribute
        Command ;; command
        s/Any ;; value
        ))

(s/defschema IndexDefinition
  {s/Keyword StatementToDatom})

(s/defschema Branch
  {:base-database DatabaseValue
   :branch-database DatabaseValue})
