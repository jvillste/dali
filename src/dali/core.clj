(ns dali.core
  (:require [schema.core :as s]))

(defprotocol TransactionLog
  (add-transaction! [this transaction-statements])
  (inclusive-subsequence [this first-transaction-number])
  (last-transaction-number [this])

  (truncate! [this first-transaction-number-to-preserve])
  (close! [this])
  (make-transient! [this])
  (make-persistent! [this]))

(defprotocol Index
  (add-datom! [this datom])
  (inclusive-subsequence [this first-datom]))

(defprotocol Btree
  (store-root! [this metadata]))

(s/defschema Database
  {:transaction-log (s/protocol TransactionLog)
   :indexes {s/Keyword {:last-indexed-transaction-number s/Int
                        :index (s/protocol Index)}}})

(s/defschema DatabaseReference
  {:database Database
   :transaction-number s/Int})


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

(s/defschema EatcvToDatom
  (s/=> Datom
        s/Any s/Keyword s/Int Command s/Any))

(s/defschema IndexDefinition
  {s/Keyword EatcvToDatom})

(s/defschema Branch
  {:base-database-reference DatabaseReference
   :branch-database Database})
