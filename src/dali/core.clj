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

(defprotocol SortedDatomSet
  (add-datom! [this datom])
  (subseq-from [this first-datom])
  (last-transaction-number [this]))

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

(s/defschema Command
  (s/enum :add :retract :set))

(s/defschema Statement
  [(s/one s/Any "entity")
   (s/one s/Keyword "attribute")
   (s/one Command "command")
   (s/one s/Any "value")])

#_(s/defschema Statement
  {:entity s/Any
   :attribute s/Keyword
   :command Command
   :value s/Any})

(s/defschema Transaction
  #{Statement})

(def validate-transaction (s/validator Transaction))

(s/defschema Datom
  "A vector that contains a transaction number along with other values."
  (s/both [s/Any]
          (s/pred (fn [datom] (some integer? datom))
                  "Contains a transaction number")))

(s/defschema StatementToDatoms
  "A function that defines what datoms an index contains."
  (s/=> [Datom]

        DatabaseReference
        s/Int ;; transaction number
        Statement))

(s/defschema SortedDatomSetCreator
  (s/=> (s/protocol SortedDatomSet)

        s/Str ;; key
        ))

(s/defschema DatomTransactionNumber
  (s/=> s/Int
        Datom))

(s/defschema IndexDefinition
  {:key s/Keyword
   :statement-to-datoms StatementToDatoms
   :datom-transaction-number-index s/Int})

(s/defschema Index
  (merge IndexDefinition
         {:sorted-datom-set (s/protocol SortedDatomSet)
          (s/optional-key :last-indexed-transaction-number) s/Int}))

(s/defschema IndexMap
  {s/Keyword Index})

(s/defschema DatabaseReference
  {:transaction-log (s/protocol TransactionLog)
   :index-map IndexMap})

(s/defschema DatabaseValue
  {:index-map IndexMap
   :last-transaction-number s/Int})

(s/defschema Branch
  {:base-database DatabaseValue
   :branch-database DatabaseReference})
