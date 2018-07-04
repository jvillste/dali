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

(s/defschema TransactionStatement
  [(s/one s/Any "entity")
   (s/one s/Keyword "attribute")
   (s/one s/Int "transaction")
   (s/one Command "command")
   (s/one s/Any "value")])

(s/defschema Transaction
  [Statement])

(def validate-transaction (s/validator Transaction))

(s/defschema Datom
  "A vector that contains a transaction number along with other values."
  (s/both [s/Any]
          (s/pred (fn [datom] (some integer? datom))
                  "Contains a transaction number")))

(s/defschema GenerateDatoms
  "A function from database reference and transaction statement to datoms."
  (s/=> [Datom]

        DatabaseReference
        TransactionStatement))

(s/defschema IndexDefinition
  {:key s/Keyword
   :generate-datoms GenerateDatoms})

(s/defschema Index
  (merge IndexDefinition
         {:sortred-datom-set (s/protocol SortedDatomSet)}))

(s/defschema IndexMap
  {s/Keyword IndexDefinitionWithIndex})

(s/defschema Branch
  {:base-database DatabaseValue
   :branch-database DatabaseValue})

(s/defschema DatabaseReference
  {:transaction-log (s/protocol TransactionLog)
   :index-map IndexMap})

(s/defschema DatabaseValue
  {:index-map IndexMap
   :last-transaction-number s/Int})
