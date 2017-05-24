# Requirements

* Branching / merging
* Live editing the same DB by multiple people
* Inter database references
  * Globaly unique ids
* Merging databses
* Read scalability
* ACID transactions

# Solution

Datomic that can be branced and merged like git repositories.

# Transaction

* consists of:
  * parent transaction hashes
  * hash
  * A set of [command entity attribute value] -tuples
* a command is one of
  * add
  * retract
  * set ( sets the new value as the only value of the attribute
* Transaction can be refered to by it's hash to add metadata in another transaction
* A special entity id "transaction" can be used to add metadata about the transaction in the same transaction

Example:

  [[set 1 name foo]
   [add transaction transact-time 12345]]

# Branch

* a set of transactions

## EATV index

* consists of:
  * A sorted set of [command entity attribute transaction-number value]
  * A sorted set of [transaction-number transaction-hash]
* can share structure with other indexes sharing the same transaction history

# Types

Entity ids and attributes are strings.

Values can be one of:

* Number (Arbitrary precision decimal number)
* String
* Binary

# Sort order

* 

# Schema

* No schema on the database level. Schema can be enforced by the applications.

# Merge

  [1 name 2 Foo-1.2]
  [1 name 1 Foo]
  
  and
  
  [1 name 2 Foo-2.2]
  [1 name 1 Foo]
  
  merges to
  
  [1 name 3 Foo-1.2]
  [1 name 2 Foo-2.2]
  [1 name 1 Foo]
  
  --------------------
  
  [3 name 2 Baz]
  [1 name 1 Foo]
  
  and
  
  [2 name 2 Bar]
  [1 name 1 Foo]
  
  merges to
  
  [3 name 3 Baz]
  [2 name 2 Bar]
  [1 name 1 Foo]



