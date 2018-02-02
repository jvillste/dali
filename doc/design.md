# Requirements

* Branching / merging
* Live editing the same DB by multiple people
* Inter database references
  * Globaly unique ids
* Merging databses
* Read scalability
* ACID transactions

# Solution

Datomic like databse that can be branced and merged like git repositories.

The database is an asyclic graph of transactions. Each transaction points to one or more parent transactions. One transaction is the root transaction that has no parents.

Transactions specify statements that specify changes to the edges of a graph of [entity attribute value] -tuples.

# Statement

* a statement is [command entity attribute value] -tuple
* a command is one of
  * add
  * retract
  * set ( sets the new value as the only value of the attribute

# Transaction

* consists of:
  * ordered list of parent transaction hashes
	  * Parent branches are applied in the specified order when the transactions are threated as description of a graph.
  * A set of statements
  * Depth: the distance to the root transaction when all of the parent transactions are listed in the specified order. Used in the indexes to order statements.
  * hash of all other fields
* Transaction can be refered to by it's hash to add metadata in another transaction
* A special entity id "transaction" can be used to add metadata about the transaction in the same transaction

Example:

  [[set 1 name foo]
   [add transaction transact-time 12345]]


# Root

* A transaction with no parents.

# Leaf

* A transaction with no children.

# Edge

* [entity attribute value] -tuple

# Path

* An ordered list of transactions between two transactions.

# Fork

* A transaction that has more than one children.

# Merge

* a transaction that has more than one parent.

# Parent ordering

* The order of parents of a merge.

# Trunk

* The transactions on a path where only the first parent are traversed for each merge.

# Branch

* A path from a non first parent of a merge to the first fork.

# Full path

* A path that spans all of the parent transactions from a transaction to the root transaction. The parents of a merge are traversed in reversed parent ordering.

# Depth

* The length of a full path of a transaction.

# Full path depth

* The distance to the root from a transaction on some full path.

# Graph

* a set of edges that are specified by applying all of the statements in all of the transactions of a full path.


## EATVC index

* A sorted set of [entity attribute transaction-full-path-depth value command] -tuples corresponding to a full path.
* Is it possible to share the index between multiple graphs?
  * The same index can be used to query the graph corresponding each transaction in the trunk of one leaf.

# Types

Entity ids and attributes are strings.

Values can be one of:

* Number (Arbitrary precision decimal number)
* String
* Entity id
* Binary

* What if the only data type is binary?

# Value size

* Each value has a maximum size
* A stream of data must be represented with a linked list of values.

# Sort order

* If there are multiple data types they must have a sort order in the index.

# Schema

* No schema on the database level. Schema can be enforced by the applications.

# Branches in the same index

* How share physical index structure between branhces?

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


# How to invalidate entities efficiently?

* Entity registers itself to an atom when it's created
* Entity keeps track of the last transaction that changed one of entitys properties
* 
* Affected entity id's are

# How to manage client changes before committing them to the server

## Pull

* Use a pull pattern to pull a graph to a local db.
* Transact a copy of the original local db.
* Create a diff of the original and changed local db and send that as a transaction to the server.
* The local copy can be refreshed by pulling again.

### Cons

* The pull pattern must be manually defined so that it pulls all of the needed data

## Fork

* Refer a server db version, read the transaction log since the last indexed version and build local indexes from it. Fetch datasegments on demand as the ui components need more data.
* Create a Datomic style reference to the whole server db and use that to fill the UI with data.
* Add transactions to the db reference and thus essentially fork the server db.
* Send the local transactions as a one transaction to the server when the user wants to commit.
* The db reference can be updated to the latest and then the local transactions must be replayed on it.

### Cons

* 


