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

# Where should we keep track of the last indexed transaction number of each sorted datom set?

If each sorted datom set keeps track of it, it needs to be given to it on each add! -call or it needs to know how to get it from the given datom.

Datom sets should only know about datoms, not about transactions.

A datom set without knowing about the last transaction number can not be efficiently updated from a transaction log.

The last transaction number is only needed for efficiency. Adding a datom to a datom set is idempotent.

If the transaction numbers are kept in the index map, it must be stored in an atom and needs to be stored on disk at least occasionally if not after every transaction.


# TODO
* Only in memory db needs to contain transaction numbers and operators in datoms. The on disk datastructure is persistent on node level anyway. Having transaction numbers in datoms improves performance because the there is no need to copy datoms from nodes to another until they fill up.
  * If transaction numbers are not stored on disk datoms, only the versions that are stored as roots are accessible.
* branch parent can be a branch. this needs to be added to the kiss test
* branch/create should take create-index as a parameter
* sorted-datom-set-branch is not needed because it's generic code and not specific to sorted-set

# terms
* operator: "add", "remove" or "set". "set" is expanded to an "add" and to zero or more "remove":s. "set" is not stored in indexes as such.
* tuplet: a list of values in a datom
* operation: a proposition and an operator. It describes a change to an index.
* statement: list of entity attribute operator value
* datom: a tuplet concatenated by transaction number and operator
* index: ordered set of datoms

* pattern: list of constants and variables
* substitution: a map from variables to constants
* tuplect: a sorted set of tuples of a certain length

I have a sorted collection of vectors like in a relational database index. Then I have “patterns” that I want match to those vectors in the collection. Pattern is a vector of contants and varaibles. The result of the matching is a set of substitutions that define what combinations of values the variables in the patterns can have so that there are corresponding vectors in the collection.

The matching occures in different levels:
* one tuple to one pattern
* a tuplect to one pattern
* a tuplect to many patterns
* many tuplects and their corresponding patterns
The last case is called join in relational algebra.


