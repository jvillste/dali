# Data tree

Datomic like database that can be branched and merged like a git repository.

## Datom

* a tuple of:
  * entity
  * attribute
  * transaction number ( unique in branch )
  * value
  * command ( one of "add", "retract" or "set" )
  
## Transaction

* A set of datoms
* ordered set of references to zero or more parent transactions
  
## Branch

* a sorted set of transactions that reference each other as their first parent
* transactions in a branch are numbered starting from the first transaction that does not have a parent belonging to the branch

## Branch part

* transactions in a branch that have numbers in a given range 

## Querying

Each branch is indexed as a sorted set of datoms. To query the database state as of a given transaction, a sorted set of all of the datoms contained in the transactions anchestors need to be queried. Each branch is split to branch parts that are ordered in a specific order and an index lookup is done for each branch part.


<img src="https://raw.github.com/jvillste/argumentica/master/doc/querying.png" />

## TODO

* It should be possible to calculate transactions parent parts based on (:brances db) and the branch and transaciton number of the desired db version. There should not be a need to know every transactions parents.

* Client applications need an in memory database that refers to a server database as a basis on which local branch is developed. Eventually the local transactions are squashed and sent to the server as a single transaction. The local branch is rebased periodically to take in to account the server changes between the branching and the transacting of the server.

* All transactions can be saved in to the same index
  * the transaction ids should be sorted in topological order
  * links between branches must be stored separately
  * merging two graphs would invalidate the old indexes. If the old indexes are kept separate, they can be kept in caches
  
* Transactions should have running numbering so that clients can tell if they have got all transactions and in the right order.

  
  
