## Data tree

Datomic like databses that can be branched and merged like git repositories.

# Datom

* a tuple of:
  * entity
  * attribute
  * transaction number ( unique in branch )
  * value
  * command ( one of "add", "retract" or "set" )
  
# Transaction

* A set of datoms
* ordered set of references to zero or more parent transactions
  
# Branch

* a sorted set of transactions that reference each other as their first parent
* transactions in a branch are numbered starting from the first transaction that does not have a parent belonging to the branch

# Branch part

* transactions in a branch that have numbers in a given range 

# Querying

Each branch is indexed as a sorted set of datoms. To query the database state as of a given transaction, a sorted set of all of the datoms contained in the transactions anchestors need to be queried. Each branch is split to branch parts that are ordered in a specific order and an index lookup is done for each branch part.


<img src="https://raw.github.com/jvillste/argumentica/master/doc/querying.png" />
