Parallel to hbase-book, but is written in Scala and uses Cassandra as database.

#### Timestamps in Cassandra

One can designate timestamp (hector API call it clock) when inserting or deleting a row. So naturally I *think* I can get all versions of one cell back. Actually I expect it that way after reading BigTable and HBase, for example. It can't. Reason seems to be that timestamp is only for Cassandra to solve conflict. Well, even if that's how Cassandra works, the way its API is designed and documented with regard to timestamp / version is misleading.

Some good pointers after I asked on SO: [Cassandra Range Query Using CompositeType](http://randomizedsort.blogspot.com/2011/11/cassandra-range-query-using.html) and [Indexing in Cassandra](http://www.anuff.com/2011/02/indexing-in-cassandra.html).