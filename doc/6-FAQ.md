# Mongo Spark Connector FAQ

## How can I achieve data locality?

MongoDB can be configured in multiple ways: Standalone, ReplicaSet, Sharded Standalone, Sharded ReplicaSet.  
In any configuration the Mongo Spark connector sets the preferred location for an RDD to be where the data is:

* For a non sharded system it sets the preferred location to be the hostname(s) of the standalone or replicaSet.
* For a sharded system it sets the preferred location to be the hostname(s) of where the chunk data is.

To achieve full data locality you should ensure:

  * That there is a Spark Worker on one of the hosts or one per shard.
  * That you use a `nearest` ReadPreference to read from the local `mongod`.
  * If sharded you should have a `MongoS` on the same nodes and use `localThreshold` configuration to connect to the nearest `MongoS`.


