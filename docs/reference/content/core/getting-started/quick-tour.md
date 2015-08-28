+++
date = "2015-08-25T11:13:02-04:00"
draft = true
title = "Quick Tour"
[menu.main]
  parent = "Core Getting Started"
  identifier = "Core Quick Tour"
  weight = 10
  pre = "<i class='fa'></i>"
+++

# Mongo Spark Connector Quick Tour

The following code snippets come from the `QuickTour.java` example code that
can be found with the [connector source]({{< srcref "examples/tour/src/main/java/com/mongodb/spark/examples/tour/QuickTour.java" >}}).

{{% note %}}
See the [installation guide]({{< relref "installation-guide.md" >}})
for instructions on how to install the Mongo Spark connector.
{{% /note %}}

## Configuration
Each worker node has to create its own MongoClient in order to be able to
connect to a MongoDB. In the application, users must configure client
options and connection properties such as database name and collection name.

### Client Configuration
Users must configure a client provider to send to each node in Spark. The
client provider implementation provides support for all options supported by
[`MongoClientURI`](http://api.mongodb.org/java/current/com/mongodb/MongoClientURI.html)
and any additional options supported by
[`MongoClientOptions.Builder`](http://api.mongodb.org/java/current/com/mongodb/MongoClientOptions.Builder.html).

```java
MongoClientProvider clientProvider = new MongoSparkClientProvider(uri);
```

See [Client Setup]({{< relref "core/reference/client/index.md" >}}) for
detailed information on setting up the client provider.

### Collection Configuration
RDDs can only be computed from single collections. Users must configure a
collection provider to allow nodes to create connections for each partition's
calculation.

```java
MongoCollectionProvider<Document> collectionProvider =
        new MongoSparkCollectionProvider<>(Document.class, clientProvider, database, collection);
```

## MongoSparkContext
A `MongoSparkContext` provides several `parallelize` methods that utilize a
collection as an input source to a JavaRDD. These methods allow for options
including the `maxChunkSize` and `pipeline`.

* `maxChunkSize` specifies the max chunk size for partitions in MB; this
  parameter is used when calculating partitions for a non-sharded collection.
  See
  [Modify Chunk Size in a Sharded Cluster](http://docs.mongodb.org/manual/tutorial/modify-chunk-size-in-sharded-cluster/)
  for more information about chunk size limitations.
* `pipeline` specifies an aggregation pipeline to filter the source data. See
  [Aggregation](http://docs.mongodb.org/manual/aggregation/) for more
  information about aggregation.

Splitting collections into chunks requires an index on the splitting key. Due
to the nature of the splitting implementation, only a single `splitKey` may
be used to split collections. In the case of compound indexes, please use the
minimal prefix index key. For example, `a` would be the minimal prefix key
of the compound index `{"a" : 1, "b" : 1, "c" : 1}`.

In order to efficiently pass collection providers to worker nodes, the
implementation will broadcast collection provider variables through the Spark
Context before calculating the RDD.

Below is an example of parallelizing an RDD:

```java
JavaRDD<Document> jrdd = msc.parallelize(Document.class, collectionProvider, splitKey);
```

## MongoRDD
Some users, especially those using the connector in Scala, may prefer working
with base RDDs. `MongoRDD` is a simple extension of the `RDD` class that
offers all the functionality of the base class but takes Mongo collections as
input. `MongoRDD` construction takes similar parameters to a MongoSparkContext
parallelize call.

```java
MongoRDD<Document> rdd = new MongoRDD<>(sc, collectionProvider, Document.class, splitKey);
```

## Writing to a Mongo Collection
RDDs may be written to Mongo collections through use of the `MongoWriter`
class.

```java
MongoWriter.writeToMongo(rdd, collectionProvider, isUpsert, isOrdered);
```
