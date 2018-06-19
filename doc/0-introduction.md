# Mongo Spark Connector Introduction

The following code snippets can be found in [Introduction.scala](../examples/src/test/scala/tour/Introduction.scala).

This introduction expects you to have a basic working knowledge of MongoDB and Apache Spark. Refer to the 
[MongoDB documentation](https://docs.mongodb.org/) and [Spark documentation](https://spark.apache.org/docs/latest/).

## Prerequisites

Have MongoDB up and running and Spark 2.2.x downloaded. This tutorial will use the Spark Shell allowing for instant feedback.

### Configuring the Mongo Spark Connector

Before loading the Spark Shell which creates a `SparkContext`, the Mongo Connector needs to be configured. The easiest way is to set 
the `mongodb.input.uri` and `mongodb.output.uri` properties. There a few configuration options available, see the [configuration](2-configuring.md) documentation for more information.

### Loading the Spark-Shell
As the Mongo Spark Connector is currently in pre-release, we'll be using the SNAPSHOT.  The Spark Shell can load jars directly. 

To load the Spark Shell, set the uri configuration and download the connector run:

```
./bin/spark-shell --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/test.coll?readPreference=primaryPreferred" \
                  --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/test.coll" \
                  --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.3
```

------
#### Troubleshooting

If you get a `java.net.BindException: Can't assign requested address` when loading the shell, ensure you don't have spark already running. You can also try `export SPARK_LOCAL_IP=127.0.0.1` and/or passing  
`--driver-java-options "-Djava.net.preferIPv4Stack=true"` to spark-shell.

If there's an error running the examples you may need to clear your local ivy cache (~/.ivy2/cache/org.mongodb.spark and ~/.ivy2/jars).

------

## RDD support

Connecting to MongoDB happens automatically when an `RDD` action requires to load data from or save data to MongoDB.
First we enable the Mongo Connector specific functions and implicits for the `SparkContext` and `RDD`:

```scala
import com.mongodb.spark._
```

### Saving data from an RDD to MongoDB

As this is a quick introduction, we'll save some data via Spark into MongoDB first.

------
**Note:** When saving `RDD` data into MongoDB, it must be a type that can be converted into a Bson document. 
You may have add a `map` step to transform the data into a `Document` (or `BsonDocument` a `DBObject`). 

Some Scala types eg: Lists are not supported and should be converted to their java equivalent. Use the `.asJava` method which becomes 
available after `import scala.collection.JavaConverters._`. to do convert from Scala into native types.

------

The `MongoSpark` class and companion provide an easy way to configure and load or save data to MongoDB. First we add some 
documents to the collection:

```scala
import org.bson.Document
val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))

MongoSpark.save(documents) // Uses the SparkConf for configuration
```

The `spark.mongodb.output` namespace configures outputting data. If using the default uri from above 
*mongodb://127.0.0.1/test.coll* this will insert the documents into the *"coll"* collection in the *"test"* database.

To change which collection the data is inserted into or how the data is inserted, supply a `WriteConfig` to the `MongoSpark.save` method. 
The following example saves data to the "spark" collection with a `majority` WriteConcern:

```scala
import com.mongodb.spark.config._

val writeConfig = WriteConfig(Map("collection" -> "spark", "writeConcern.w" -> "majority"), Some(WriteConfig(sc))
val sparkDocuments = sc.parallelize((1 to 10).map(i => Document.parse(s"{spark: $i}")))
MongoSpark.save(sparkDocuments, writeConfig)
```

Implicit helper methods on `RDD` can also be used to save data to MongoDB:

```scala
rdd.saveToMongoDB() // Uses the SparkConf for configuration
rdd.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://example.com/database.collection"))) // Uses the WriteConfig
```

### Loading and analyzing data from MongoDB

Now we have some data in MongoDB we can use the `sc.loadFromMongoDB` method to create an `RDD` representing a collection:

```scala
val rdd = MongoSpark.load(sc)
println(rdd.count)
println(rdd.first.toJson)
```

The `spark.mongodb.input` namespace configures reading data. If using the default uri from above 
*mongodb://127.0.0.1/test.coll* this will read the documents from the *"coll"* collection in the *"test"* database.

To change where the data is read from or how the data is read, supply a `ReadConfig` to the `sc.loadFromMongoDB` method. 
The following example reads from the "spark" collection with a `secondaryPreferred` ReadPreference:

```scala
import com.mongodb.spark.config._

val readConfig = ReadConfig(Map("collection" -> "spark", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
val customRdd = MongoSpark.load(sc, readConfig)
println(customRdd.count)
println(customRdd.first.toJson)
```

Implicit helper methods on the `SparkContext` can also be used to load data from MongoDB:

```scala
sc.loadFromMongoDB() // Uses the SparkConf for configuration
sc.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://example.com/database.collection"))) // Uses the ReadConfig
```

### Aggregations

As mentioned earlier, Spark RDDs only support two types of operations: *Transformations* and *Actions*. 
Transformations such as mapping or filtering are saved and only applied once an action is called.  With RDD's its important to understand 
what data from MongoDB is loaded into Spark. Filtering data may seen a simple transformation but it 
can be imperformant. The following example filters all documents where the "test" field has a value greater than 5:

```scala
val filteredRdd = rdd.filter(doc => doc.getInteger("test") > 5)
println(filteredRdd.count)
println(filteredRdd.first.toJson)
```

-----
**Note:** If you get a `ERROR Executor: Exception in task 0.0 in stage 1.0 (TID 8) java.lang.NullPointerException` its because you
have other data in your collection and the `filter` method doesn't handle `null` data. This is one of the challenges of working with a Document database.  You'll see that by using an aggregation pipeline we can mitigate that risk.

-----

Where possible filter the data in MongoDB and less data has to be passed over the wire into Spark.  A `MongoRDD` instance can be 
passed an [aggregation pipeline](https://docs.mongodb.org/manual/core/aggregation-pipeline/) which allows a user to filter data from 
MongoDB before its passed to Spark.

The following example also filters all documents where the "test" field has a value greater than 5 but *only* those matching documents are 
passed across the wire to Spark.

```scala
val aggregatedRdd = rdd.withPipeline(Seq(Document.parse("{ $match: { test : { $gt : 5 } } }")))
println(aggregatedRdd.count)
println(aggregatedRdd.first.toJson)
```
Any aggregation pipeline is valid, pre aggregating data in MongoDB may be more performant than doing it via Spark in certain circumstances.

-----

## MongoSpark.builder()

If you require granular control over your configuration, then the `MongoSpark` companion provides a `builder()` method for configuring 
all aspects of the Mongo Spark Connector.  It also provides easy methods for going on to create an `RDD`, `DataFrame` or `Dataset`.

[Next - Spark SQL](1-sparkSQL.md)
