# Mongo Spark Connector Introduction

The following code snippets can be found in [Introduction.scala](../examples/src/test/scala/tour/Introduction.scala).

This introduction expects you to have a basic working knowledge of MongoDB and Apache Spark. Refer to the 
[MongoDB documentation](https://docs.mongodb.org/) and [Spark documentation](https://spark.apache.org/docs/latest/).

## Prerequisites

Have MongoDB up and running and the Spark 1.6.x downloaded. This tutorial will use the Spark Shell allowing for instant feedback.

### Configuring the Mongo connector

Before loading the Spark Shell and creating the `SparkContext`, the Mongo Connector needs to be configured.  The easiest way is to set 
the `mongodb.input.uri` and `mongodb.output.uri` properties, which can be used to easily configure the `database` and `collection` as well as 
the `readPreference`, `readConcern` and `writeConcern`. 

Alternatively, as each connector config option has a specific named property you can set any the properties directly. See the 
`ReadConfig` or `WriteConfig` classes for more information.

An example configuration for the uri is:
```
spark.mongodb.input.uri=mongodb://127.0.0.1/databaseName.collectionName?readPreference=primaryPreferred
```

Which is the same as:
```
spark.mongodb.input.uri=mongodb://127.0.0.1/
spark.mongodb.input.database=databaseName
spark.mongodb.input.collection=collectionName
spark.mongodb.input.readPreference.name=primaryPreferred
```

### Loading the Spark-Shell
As the Mongo Spark Connector is currently in pre-release, we'll be using the SNAPSHOT.  The Spark Shell can load Jars from the local maven 
repository and if not present it can download it from Sonatype. 


To load the Spark Shell, set the uri configuration and download the connector run:

```
./bin/spark-shell --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/test.coll?readPreference=primaryPreferred" \
                  --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/test.coll" \
                  --packages org.mongodb.spark:mongo-spark-connector_2.10:0.1.0-SNAPSHOT \
                  --repositories https://oss.sonatype.org/content/repositories/snapshots
```

*Note:* If there's an error loading the shell you may need to clear your local ivy cache. 


## RDD support

Connecting to MongoDB happens automatically, when an `RDD` action requires to load or save data to MongoDB.
To enable the Mongo Connector specific functions on the `SparkContext` and `RDD`:

```scala
import com.mongodb.spark._
```

*Note:* Currently, Scala types eg: Lists are not supported and should be converted to their java equivalent.
To do this use the `.asJava` method which becomes available after `import scala.collection.JavaConverters._`.

### Saving data from an RDD to MongoDB

As this is a quick tour, we'll save some data via Spark into MongoDB.

*Note:* When saving `RDD` data into MongoDB, it must be a type that can be converted into a Bson document. 
You may have add a `map` step to transform the data into a `Document` (or `BsonDocument` a `DBObject`).

Add Documents to the collection:

```scala
import org.bson.Document
val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))
documents.saveToMongoDB()
```

The `spark.mongodb.output` namespace configures outputting data. If using the default uri from above 
*mongodb://127.0.0.1/test.coll* this will insert the documents into the *"coll"* collection in the *"test"* database.

To change which collection the data is inserted into or how the data is inserted, supply a `WriteConfig` to the `sc.saveToMongoDB`. 
The following example saves data to the "spark" collection, with a `majority` WriteConcern:
                                                                                                                             
```scala
import com.mongodb.spark.config._

val defaultWriteConfig = WriteConfig(sc)
val writeConfig = WriteConfig(Map("collection" -> "spark", "writeConcern.w" -> "majority"), Some(defaultWriteConfig))

val sparkDocuments = sc.parallelize(sc.parallelize((1 to 10).map(i => Document.parse(s"{spark: $i}"))))
sparkDocuments.saveToMongoDB(writeConfig)
```

### Loading and analyzing data from MongoDB

Now we have some data in MongoDB we can use the `sc.loadFromMongoDB` method to create an `RDD` representing a collection:

```scala
val rdd = sc.loadFromMongoDB()
println(rdd.count)
println(rdd.first.toJson)
```

The `spark.mongodb.input` namespace configures reading data. If using the default uri from above 
*mongodb://127.0.0.1/test.coll* this will read the documents from the *"coll"* collection in the *"test"* database.

To change where the data is read from or how the data is read, supply a `ReadConfig` to the `sc.loadFromMongoDB`. 
The following example reads from the "spark" collection, with a `secondaryPreferred` ReadPreference:
                                                                                                                             
```scala
import com.mongodb.spark.config._

val defaultReadConfig = ReadConfig(sc)
val readConfig = ReadConfig(Map("collection" -> "spark", "readPreference.name" -> "secondaryPreferred"), Some(defaultReadConfig))
val customRdd = sc.loadFromMongoDB(readConfig = readConfig)
println(customRdd.count)
println(customRdd.first.toJson)
```

### Aggregations

Spark RDDs only support two types of operations: *transformations* and *actions*. 
Transformations such as mapping or filtering are saved and only applied once an action such as loading data is called.

With RDD's its important to understand what data from MongoDB is loaded into Spark. Filtering data may seen a simple transformation but it 
can be imperformant. The following example filters all documents where the "test" field has a value greater than 5:

```scala
val filteredRdd = rdd.filter(doc => doc.getInteger("test") > 5)
println(filteredRdd.count)
println(filteredRdd.first.toJson)
```

Where possible filter the data in MongoDB, so that then less data has to be passed over the wire into Spark.  A `MongoRDD` instance can be 
passed an [aggregation pipeline](https://docs.mongodb.org/manual/core/aggregation-pipeline/) and this allows a user to filter data from 
MongoDB before its passed to Spark.

The following example also filters all documents where the "test" field has a value greater than 5 but only those matching documents are 
passed to Spark.

```scala
val aggregatedRdd = rdd.withPipeline(Seq(Document.parse("{ $match: { test : { $gt : 5 } } }")))
println(aggregatedRdd.count)
println(aggregatedRdd.first.toJson)
```
Any aggregation pipeline is valid and pre aggregating data in MongoDB may be more performant than doing it via Spark.

[Next - Spark SQL](1-sparkSQL.md)
