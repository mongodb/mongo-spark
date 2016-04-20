# Mongo Spark Connector Configuration

The following table describes the various configuration options for the Spark Connector. 

The following connection options can be set via the `SparkConf` object. These are prefixed with `spark.` so that they can be recognized
from the spark-shell and can be passed via --conf settings or via $SPARK_HOME/conf/spark-default.conf.

------
**Note:** It is expected that users will configure all the settings in the `SparkConf` at the start and then the Mongo Connector will use those settings as the defaults.

Various methods in the Mongo Connector API will also accept an override for a [`ReadConfig`](../src/main/scala/com/mongodb/spark/config/ReadConfig.scala) or a [`WriteConfig`](src/main/scala/com/mongodb/spark/config/WriteConfig.scala). This is so you can override any values set in the `SparkConf`. (Refer to the source for these methods but in general its where data is loaded or saved to MongoDB). 

In the Spark API there are some methods that accept extra options in the form of a `Map[String, String]`.  For example the `DataFrameReader` or `DataFrameWriter` API's.  Custom `ReadConfig` or `WriteConfig` settings can easily be converted into a `Map` via the `asOptions()` method.

------

## Input Configuration

The following options are available on `SparkConf` object:

Property name                              | Description                                                       | Default value
-------------------------------------------|-------------------------------------------------------------------|--------------------
spark.mongodb.input.uri                    | The connnection string                                            |
spark.mongodb.input.database               | The database name to read data from                               |
spark.mongodb.input.collection             | The collection name to read data from                             |
spark.mongodb.input.localThreshold         | The threshold for choosing a server from multiple MongoDB servers | 15 ms
spark.mongodb.input.readPreference.name    | The name of the `ReadPreference` to use                           | Primary
spark.mongodb.input.readPreference.tagSets | The `ReadPreference` TagSets to use                               |
spark.mongodb.input.readConcern.level      | The `ReadConcern` level to use                                    |
spark.mongodb.input.sampleSize             | The sample size to use when inferring the schema                  | 1000
spark.mongodb.input.splitKey               | The partition key to split the data                               | `_id`
spark.mongodb.input.maxChunkSize           | The maximum chunk size for partitioning an unsharded collection   | 64 MB

-----
**Note**: When passing input configurations via an options Map then the prefix `spark.mongodb.input.` is not needed.

-----

## Output Configuration

The following options are available on `SparkConf` object:

Property name                                | Description                                                       | Default value
---------------------------------------------|-------------------------------------------------------------------|--------------------
spark.mongodb.output.uri                     | The connnection string                                            |
spark.mongodb.output.database                | The database name to write data to                                |
spark.mongodb.output.collection              | The collection name to write data to                              |
spark.mongodb.input.localThreshold           | The threshold for choosing a server from multiple MongoDB servers | 15 ms
spark.mongodb.output.writeConcern.w          | The write concern w value                                         | (WriteConcern.ACKNOWLEDGED)
spark.mongodb.output.writeConcern.journal    | The write concern journal value                                   |
spark.mongodb.output.writeConcern.wTimeoutMS | The write concern wTimeout value                                  |

-----
**Note**: When passing output configurations via an options Map then the prefix `spark.mongodb.output.` is not needed.

-----

## Configuring via the uri

These configurations are deliberately kept simple all that is required is the uri, the database and collection and name to connect.
Any configurations via the uri will override any default values, so the uri can be used to configure the ReadPreference or the WriteConcern.

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

## Configuration via system properties

The MongoConnector includes a cache for MongoClients, so workers can share the MongoClient across threads. As the cache is setup before the
Spark Configuration is available it can only be configured via a System Property:

System Property name         | Description                                                     | Default value
-----------------------------|-----------------------------------------------------------------|--------------------
spark.mongodb.keep_alive_ms  | The length of time to keep a MongoClient available for sharing  | 5000
