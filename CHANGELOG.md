# Mongo Spark Connector Changelog

## 2.3.4

  * [[SPARK-265](https://jira.mongodb.org/browse/SPARK-265)] Updated Mongo Java Driver to 3.12.5
  * [[SPARK-271](https://jira.mongodb.org/browse/SPARK-271)] Don't use SPI for the Datasource internally
  * [[SPARK-262](https://jira.mongodb.org/browse/SPARK-262)] Fix BsonOrdering bug for Strings of different lengths

## 2.3.3
  * [[SPARK-235](https://jira.mongodb.org/browse/SPARK-235)] Ensure nullable fields or container types accept null values
  * [[SPARK-233](https://jira.mongodb.org/browse/SPARK-233)] Added ReadConfig.batchSize property
  * [[SPARK-246](https://jira.mongodb.org/browse/SPARK-246)] Renamed system property `spark.mongodb.keep_alive_ms` to `mongodb.keep_alive_ms`
  * [[SPARK-207](https://jira.mongodb.org/browse/SPARK-207)] Added MongoDriverInformation to the default MongoClient
  * [[SPARK-248](https://jira.mongodb.org/browse/SPARK-248)] Update to latest Java driver (3.10.+)
  * [[SPARK-218](https://jira.mongodb.org/browse/SPARK-218)] Update PartitionerHelper.matchQuery - no longer includes $ne/$exists checks
  * [[SPARK-237](https://jira.mongodb.org/browse/SPARK-237)] Added logging of partitioner and their queries
  * [[SPARK-239](https://jira.mongodb.org/browse/SPARK-239)] Added WriteConfig.extendedBsonTypes setting, so users can disable extended bson types when writing.
  * [[SPARK-249](https://jira.mongodb.org/browse/SPARK-249)] Added Java spi can now use short form: `spark.read.format("mongo")`

## 2.3.2
  * [[SPARK-225](https://jira.mongodb.org/browse/SPARK-225)] Ensure WriteConfig.ordered is applied to write operations.
  * [[SPARK-220](https://jira.mongodb.org/browse/SPARK-220)] Fixed MongoSpark.toDF() to use the provided MongoConnector

## 2.3.1
  * Updated Mongo Java Driver to 3.8.2
  * [[SPARK-206](https://jira.mongodb.org/browse/SPARK-206)] Updated Spark dependency to 2.3.2 
  * [[SPARK-210](https://jira.mongodb.org/browse/SPARK-210)] Added ReadConfig.samplePoolSize to improve the performance of inferring schemas
  * [[SPARK-216](https://jira.mongodb.org/browse/SPARK-216)] Updated UDF helpers, don't overwrite JavaScript with no scope and Regex with no options helpers.

## 2.3.0
  * [[SPARK-156](https://jira.mongodb.org/browse/SPARK-156)] Updated Spark dependency to 2.3.0. Dropped Scala 2.10 support.
  * [[SPARK-174](https://jira.mongodb.org/browse/SPARK-174)] Updated Mongo Java Driver to 3.8.0
  * [[SPARK-133](https://jira.mongodb.org/browse/SPARK-133)] Added support for MapType when inferring the schema
  * [[SPARK-186](https://jira.mongodb.org/browse/SPARK-186)] Added configuration to disable auto pipeline manipulation with spark sql
  * [[SPARK-188](https://jira.mongodb.org/browse/SPARK-188)] Removed minKey/maxKey bounds from partitioners.
    Partitioners that produce empty querybounds no longer modify the pipeline.
  * [[SPARK-164](https://jira.mongodb.org/browse/SPARK-164)] Added ordered property to WriteConfig.
  * [[SPARK-192](https://jira.mongodb.org/browse/SPARK-192)] Added WriteConfig.forceInsert property.
    DataFrame overwrites will automatically set force insert to true.
  * [[SPARK-178](https://jira.mongodb.org/browse/SPARK-178)] Log partitioner errors to provide users clearer feedback.
  * [[SPARK-102](https://jira.mongodb.org/browse/SPARK-102)] Added AggregationConfig to configure reads from Mongo.
  * [[SPARK-197](https://jira.mongodb.org/browse/SPARK-197)] Fixed bson compatibility for non nullable struct fields.
  * [[SPARK-199](https://jira.mongodb.org/browse/SPARK-199)] Row to Document optimization.
