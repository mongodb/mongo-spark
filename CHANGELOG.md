# Mongo Spark Connector Changelog

## 2.4.4
  * [[SPARK-321](https://jira.mongodb.org/browse/SPARK-321)] Support modern sharded configurations.
  * [[SPARK-324](https://jira.mongodb.org/browse/SPARK-324)] Added new save API

## 2.4.3
  * [[SPARK-280](https://jira.mongodb.org/browse/SPARK-280)] Support replace and updates from RDD's that include an `_id`.
  Use `forceInsert` to keep existing behaviour.
  * [[SPARK-268](https://jira.mongodb.org/browse/SPARK-268)] Improve Spark numeric type interoperability.

## 2.4.2

  * [[SPARK-265](https://jira.mongodb.org/browse/SPARK-265)] Updated Mongo Java Driver to 3.12.5
  * [[SPARK-271](https://jira.mongodb.org/browse/SPARK-271)] Don't use SPI for the Datasource internally
  * [[SPARK-262](https://jira.mongodb.org/browse/SPARK-262)] Fix BsonOrdering bug for Strings of different lengths

## 2.4.1
  * [[SPARK-235](https://jira.mongodb.org/browse/SPARK-235)] Ensure nullable fields or container types accept null values
  * [[SPARK-233](https://jira.mongodb.org/browse/SPARK-233)] Added ReadConfig.batchSize property
  * [[SPARK-246](https://jira.mongodb.org/browse/SPARK-246)] Renamed system property `spark.mongodb.keep_alive_ms` to `mongodb.keep_alive_ms`
  * [[SPARK-207](https://jira.mongodb.org/browse/SPARK-207)] Added MongoDriverInformation to the default MongoClient
  * [[SPARK-248](https://jira.mongodb.org/browse/SPARK-248)] Update to latest Java driver (3.10.+)
  * [[SPARK-218](https://jira.mongodb.org/browse/SPARK-218)] Update PartitionerHelper.matchQuery - no longer includes $ne/$exists checks
  * [[SPARK-237](https://jira.mongodb.org/browse/SPARK-237)] Added logging of partitioner and their queries
  * [[SPARK-239](https://jira.mongodb.org/browse/SPARK-239)] Added WriteConfig.extendedBsonTypes setting, so users can disable extended bson types when writing.
  * [[SPARK-249](https://jira.mongodb.org/browse/SPARK-249)] Added Java spi can now use short form: `spark.read.format("mongo")`

## 2.4.0
  * [[SPARK-224](https://jira.mongodb.org/browse/SPARK-224)] Support Spark 2.4.0. Updated Spark dependency to 2.4.0
  * [[SPARK-225](https://jira.mongodb.org/browse/SPARK-225)] Ensure WriteConfig.ordered is applied to write operations.
  * [[SPARK-226](https://jira.mongodb.org/browse/SPARK-226)] Updated Mongo Java Driver to 3.9.0
  * [[SPARK-227](https://jira.mongodb.org/browse/SPARK-227)] Added Scala 2.12 support
  * [[SPARK-220](https://jira.mongodb.org/browse/SPARK-220)] Fixed MongoSpark.toDF() to use the provided MongoConnector
