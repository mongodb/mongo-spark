# Mongo Spark Connector Changelog

## 2.0.0

  * [[SPARK-47](https://jira.mongodb.org/browse/SPARK-47)] Updated API to use SparkSession and deprecated public methods using SQLContext.
  * [[SPARK-20](https://jira.mongodb.org/browse/SPARK-20)] Updated Spark Version to 2.0.0

------

## 0.4
  * [[SPARK-49](https://jira.mongodb.org/browse/SPARK-49)] Marked internal public code with DeveloperApi annotation.
  * [[SPARK-60](https://jira.mongodb.org/browse/SPARK-60)] Added partitioner to ReadConfig and added custom partitioner options.
  * [[SPARK-54](https://jira.mongodb.org/browse/SPARK-54)] Added a sample and pagination based partitioners.
  * [[SPARK-53](https://jira.mongodb.org/browse/SPARK-53)] Updated DefaultMongoPartitioner implementation.
  * [[SPARK-51](https://jira.mongodb.org/browse/SPARK-51)] Documented partition permissions required.
  * [[SPARK-61](https://jira.mongodb.org/browse/SPARK-61)] Ensure that MongoSpark builder applies overridden options correctly.

## 0.3
  * [[SPARK-58](https://jira.mongodb.org/browse/SPARK-58)] Added the ability to explicitly pass schema when creating a DataFrame.
  * [[SPARK-59](https://jira.mongodb.org/browse/SPARK-59)] Fixed being able to directly connect to a single MongoD in a replicaSet.
  * [[SPARK-56](https://jira.mongodb.org/browse/SPARK-56)] Moved MongoSpark into the Scala API as the main gateway for configuring the connector
    Removed the now redundant `com.mongodb.spark.api.java` namespace.
  * Added abstract class `Logging` so that implementations can be extended easily in Java.
  * [[SPARK-55](https://jira.mongodb.org/browse/SPARK-55)] Made Paritioners public.
  * [[SPARK-52](https://jira.mongodb.org/browse/SPARK-52)] MongoConnector is accessible from the new MongoSpark class or directly. 
         Added Java specific methods for withMongoClient, withMongoDatabase and withMongoCollection.
  * [[SPARK-50](https://jira.mongodb.org/browse/SPARK-50)] Made MongoPartition public added tests for custom partitioners.
  * [[SPARK-45](https://jira.mongodb.org/browse/SPARK-45)] Ensure that the SQLContext is reused correctly.

## 0.2
  * [[SPARK-43](https://jira.mongodb.org/browse/SPARK-43)] Ensure that Bson Types are preserved when round tripping dataframes 
    * Closed the type system to map `BsonValue` to Spark `DataTypes`
    * Created Case Classes and Java Beans representing unsupported Spark `DataTypes`
  * [[SPARK-42](https://jira.mongodb.org/browse/SPARK-42)] Allow Dataframe readers and writers be configurable just from options 
  * [[SPARK-41](https://jira.mongodb.org/browse/SPARK-41)] Don't assume ObjectId's when sampling data 
  * [[SPARK-40](https://jira.mongodb.org/browse/SPARK-40)] Fixed Schema inference on arrays with nested structs 
  * [[SPARK-38](https://jira.mongodb.org/browse/SPARK-38)] Fixed DataFrame NPE issue handling null data 
  * [[SPARK-37](https://jira.mongodb.org/browse/SPARK-37)] Fixed conversion to Numeric types after an RDD had been cached 
  * [[SPARK-36](https://jira.mongodb.org/browse/SPARK-36)] Fixed race condition closing MongoClients in RDDs 
  * [[SPARK-33](https://jira.mongodb.org/browse/SPARK-33)] Fixed schema inference when saving a DataFrame  
  * [[SPARK-32](https://jira.mongodb.org/browse/SPARK-32)] Validated the URI in the Spark Configs.  
  * Reorganised the connection string arg position in WriteConfig to match ReadConfig

## 0.1 - Initial release
