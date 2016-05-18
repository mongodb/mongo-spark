# Mongo Spark Connector Changelog

## 0.3-SNAPSHOT
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
