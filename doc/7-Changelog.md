# Mongo Spark Connector Changelog

## 0.2
  * Reorganised the connection string arg position in WriteConfig to match ReadConfig
  * Validated the URI in the Spark Configs. [SPARK-32](https://jira.mongodb.org/browse/SPARK-32)
  * Fixed issue when `DataFrameWriter.save` caused extra queries by going on to infer the schema. [SPARK-33](https://jira.mongodb.org/browse/SPARK-33)

## 0.1 - Initial release
