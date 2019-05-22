/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.spark.config

/**
 * Mongo input configurations
 *
 * Configurations used when reading from MongoDB
 *
 * $inputProperties
 *
 * @see [[com.mongodb.spark.config.ReadConfig$]]
 * @since 1.0
 *
 * @define inputProperties
 *
 * == Configuration Properties ==
 *
 * The prefix when using `sparkConf` is: `spark.mongodb.input.` followed by the property name:
 *
 *  - [[databaseNameProperty database]], the database name to read data from.
 *  - [[collectionNameProperty collection]], the collection name to read data from.
 *  - [[readPreferenceNameProperty readPreference.name]], the name of the `ReadPreference` to use.
 *  - [[readPreferenceTagSetsProperty readPreference.tagSets]], the `ReadPreference` TagSets to use.
 *  - [[readConcernLevelProperty readConcern.level]], the `ReadConcern` level to use.
 *  - [[sampleSizeProperty sampleSize]], the sample size to use when inferring the schema.
 *  - [[samplePoolSizeProperty sampleSize]], the size pool to sample from when inferring the schema.
 *  - [[partitionerProperty partitioner]], the name of the partitioner to use to partition the data.
 *  - [[partitionerOptionsProperty partitionerOptions]], the custom options used to configure the partitioner.
 *  - [[localThresholdProperty localThreshold]], the number of milliseconds used when choosing among multiple MongoDB servers to send a request.
 *  - [[registerSQLHelperFunctionsProperty registerSQLHelperFunctions]], register SQL helper functions allow easy querying of Bson types inside SQL queries.
 *  - [[inferSchemaMapTypeEnabledProperty sql.inferschema.mapTypes.enabled]], enable schema inference of MapTypes.
 *  - [[inferSchemaMapTypeMinimumKeysProperty sql.inferschema.mapTypes.minimumKeys]], the minimum number of keys of how large a struct must be before a MapType
 *    should be inferred.
 *  - [[pipelineIncludeNullFiltersProperty sql.pipeline.includeNullFilters]], include null filters in the aggregation pipeline
 *  - [[pipelineIncludeFiltersAndProjectionsProperty sql.pipeline.includeFiltersAndProjections]], include any filters and projections in
 *    the aggregation pipeline
 *  - [[pipelineProperty pipeline]], enables custom aggregation pipelines to applied to the collection before sending to Spark
 *  - [[allowDiskUseProperty allowDiskUse]], enables writing to temporary files during aggregation in MongoDB.
 *  - [[batchSizeProperty batchSize]], customize the size of the internal batches within the MongoDB cursor.
 */
trait MongoInputConfig extends MongoCompanionConfig {

  override val configPrefix: String = "spark.mongodb.input."

  /**
   * The database name property
   */
  val databaseNameProperty: String = "database"

  /**
   * The collection name property
   */
  val collectionNameProperty: String = "collection"

  /**
   * The `ReadPreference` name property
   *
   * Default: `primary`
   * @see [[ReadPreferenceConfig]]
   */
  val readPreferenceNameProperty: String = "readPreference.name".toLowerCase

  /**
   * The `ReadPreference` tags property
   *
   * @see [[ReadPreferenceConfig]]
   */
  val readPreferenceTagSetsProperty: String = "readPreference.tagSets".toLowerCase

  /**
   * The `ReadConcern` level property
   *
   * Default: `DEFAULT`
   * @see [[ReadConcernConfig]]
   */
  val readConcernLevelProperty: String = "readConcern.level".toLowerCase

  /**
   * The sample size property
   *
   * Used when sampling data from MongoDB to determine the Schema. Should be equal or less than the sample pool size.
   * Default: `1000`
   */
  val sampleSizeProperty: String = "sampleSize".toLowerCase

  /**
   * The sample pool size property
   *
   * The size of the pool to take a sample from, used when there is no `\$sample` support or if there is a pushed down aggregation. Can be
   * used to significantly reduce the costs of inferring the schema. A negative value disables limiting when using `\$sample` and
   * will sample from the whole collection.
   *
   * Default: `10000`
   *
   * @since 2.3.1
   */
  val samplePoolSizeProperty: String = "samplePoolSize".toLowerCase

  /**
   * The partition property
   *
   * Represents the name of the partitioner to use when partitioning the data in the collection.
   * Default: `MongoDefaultPartitioner`
   */
  val partitionerProperty: String = "partitioner".toLowerCase

  /**
   * The partitioner options property
   *
   * Represents a map of options for customising the configuration of a partitioner.
   * Default: `Map.empty[String, String]`
   */
  val partitionerOptionsProperty: String = "partitionerOptions".toLowerCase

  /**
   * The localThreshold property
   *
   * The local threshold in milliseconds is used when choosing among multiple MongoDB servers to send a request.
   * Only servers whose ping time is less than or equal to the server with the fastest ping time *plus* the local threshold will be chosen.
   *
   * For example when choosing which MongoS to send a request through a `localThreshold` of 0 would pick the MongoS with the fastest ping time.
   *
   * Default: `15 ms`
   */
  val localThresholdProperty: String = MongoSharedConfig.localThresholdProperty

  /**
   * Register SQL Helper functions
   *
   * The SQL helper functions allow easy querying of Bson types inside SQL queries
   *
   * @since 1.1
   */
  val registerSQLHelperFunctionsProperty: String = "registerSQLHelperFunctions".toLowerCase()
  val registerSQLHelperFunctions: String = registerSQLHelperFunctionsProperty

  /**
   * The infer schema MapType enabled property
   *
   * A boolean flag to enable or disable MapType infer.
   * If this flag is enabled, large compatible struct types will be inferred to a MapType instead.
   *
   * Default: `true`
   * @since 2.3
   */
  val inferSchemaMapTypeEnabledProperty: String = "sql.inferSchema.mapTypes.enabled".toLowerCase

  /**
   * The infer schema MapType minimum keys property
   *
   * The minimum keys property controls how large a struct must be before a MapType should be inferred.
   *
   * Default: `250`
   * @since 2.3
   */
  val inferSchemaMapTypeMinimumKeysProperty: String = "sql.inferSchema.mapTypes.minimumKeys".toLowerCase

  /**
   * The sql include null filters in the pipeline property
   *
   * A boolean flag to enable or disable pushing null value checks into MongoDB when using spark sql.
   * These ensure that the value exists and is not null for each not nullable field.
   *
   * Default: `true`
   * @since 2.3
   */
  val pipelineIncludeNullFiltersProperty: String = "sql.pipeline.includeNullFilters".toLowerCase

  /**
   * The sql include pipeline filters and projections property
   *
   * A boolean flag to enable or disable pushing down filters and projections into MongoDB when using spark sql.
   * A `false` value will be expensive as all data will be sent to spark and filtered in Spark.
   *
   * Default: `true`
   * @since 2.3
   */
  val pipelineIncludeFiltersAndProjectionsProperty: String = "sql.pipeline.includeFiltersAndProjections".toLowerCase

  /**
   * The collation property
   *
   * The json representation of a Collation. Created via `Collation.asDocument.toJson`.
   *
   * @since 2.3
   */
  val collationProperty: String = "collation".toLowerCase

  /**
   * The hint property
   *
   * The json representation of a hint document
   *
   * @since 2.3
   */
  val hintProperty: String = "hint".toLowerCase

  /**
   * The pipeline property
   *
   * Enables custom aggregation pipelines to applied to the collection before sending to Spark.
   * When configuring this should either be an extended json representation of a list of documents:
   *
   * ```"""[{"\$match": {"closed": false}}, {"\$project": {"status": 1, "name": 1, "description": 1}}]"""```
   *
   * Or the extended json syntax of a single document:
   *
   * ```"""{"\$match": {"closed": false}}"""```
   *
   * '''Note:''' Custom aggregation pipelines must work with the partitioner strategy. Some stages such as `\$group` may not work as expected.
   *
   * @since 2.3.1
   */
  val pipelineProperty: String = "pipeline".toLowerCase

  /**
   * The allow disk use property
   *
   * Enables writing to temporary files
   *
   * @since 2.3.1
   */
  val allowDiskUseProperty: String = "allowDiskUse".toLowerCase

  /**
   * The batch size property
   *
   * The size of batches used by the underlying cursor. Smaller batches will result in more round trips to MongoDB.
   *
   * Default: The servers default
   *
   * @since 2.4.1
   */
  val batchSizeProperty: String = "batchSize".toLowerCase
}
