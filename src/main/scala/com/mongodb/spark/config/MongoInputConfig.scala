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
 *  - [[splitKeyProperty splitKey]], the partition key to split the data.
 *  - [[maxChunkSizeProperty maxChunkSize]], the maximum chunk size when partitioning data from an unsharded collection.
 *  - [[localThresholdProperty localThreshold]], the number of milliseconds used when choosing among multiple MongoDB servers to send a request.
 *
 */
trait MongoInputConfig extends MongoCompanionConfig {

  override val configPrefix = "spark.mongodb.input."

  /**
   * The database name property
   */
  val databaseNameProperty = "database"

  /**
   * The collection name property
   */
  val collectionNameProperty = "collection"

  /**
   * The `ReadPreference` name property
   *
   * Default: `ACKNOWLEDGED`
   * @see [[ReadPreferenceConfig]]
   */
  val readPreferenceNameProperty = "readPreference.name".toLowerCase

  /**
   * The `ReadPreference` tags property
   *
   * @see [[ReadPreferenceConfig]]
   */
  val readPreferenceTagSetsProperty = "readPreference.tagSets".toLowerCase

  /**
   * The `ReadConcern` level property
   *
   * @see [[ReadConcernConfig]]
   */
  val readConcernLevelProperty = "readConcern.level".toLowerCase

  /**
   * The sample size property
   *
   * Used when sampling data from MongoDB to determine the Schema.
   * Default: `1000`
   */
  val sampleSizeProperty = "sampleSize".toLowerCase

  /**
   * The split key property
   *
   * Represents the key to be used when partitioning the data in the collection.
   * Default: `_id`
   */
  val splitKeyProperty = "splitKey".toLowerCase

  /**
   * The max chunk size property
   *
   * Represents the max size (in MB) for each partition. Only used when partitioning non-sharded collections.
   * Default: `64`
   */
  val maxChunkSizeProperty = "maxChunkSize".toLowerCase

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
  val localThresholdProperty = MongoSharedConfig.localThresholdProperty

}
