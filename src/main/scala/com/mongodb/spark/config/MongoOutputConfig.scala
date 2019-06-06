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
 * Mongo output configurations
 *
 * Configurations used when writing data from Spark into MongoDB
 *
 * outputProperties
 *
 * @see [[WriteConfig]]
 * @since 1.0
 *
 * @define outputProperties
 *
 * == Configuration Properties ==
 *
 * The prefix when using `sparkConf` is: `spark.mongodb.output.` followed by the property name:
 *
 *  - [[databaseNameProperty databaseName]], the database name to write data to.
 *  - [[collectionNameProperty collectionName]], the collection name to write data to.
 *  - [[writeConcernWProperty writeConcern.w]], the write concern w value.
 *  - [[writeConcernJournalProperty writeConcern.journal]], the write concern journal value.
 *  - [[writeConcernWTimeoutMSProperty writeConcern.wTimeoutMS]], the write concern wTimeout value.
 *  - [[localThresholdProperty localThreshold]], the number of milliseconds used when choosing among multiple MongoDB servers to send a request.
 *
 */
trait MongoOutputConfig extends MongoCompanionConfig {

  override val configPrefix = "spark.mongodb.output."

  /**
   * The database name property
   */
  val databaseNameProperty = "database".toLowerCase

  /**
   * The collection name property
   */
  val collectionNameProperty = "collection".toLowerCase

  /**
   * The `WriteConcern` w property
   *
   * @see [[WriteConcernConfig]]
   */
  val writeConcernWProperty = "writeConcern.w".toLowerCase

  /**
   * The `WriteConcern` journal property
   *
   * @see [[WriteConcernConfig]]
   */
  val writeConcernJournalProperty = "writeConcern.journal".toLowerCase

  /**
   * The `WriteConcern` wTimeoutMS property
   *
   * @see [[WriteConcernConfig]]
   */
  val writeConcernWTimeoutMSProperty = "writeConcern.wTimeoutMS".toLowerCase

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

  /**
   * The replaceDocument property
   *
   * When saving a Dataset with an `_id` field, the whole document will be replaced if true. Otherwise, only the fields in the Dataset will
   * be updated.
   *
   * Default: true
   * @since 2.1
   */
  val replaceDocumentProperty = "replaceDocument".toLowerCase

  /**
   * The maxBatchSize property
   *
   * The maximum size of the batch when performing a bulk update / insert operation.
   *
   * Default: 512
   * @since 2.1
   */
  val maxBatchSizeProperty = "maxBatchSize".toLowerCase

  /**
   * The shardKey property
   *
   * The optional shardKey is required when upserting Datasets in sharded clusters
   *
   * Default: None
   * @since 2.2
   */
  val shardKeyProperty = "shardKey".toLowerCase

  /**
   * The force insert property
   *
   * If `true`, will always use inserts when saving data, even if a `Dataset` contains an `_id` field.
   *
   * Default: None
   * @since 2.2.4
   */
  val forceInsertProperty = "forceInsert".toLowerCase

  /**
   * The ordered property
   *
   * Configures the bulk operation ordered property.
   *
   * Default: true
   * @since 2.3
   */
  val orderedProperty = "ordered"

  /**
   * The extended bson types property
   *
   * The data contains extended bson types and any datasets that contain structs that follow the extended bson types will automatically
   * be converted into native bson types.
   *
   * Default: true
   * @since 2.4.1
   */
  val extendedBsonTypesProperty = "extendedBsonTypes".toLowerCase
}
