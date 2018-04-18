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

import java.util

import scala.collection.JavaConverters._
import scala.util.Try
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import com.mongodb.{ConnectionString, WriteConcern}
import com.mongodb.spark.notNull
import org.bson.BsonDocument

import scala.collection.mutable

/**
 * The `WriteConfig` companion object
 *
 * @since 1.0
 */
object WriteConfig extends MongoOutputConfig {

  type Self = WriteConfig

  private val DefaultReplaceDocument: Boolean = true
  private val DefaultMaxBatchSize: Int = 512

  /**
   * Creates a WriteConfig
   *
   * @param databaseName the database name
   * @param collectionName the collection name
   * @param localThreshold the local threshold in milliseconds used when choosing among multiple MongoDB servers to send a request.
   *                       Only servers whose ping time is less than or equal to the server with the fastest ping time plus the local
   *                       threshold will be chosen.
   * @param writeConcern the WriteConcern to use
   * @return the write config
   */
  def apply(databaseName: String, collectionName: String, localThreshold: Int, writeConcern: WriteConcern): WriteConfig =
    WriteConfig(databaseName, collectionName, None, DefaultReplaceDocument, DefaultMaxBatchSize, localThreshold,
      WriteConcernConfig(writeConcern))

  /**
   * Creates a WriteConfig
   *
   * @param databaseName the database name
   * @param collectionName the collection name
   * @param connectionString the connection string used in the creation of this configuration
   * @param localThreshold the local threshold in milliseconds used when choosing among multiple MongoDB servers to send a request.
   *                       Only servers whose ping time is less than or equal to the server with the fastest ping time plus the local
   *                       threshold will be chosen.
   * @param writeConcern the WriteConcern to use
   * @return the write config
   */
  def apply(databaseName: String, collectionName: String, connectionString: String, localThreshold: Int, writeConcern: WriteConcern): WriteConfig =
    apply(databaseName, collectionName, Some(connectionString), DefaultReplaceDocument, DefaultMaxBatchSize,
      localThreshold, WriteConcernConfig(writeConcern))

  /**
   * Creates a WriteConfig
   *
   * @param databaseName the database name
   * @param collectionName the collection name
   * @param connectionString the optional connection string used in the creation of this configuration
   * @param localThreshold the local threshold in milliseconds used when choosing among multiple MongoDB servers to send a request.
   *                       Only servers whose ping time is less than or equal to the server with the fastest ping time plus the local
   *                       threshold will be chosen.
   * @param writeConcern the WriteConcern to use
   * @return the write config
   * @since 2.1
   */
  def apply(databaseName: String, collectionName: String, connectionString: Option[String], localThreshold: Int, writeConcern: WriteConcern): WriteConfig =
    apply(databaseName, collectionName, connectionString, DefaultReplaceDocument, DefaultMaxBatchSize, localThreshold,
      WriteConcernConfig(writeConcern))

  /**
   * Creates a WriteConfig
   *
   * @param databaseName the database name
   * @param collectionName the collection name
   * @param connectionString the optional connection string used in the creation of this configuration
   * @param replaceDocument replaces the whole document, when saving a Dataset that contains an `_id` field.
   *                        If false only updates / sets the fields declared in the Dataset.
   * @param localThreshold the local threshold in milliseconds used when choosing among multiple MongoDB servers to send a request.
   *                       Only servers whose ping time is less than or equal to the server with the fastest ping time plus the local
   *                       threshold will be chosen.
   * @param writeConcern the WriteConcern to use
   * @return the write config
   * @since 2.1
   */
  def apply(databaseName: String, collectionName: String, connectionString: Option[String], replaceDocument: Boolean, localThreshold: Int,
            writeConcern: WriteConcern): WriteConfig = {
    apply(databaseName, collectionName, connectionString, replaceDocument, DefaultMaxBatchSize, localThreshold, writeConcern)
  }

  /**
   * Creates a WriteConfig
   *
   * @param databaseName the database name
   * @param collectionName the collection name
   * @param connectionString the optional connection string used in the creation of this configuration
   * @param replaceDocument replaces the whole document, when saving a Dataset that contains an `_id` field.
   *                        If false only updates / sets the fields declared in the Dataset.
   * @param maxBatchSize the maxBatchSize when performing a bulk update/insert. Defaults to 512.
   * @param localThreshold the local threshold in milliseconds used when choosing among multiple MongoDB servers to send a request.
   *                       Only servers whose ping time is less than or equal to the server with the fastest ping time plus the local
   *                       threshold will be chosen.
   * @param writeConcern the WriteConcern to use
   * @return the write config
   * @since 2.1
   */
  def apply(databaseName: String, collectionName: String, connectionString: Option[String], replaceDocument: Boolean, maxBatchSize: Int,
            localThreshold: Int, writeConcern: WriteConcern): WriteConfig = {
    apply(databaseName, collectionName, connectionString, replaceDocument, maxBatchSize, localThreshold, WriteConcernConfig(writeConcern))
  }

  /**
   * Creates a WriteConfig
   *
   * @param databaseName     the database name
   * @param collectionName   the collection name
   * @param connectionString the optional connection string used in the creation of this configuration
   * @param replaceDocument  replaces the whole document, when saving a Dataset that contains an `_id` field.
   *                         If false only updates / sets the fields declared in the Dataset.
   * @param maxBatchSize     the maxBatchSize when performing a bulk update/insert. Defaults to 512.
   * @param localThreshold   the local threshold in milliseconds used when choosing among multiple MongoDB servers to send a request.
   *                         Only servers whose ping time is less than or equal to the server with the fastest ping time plus the local
   *                         threshold will be chosen.
   * @param writeConcern     the WriteConcern to use
   * @param shardKey         an optional shardKey in extended form: `"{key: 1, key2: 1}"`. Used when upserting DataSets in sharded clusters.
   * @return the write config
   * @since 2.2.2
   */
  def apply(databaseName: String, collectionName: String, connectionString: Option[String], replaceDocument: Boolean, maxBatchSize: Int,
            localThreshold: Int, writeConcern: WriteConcern, shardKey: Option[String]): WriteConfig = {
    apply(databaseName, collectionName, connectionString, replaceDocument, maxBatchSize, localThreshold, WriteConcernConfig(writeConcern),
      shardKey)
  }

  override def apply(options: collection.Map[String, String], default: Option[WriteConfig]): WriteConfig = {
    val cleanedOptions = stripPrefix(options)
    val cachedConnectionString = connectionString(cleanedOptions)
    val defaultDatabase = default.map(conf => conf.databaseName).orElse(Option(cachedConnectionString.getDatabase))
    val defaultCollection = default.map(conf => conf.collectionName).orElse(Option(cachedConnectionString.getCollection))

    WriteConfig(
      databaseName = databaseName(databaseNameProperty, cleanedOptions, defaultDatabase),
      collectionName = collectionName(collectionNameProperty, cleanedOptions, defaultCollection),
      connectionString = cleanedOptions.get(mongoURIProperty).orElse(default.flatMap(conf => conf.connectionString)),
      replaceDocument = getBoolean(cleanedOptions.get(replaceDocumentProperty), default.map(conf => conf.replaceDocument),
        defaultValue = true),
      maxBatchSize = getInt(cleanedOptions.get(maxBatchSizeProperty), default.map(conf => conf.maxBatchSize),
        DefaultMaxBatchSize),
      localThreshold = getInt(cleanedOptions.get(localThresholdProperty), default.map(conf => conf.localThreshold),
        MongoSharedConfig.DefaultLocalThreshold),
      writeConcernConfig = WriteConcernConfig(cleanedOptions, default.map(writeConf => writeConf.writeConcernConfig)),
      shardKey = cleanedOptions.get(shardKeyProperty).orElse(default.flatMap(conf => conf.shardKey).orElse(None))
    )
  }

  /**
   * Creates a WriteConfig
   *
   * @param databaseName the database name
   * @param collectionName the collection name
   * @param connectionString the optional connection string used in the creation of this configuration
   * @param localThreshold the local threshold in milliseconds used when choosing among multiple MongoDB servers to send a request.
   *                       Only servers whose ping time is less than or equal to the server with the fastest ping time plus the local
   *                       threshold will be chosen.
   * @param writeConcern the WriteConcern to use
   * @return the write config
   */
  def create(databaseName: String, collectionName: String, connectionString: String, localThreshold: Int, writeConcern: WriteConcern): WriteConfig = {
    create(databaseName, collectionName, connectionString, DefaultReplaceDocument, DefaultMaxBatchSize, localThreshold, writeConcern)
  }

  /**
   * Creates a WriteConfig
   *
   * @param databaseName the database name
   * @param collectionName the collection name
   * @param connectionString the optional connection string used in the creation of this configuration
   * @param replaceDocument replaces the whole document, when saving a Dataset that contains an `_id` field.
   *                        If false only updates / sets the fields declared in the Dataset.
   * @param localThreshold the local threshold in milliseconds used when choosing among multiple MongoDB servers to send a request.
   *                       Only servers whose ping time is less than or equal to the server with the fastest ping time plus the local
   *                       threshold will be chosen.
   * @param writeConcern the WriteConcern to use
   * @return the write config
   * @since 2.1
   */
  def create(databaseName: String, collectionName: String, connectionString: String, replaceDocument: Boolean, localThreshold: Int,
             writeConcern: WriteConcern): WriteConfig = {
    create(databaseName, collectionName, connectionString, replaceDocument, DefaultMaxBatchSize, localThreshold, writeConcern)
  }

  /**
   * Creates a WriteConfig
   *
   * @param databaseName the database name
   * @param collectionName the collection name
   * @param connectionString the optional connection string used in the creation of this configuration
   * @param replaceDocument replaces the whole document, when saving a Dataset that contains an `_id` field.
   *                        If false only updates / sets the fields declared in the Dataset.
   * @param maxBatchSize the maxBatchSize when performing a bulk update/insert. Defaults to 512.
   * @param localThreshold the local threshold in milliseconds used when choosing among multiple MongoDB servers to send a request.
   *                       Only servers whose ping time is less than or equal to the server with the fastest ping time plus the local
   *                       threshold will be chosen.
   * @param writeConcern the WriteConcern to use
   * @return the write config
   * @since 2.1
   */
  def create(databaseName: String, collectionName: String, connectionString: String, replaceDocument: Boolean, maxBatchSize: Int,
             localThreshold: Int, writeConcern: WriteConcern): WriteConfig = {
    notNull("databaseName", databaseName)
    notNull("collectionName", collectionName)
    notNull("replaceDocument", replaceDocument)
    notNull("maxBatchSize", maxBatchSize)
    notNull("localThreshold", localThreshold)
    notNull("writeConcern", writeConcern)
    new WriteConfig(databaseName, collectionName, Option(connectionString), replaceDocument, maxBatchSize, localThreshold,
      WriteConcernConfig(writeConcern))
  }

  /**
   * Creates a WriteConfig
   *
   * @param databaseName     the database name
   * @param collectionName   the collection name
   * @param connectionString the optional connection string used in the creation of this configuration
   * @param replaceDocument  replaces the whole document, when saving a Dataset that contains an `_id` field.
   *                         If false only updates / sets the fields declared in the Dataset.
   * @param maxBatchSize     the maxBatchSize when performing a bulk update/insert. Defaults to 512.
   * @param localThreshold   the local threshold in milliseconds used when choosing among multiple MongoDB servers to send a request.
   *                         Only servers whose ping time is less than or equal to the server with the fastest ping time plus the local
   *                         threshold will be chosen.
   * @param writeConcern     the WriteConcern to use
   * @param shardKey         an optional shardKey in extended form: `"{key: 1, key2: 1}"`. Used when upserting DataSets in sharded clusters.
   * @return the write config
   * @since 2.2.2
   */
  def create(databaseName: String, collectionName: String, connectionString: String, replaceDocument: Boolean, maxBatchSize: Int,
             localThreshold: Int, writeConcern: WriteConcern, shardKey: String): WriteConfig = {
    notNull("databaseName", databaseName)
    notNull("collectionName", collectionName)
    notNull("replaceDocument", replaceDocument)
    notNull("maxBatchSize", maxBatchSize)
    notNull("localThreshold", localThreshold)
    notNull("writeConcern", writeConcern)
    notNull("shardKey", shardKey)
    new WriteConfig(databaseName, collectionName, Option(connectionString), replaceDocument, maxBatchSize, localThreshold,
      WriteConcernConfig(writeConcern), Option(shardKey))
  }

  override def create(javaSparkContext: JavaSparkContext): WriteConfig = {
    notNull("javaSparkContext", javaSparkContext)
    apply(javaSparkContext.getConf)
  }

  override def create(sparkConf: SparkConf): WriteConfig = {
    notNull("sparkConf", sparkConf)
    apply(sparkConf)
  }

  override def create(options: util.Map[String, String]): WriteConfig = {
    notNull("options", options)
    apply(options.asScala)
  }

  override def create(options: util.Map[String, String], default: WriteConfig): WriteConfig = {
    notNull("options", options)
    notNull("default", default)
    apply(options.asScala, Some(default))
  }

  override def create(sparkConf: SparkConf, options: util.Map[String, String]): WriteConfig = {
    notNull("sparkConf", sparkConf)
    notNull("options", options)
    apply(sparkConf, options.asScala)
  }

  override def create(sqlContext: SQLContext): WriteConfig = {
    notNull("sqlContext", sqlContext)
    create(sqlContext.sparkSession)
  }

  override def create(sparkSession: SparkSession): WriteConfig = {
    notNull("sparkSession", sparkSession)
    apply(sparkSession)
  }

}

/**
 * Write Configuration for writes to MongoDB
 *
 * @param databaseName       the database name
 * @param collectionName     the collection name
 * @param connectionString   the optional connection string used in the creation of this configuration.
 * @param replaceDocument    replaces the whole document, when saving a Dataset that contains an `_id` field.
 *                           If false only updates / sets the fields declared in the Dataset.
 * @param maxBatchSize       the maxBatchSize when performing a bulk update/insert. Defaults to 512.
 * @param localThreshold     the local threshold in milliseconds used when choosing among multiple MongoDB servers to send a request.
 *                           Only servers whose ping time is less than or equal to the server with the fastest ping time plus the local
 *                           threshold will be chosen.
 * @param writeConcernConfig the write concern configuration
 * @param shardKey           an optional shardKey in extended form: `"{key: 1, key2: 1}"`. Used when upserting DataSets in sharded clusters.
 * @since 1.0
 */
case class WriteConfig(
    databaseName:       String,
    collectionName:     String,
    connectionString:   Option[String]     = None,
    replaceDocument:    Boolean            = WriteConfig.DefaultReplaceDocument,
    maxBatchSize:       Int                = WriteConfig.DefaultMaxBatchSize,
    localThreshold:     Int                = MongoSharedConfig.DefaultLocalThreshold,
    writeConcernConfig: WriteConcernConfig = WriteConcernConfig.Default,
    shardKey:           Option[String]     = None
) extends MongoCollectionConfig with MongoClassConfig {
  require(maxBatchSize >= 1, s"maxBatchSize ($maxBatchSize) must be greater or equal to 1")
  require(localThreshold >= 0, s"localThreshold ($localThreshold) must be greater or equal to 0")
  require(Try(connectionString.map(uri => new ConnectionString(uri))).isSuccess, s"Invalid uri: '${connectionString.get}'")
  require(Try(shardKey.map(json => BsonDocument.parse(json))).isSuccess, s"Invalid shardKey: '${shardKey.get}'")

  type Self = WriteConfig

  override def withOption(key: String, value: String): WriteConfig = WriteConfig(this.asOptions + (key -> value))

  override def withOptions(options: collection.Map[String, String]): WriteConfig = WriteConfig(options, Some(this))

  override def asOptions: collection.Map[String, String] = {
    val options = mutable.Map("database" -> databaseName, "collection" -> collectionName,
      "replaceDocument" -> replaceDocument.toString,
      "localThreshold" -> localThreshold.toString) ++ writeConcernConfig.asOptions
    connectionString.map(uri => options += (WriteConfig.mongoURIProperty -> uri))
    shardKey.map(json => options += ("shardKey" -> json))
    options.toMap
  }

  override def withOptions(options: util.Map[String, String]): WriteConfig = withOptions(options.asScala)

  override def asJavaOptions: util.Map[String, String] = asOptions.asJava

  /**
   * The `WriteConcern` that this config represents
   *
   * @return the WriteConcern
   */
  def writeConcern: WriteConcern = writeConcernConfig.writeConcern
}
