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

import com.mongodb.spark.notNull
import com.mongodb.{ReadConcern, ReadPreference}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

import scala.collection.JavaConverters._

/**
 * The `ReadConfig` companion object
 *
 * $inputProperties
 *
 * @since 1.0
 */
object ReadConfig extends MongoInputConfig {

  type Self = ReadConfig

  private val DefaultSampleSize: Int = 1000
  private val DefaultMaxChunkSize = 64 // 64 MB
  private val DefaultSplitKey = "_id"

  override def apply(options: collection.Map[String, String], default: Option[ReadConfig]): ReadConfig = {
    val cleanedOptions = prefixLessOptions(options)
    ReadConfig(
      databaseName = databaseName(databaseNameProperty, cleanedOptions, default.map(conf => conf.databaseName)),
      collectionName = collectionName(collectionNameProperty, cleanedOptions, default.map(conf => conf.collectionName)),
      connectionString = cleanedOptions.get(mongoURIProperty).orElse(default.flatMap(conf => conf.connectionString)),
      sampleSize = getInt(cleanedOptions.get(sampleSizeProperty), default.map(conf => conf.sampleSize), DefaultSampleSize),
      maxChunkSize = getInt(cleanedOptions.get(maxChunkSizeProperty), default.map(conf => conf.maxChunkSize), DefaultMaxChunkSize),
      splitKey = getString(cleanedOptions.get(splitKeyProperty), default.map(conf => conf.splitKey), DefaultSplitKey),
      localThreshold = getInt(cleanedOptions.get(localThresholdProperty), default.map(conf => conf.localThreshold),
        MongoSharedConfig.DefaultLocalThreshold),
      readPreferenceConfig = ReadPreferenceConfig(cleanedOptions, default.map(conf => conf.readPreferenceConfig)),
      readConcernConfig = ReadConcernConfig(cleanedOptions, default.map(conf => conf.readConcernConfig))
    )
  }

  // scalastyle:off parameter.number
  /**
   * Read Configuration used when reading data from MongoDB
   *
   * @param databaseName the database name
   * @param collectionName the collection name
   * @param connectionString the optional connection string used in the creation of this configuration
   * @param sampleSize a positive integer sample size to draw from the collection when inferring the schema
   * @param maxChunkSize   the maximum chunkSize for non-sharded collections
   * @param splitKey the key to split the collection by for non-sharded collections or the "shard key" for sharded collection
   * @param localThreshold the local threshold in milliseconds used when choosing among multiple MongoDB servers to send a request.
   *                       Only servers whose ping time is less than or equal to the server with the fastest ping time plus the local
   *                       threshold will be chosen.
   * @param readPreference the readPreference configuration
   * @param readConcern the readConcern configuration
   *
   * @since 1.0
   */
  def create(databaseName: String, collectionName: String, connectionString: String, sampleSize: Int, maxChunkSize: Int, splitKey: String,
             localThreshold: Int, readPreference: ReadPreference, readConcern: ReadConcern): ReadConfig = {
    notNull("databaseName", databaseName)
    notNull("collectionName", collectionName)
    notNull("splitKey", splitKey)
    notNull("readPreference", readPreference)
    notNull("readConcern", readConcern)

    new ReadConfig(databaseName, collectionName, Option(connectionString), sampleSize, maxChunkSize, splitKey, localThreshold,
      ReadPreferenceConfig.apply(readPreference), ReadConcernConfig.apply(readConcern))
  }
  // scalastyle:on parameter.number

  override def create(javaSparkContext: JavaSparkContext): ReadConfig = {
    notNull("javaSparkContext", javaSparkContext)
    apply(javaSparkContext.getConf)
  }

  override def create(sparkConf: SparkConf): ReadConfig = {
    notNull("sparkConf", sparkConf)
    apply(sparkConf)
  }

  override def create(options: util.Map[String, String]): ReadConfig = {
    notNull("options", options)
    apply(options.asScala)
  }

  override def create(options: util.Map[String, String], default: ReadConfig): ReadConfig = {
    notNull("options", options)
    notNull("default", default)
    apply(options.asScala, Option(default))
  }

}

/**
 * Read Configuration used when reading data from MongoDB
 *
 * @param databaseName the database name
 * @param collectionName the collection name
 * @param connectionString the optional connection string used in the creation of this configuration
 * @param sampleSize a positive integer sample size to draw from the collection when inferring the schema
 * @param maxChunkSize   the maximum chunkSize for non-sharded collections
 * @param splitKey the key to split the collection by for non-sharded collections or the "shard key" for sharded collection
 * @param localThreshold the local threshold in milliseconds used when choosing among multiple MongoDB servers to send a request.
 *                       Only servers whose ping time is less than or equal to the server with the fastest ping time plus the local
 *                       threshold will be chosen.
 * @param readPreferenceConfig the readPreference configuration
 * @param readConcernConfig the readConcern configuration
 *
 * @since 1.0
 */
case class ReadConfig(
    databaseName:         String,
    collectionName:       String,
    connectionString:     Option[String]       = None,
    sampleSize:           Int                  = ReadConfig.DefaultSampleSize,
    maxChunkSize:         Int                  = ReadConfig.DefaultMaxChunkSize,
    splitKey:             String               = ReadConfig.DefaultSplitKey,
    localThreshold:       Int                  = MongoSharedConfig.DefaultLocalThreshold,
    readPreferenceConfig: ReadPreferenceConfig = ReadPreferenceConfig(),
    readConcernConfig:    ReadConcernConfig    = ReadConcernConfig()
) extends MongoCollectionConfig with MongoClassConfig {
  require(sampleSize > 0, s"sampleSize ($sampleSize) must be greater than 0")
  require(localThreshold >= 0, s"localThreshold ($localThreshold) must be greater or equal to 0")

  type Self = ReadConfig

  override def withOptions(options: collection.Map[String, String]): ReadConfig = ReadConfig(options, Some(this))

  override def asOptions: collection.Map[String, String] = {
    val options = Map(
      ReadConfig.databaseNameProperty -> databaseName,
      ReadConfig.collectionNameProperty -> collectionName,
      ReadConfig.sampleSizeProperty -> sampleSize.toString,
      ReadConfig.maxChunkSizeProperty -> maxChunkSize.toString,
      ReadConfig.splitKeyProperty -> splitKey,
      ReadConfig.localThresholdProperty -> localThreshold.toString
    ) ++ readPreferenceConfig.asOptions ++ readConcernConfig.asOptions

    connectionString match {
      case Some(uri) => options + (ReadConfig.mongoURIProperty -> uri)
      case None      => options
    }
  }

  override def withJavaOptions(options: util.Map[String, String]): ReadConfig = withOptions(options.asScala)

  override def asJavaOptions: util.Map[String, String] = asOptions.asJava

  /**
   * The `ReadPreference` to use
   *
   * @return the ReadPreference
   */
  def readPreference: ReadPreference = readPreferenceConfig.readPreference

  /**
   * The `ReadConcern` to use
   *
   * @return the ReadConcern
   */
  def readConcern: ReadConcern = readConcernConfig.readConcern
}
