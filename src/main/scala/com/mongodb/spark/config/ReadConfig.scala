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
import org.apache.spark.SparkConf
import com.mongodb.{ReadConcern, ReadPreference}
import org.apache.spark.api.java.JavaSparkContext

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
      sampleSize = getInt(cleanedOptions.get(sampleSizeProperty), default.map(conf => conf.sampleSize), DefaultSampleSize),
      maxChunkSize = getInt(cleanedOptions.get(maxChunkSizeProperty), default.map(conf => conf.maxChunkSize), DefaultMaxChunkSize),
      splitKey = getString(cleanedOptions.get(splitKeyProperty), default.map(conf => conf.splitKey), DefaultSplitKey),
      readPreferenceConfig = ReadPreferenceConfig(cleanedOptions, default.map(conf => conf.readPreferenceConfig)),
      readConcernConfig = ReadConcernConfig(cleanedOptions, default.map(conf => conf.readConcernConfig))
    )
  }

  override def create(javaSparkContext: JavaSparkContext): ReadConfig = apply(javaSparkContext.getConf)

  override def create(sparkConf: SparkConf): ReadConfig = apply(sparkConf)

  override def create(options: util.Map[String, String]): ReadConfig = apply(options.asScala)

  override def create(options: util.Map[String, String], default: ReadConfig): ReadConfig = apply(options.asScala, Option(default))

}

/**
 * Read Configuration used when reading data from MongoDB
 *
 * @param databaseName the database name
 * @param collectionName the collection name
 * @param sampleSize a positive integer sample size to draw from the collection when inferring the schema
 * @param maxChunkSize   the maximum chunkSize for non-sharded collections
 * @param splitKey the key to split the collection by for non-sharded collections or the "shard key" for sharded collection
 * @param readPreferenceConfig the readPreference configuration
 * @param readConcernConfig the readConcern configuration
 *
 * @since 1.0
 */
case class ReadConfig(
    databaseName:         String,
    collectionName:       String,
    sampleSize:           Int                  = ReadConfig.DefaultSampleSize,
    maxChunkSize:         Int                  = ReadConfig.DefaultMaxChunkSize,
    splitKey:             String               = ReadConfig.DefaultSplitKey,
    readPreferenceConfig: ReadPreferenceConfig = ReadPreferenceConfig(),
    readConcernConfig:    ReadConcernConfig    = ReadConcernConfig()
) extends MongoCollectionConfig with MongoSparkConfig {
  require(sampleSize > 0, s"sampleSize ($sampleSize) must be greater than 0")

  type Self = ReadConfig

  override def withOptions(options: collection.Map[String, String]): ReadConfig = ReadConfig(options, Some(this))

  override def asOptions: collection.Map[String, String] = {
    Map(
      ReadConfig.databaseNameProperty -> databaseName,
      ReadConfig.collectionNameProperty -> collectionName,
      ReadConfig.sampleSizeProperty -> sampleSize.toString,
      ReadConfig.maxChunkSizeProperty -> maxChunkSize.toString,
      ReadConfig.splitKeyProperty -> splitKey
    ) ++ readPreferenceConfig.asOptions ++ readConcernConfig.asOptions
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
