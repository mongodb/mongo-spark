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

package com.mongodb.spark.conf

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.spark.SparkConf

import com.mongodb.{ReadConcern, ReadConcernLevel, ReadPreference}

/**
 * The `ReadConfig` companion object
 *
 * @since 1.0
 */
object ReadConfig {

  // Property names
  val databaseNameProperty = "mongodb.input.databaseName"
  val collectionNameProperty = "mongodb.input.collectionName"
  val maxChunkSizeProperty = "mongodb.input.maxChunkSize"
  val splitKeyProperty = "mongodb.input.splitKey"
  val readPreferenceProperty = "mongodb.input.readPreference"
  val readConcernLevelProperty = "mongo.input.readConcernLevel"
  val sampleSizeProperty = "mongo.input.sampleSize"
  val samplingRatioProperty = "mongo.input.samplingRatio"

  // Whitelist for allowed Read environment variables
  val Properties = Set(
    databaseNameProperty,
    collectionNameProperty,
    maxChunkSizeProperty,
    splitKeyProperty,
    readPreferenceProperty,
    readConcernLevelProperty,
    sampleSizeProperty,
    samplingRatioProperty
  )

  private val DefaultMaxChunkSize = 64 // 64 MB
  private val DefaultSplitKey = "_id"
  private val DefaultReadPreferenceName = "primary"
  private val DefaultReadConcernLevel = None
  private val DefaultSampleSize: Int = 1000

  /**
   * Creates the `ReadConfig` from settings in the `SparkConf`
   *
   * @param sparkConf the spark configuration
   * @return the ReadConfig
   */
  def apply(sparkConf: SparkConf): ReadConfig = {
    require(sparkConf.contains(databaseNameProperty), s"Missing '$databaseNameProperty' property in the SparkConf")
    require(sparkConf.contains(collectionNameProperty), s"Missing '$collectionNameProperty' property in the SparkConf")

    ReadConfig(
      databaseName = sparkConf.get(databaseNameProperty),
      collectionName = sparkConf.get(collectionNameProperty),
      maxChunkSize = sparkConf.getInt(maxChunkSizeProperty, DefaultMaxChunkSize),
      splitKey = sparkConf.get(splitKeyProperty, DefaultSplitKey),
      readPreferenceName = sparkConf.get(readPreferenceProperty, DefaultReadPreferenceName),
      readConcernLevel = sparkConf.getOption(readConcernLevelProperty),
      sampleSize = sparkConf.getInt(sampleSizeProperty, DefaultSampleSize)
    )
  }

  /**
   * Creates the `ReadConfig` from settings in the `SparkConf`
   *
   * @param sparkConf the spark configuration
   * @return the ReadConfig
   */
  def create(sparkConf: SparkConf): ReadConfig = apply(sparkConf)

}

/**
 * Read Configuration used when reading data from MongoDB
 *
 * @param databaseName the database name
 * @param collectionName the collection name
 * @param maxChunkSize the maximum chunkSize for non-sharded collections
 * @param splitKey     the key to split the collection by for non-sharded collections
 * @param readPreferenceName the readPreference to use when reading data
 * @param readConcernLevel the readConcern to use
 * @param sampleSize         a positive integer sample size to draw from the collection
 * @since 1.0
 */
case class ReadConfig(
    databaseName:       String,
    collectionName:     String,
    maxChunkSize:       Int            = ReadConfig.DefaultMaxChunkSize,
    splitKey:           String         = ReadConfig.DefaultSplitKey,
    readPreferenceName: String         = ReadConfig.DefaultReadPreferenceName,
    readConcernLevel:   Option[String] = ReadConfig.DefaultReadConcernLevel,
    sampleSize:         Int            = ReadConfig.DefaultSampleSize
) {

  require(maxChunkSize > 0, s"maxChunkSize ($maxChunkSize) must be greater than 0")
  require(sampleSize > 0, s"sampleSize ($sampleSize) must be greater than 0")
  require(Try(readPreference).isSuccess, s"readPreferenceName ($readPreferenceName) is not valid")
  require(Try(readConcern).isSuccess, s"readConcernLevel ($readConcernLevel) is not valid")

  /**
   * Returns the `ReadConcern` setting for the `ReadConfig`
   *
   * @return the `ReadConcern`
   */
  def readConcern: ReadConcern = readConcernLevel match {
    case Some(level) => new ReadConcern(ReadConcernLevel.fromString(level))
    case None        => ReadConcern.DEFAULT
  }

  /**
   * Returns the `ReadPreference` setting for the `ReadConfig`
   *
   * @return the `ReadPreference`
   */
  def readPreference: ReadPreference = ReadPreference.valueOf(readPreferenceName)

  // scalastyle:off cyclomatic.complexity
  /**
   * Creates a new `ReadConfig` with the options applied
   *
   * *Note:* The `ReadConfig` options should not have the "mongodb.input." property prefix
   *
   * @param options a map of options to be applied to the `ReadConfig`
   * @return an updated `ReadConfig`
   */
  def withOptions(options: scala.collection.Map[String, String]): ReadConfig = {
    ReadConfig(
      databaseName = options.getOrElse("databaseName", databaseName),
      collectionName = options.getOrElse("collectionName", collectionName),
      maxChunkSize = options.get("maxChunkSize") match {
        case Some(size) => size.toInt
        case None       => maxChunkSize
      },
      splitKey = options.getOrElse("splitKey", splitKey),
      readPreferenceName = options.get("readPreference") match {
        case Some(readPreference) => readPreference
        case None                 => readPreferenceName
      },
      readConcernLevel = options.get("readConcernLevel") match {
        case Some(level) => Some(level)
        case None        => readConcernLevel
      },
      sampleSize = options.get("sampleSize") match {
        case Some(size) => size.toInt
        case None       => sampleSize
      }
    )
  }
  // scalastyle:on cyclomatic.complexity

  /**
   * Creates a map of options representing the  `ReadConfig`
   *
   * @return the map representing the `ReadConfig`
   */
  def asOptions: Map[String, String] = {
    val options: Map[String, String] = Map("databaseName" -> databaseName, "collectionName" -> collectionName,
      "maxChunkSize" -> maxChunkSize.toString, "splitKey" -> splitKey, "readPreference" -> readPreference.getName,
      "sampleSize" -> sampleSize.toString)

    readConcernLevel.isDefined match {
      case true  => options ++ Map("readConcernLevel" -> readConcernLevel.get)
      case false => options
    }
  }

  /**
   * Creates a new `ReadConfig` with the options applied
   *
   * *Note:* The `ReadConfig` options should not have the "mongodb.input." property prefix
   *
   * @param options a map of options to be applied to the `ReadConfig`
   * @return an updated `ReadConfig`
   */
  def withJavaOptions(options: java.util.Map[String, String]): ReadConfig = withOptions(options.asScala)

  /**
   * Creates a map of options representing the  `ReadConfig`
   *
   * @return the map representing the `ReadConfig`
   */
  def asJavaOptions: java.util.Map[String, String] = asOptions.asJava
}
