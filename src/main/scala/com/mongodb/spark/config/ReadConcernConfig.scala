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
import scala.collection.Map
import scala.util.Try

import org.apache.spark.SparkConf

import com.mongodb.{ReadConcern, ReadConcernLevel}

/**
 * The `ReadConcernConfig` companion object
 *
 * @since 1.0
 */
object ReadConcernConfig extends MongoInputConfig {

  type Self = ReadConcernConfig

  /**
   * Creates a `ReadConcernConfig` from a `ReadConcern` instance
   *
   * @param readConcern the read concern
   * @return the configuration
   */
  def apply(readConcern: ReadConcern): ReadConcernConfig = {
    new ReadConcernConfig(Option(readConcern.asDocument().get("level")) match {
      case Some(level) => Some(level.asString().getValue)
      case None        => None
    })
  }

  override def apply(options: collection.Map[String, String], default: Option[ReadConcernConfig]): ReadConcernConfig = {
    val cleanedOptions = prefixLessOptions(options)

    val defaultReadConcernConfig: ReadConcernConfig = default.getOrElse(
      Option(connectionString(cleanedOptions).getReadConcern) match {
        case Some(readCon) => ReadConcernConfig(readCon)
        case None          => ReadConcernConfig()
      }
    )

    options.get(readConcernLevelProperty).map(level => new ReadConcernConfig(Some(level))).getOrElse(defaultReadConcernConfig)
  }

  /**
   * Default configuration
   *
   * @return the configuration
   */
  def create(): ReadConcernConfig = ReadConcernConfig()

  /**
   * Creates a `ReadConcernConfig` from a `ReadConcern` instance
   *
   * @param readConcern the read concern
   * @return the configuration
   */
  def create(readConcern: ReadConcern): ReadConcernConfig = apply(readConcern)

  override def create(sparkConf: SparkConf): ReadConcernConfig = apply(sparkConf)

  override def create(options: util.Map[String, String]): ReadConcernConfig = apply(options.asScala, None)

  override def create(options: util.Map[String, String], default: ReadConcernConfig): ReadConcernConfig = apply(options.asScala, Option(default))
}

/**
 * The `ReadConcern` configuration used by the [[ReadConfig]].
 *
 * @param readConcernLevel the optional read concern level. If None the servers default level will be used.
 * @since 1.0
 */
case class ReadConcernConfig(private val readConcernLevel: Option[String] = None) extends MongoSparkConfig {
  require(Try(readConcern).isSuccess, s"Invalid ReadConcernConfig configuration")

  type Self = ReadConcernConfig

  override def asOptions: collection.Map[String, String] = readConcernLevel match {
    case Some(level) => Map(ReadConcernConfig.readConcernLevelProperty -> level)
    case None        => Map()
  }

  override def withOptions(options: collection.Map[String, String]): ReadConcernConfig = ReadConcernConfig(options, Some(this))

  override def withJavaOptions(options: util.Map[String, String]): ReadConcernConfig = withOptions(options.asScala)

  override def asJavaOptions: util.Map[String, String] = asOptions.asJava

  /**
   * The `ReadConcern` that this config represents
   *
   * @return the ReadConcern
   */
  def readConcern: ReadConcern = {
    readConcernLevel match {
      case Some(level) => new ReadConcern(ReadConcernLevel.fromString(level))
      case None        => ReadConcern.DEFAULT
    }
  }
}
