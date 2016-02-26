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

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.apache.spark.SparkConf

import org.bson._
import com.mongodb.WriteConcern

/**
 * The `WriteConfig` companion object
 *
 * @since 1.0
 */
object WriteConfig {

  // Property names
  val databaseNameProperty = "mongodb.output.databaseName"
  val collectionNameProperty = "mongodb.output.collectionName"
  val writeConcernProperty = "mongodb.output.writeConcern"

  // Whitelist for allowed Write context variables
  val Properties = Set(
    databaseNameProperty,
    collectionNameProperty,
    writeConcernProperty
  )

  private val DefaultWriteConcern = WriteConcern.ACKNOWLEDGED

  /**
   * Creates the `WriteConfig` from settings in the `SparkConf`
   *
   * @param sparkConf the spark configuration
   * @return the WriteConfig
   */
  def apply(sparkConf: SparkConf): WriteConfig = {
    require(sparkConf.contains(databaseNameProperty), s"Missing '$databaseNameProperty' property in the SparkConf")
    require(sparkConf.contains(collectionNameProperty), s"Missing '$collectionNameProperty' property in the SparkConf")

    WriteConfig(
      databaseName = sparkConf.get(databaseNameProperty),
      collectionName = sparkConf.get(collectionNameProperty),
      writeConcern = getWriteConcern(sparkConf.getOption(writeConcernProperty))
    )
  }

  /**
   * Creates the `WriteConfig` from settings in the `SparkConf`
   *
   * @param sparkConf the spark configuration
   * @return the WriteConfig
   */
  def create(sparkConf: SparkConf): WriteConfig = apply(sparkConf)

  // scalastyle:off cyclomatic.complexity
  private def getWriteConcern(writeConcernOption: Option[String], default: WriteConcern = DefaultWriteConcern): WriteConcern = {
    val writeConcern: WriteConcern = writeConcernOption match {
      case Some(json) if json.contains("{") => Try(BsonDocument.parse(json)) match {
        case Success(writeConcernDoc) =>
          val nullValue = new BsonNull()
          var concern = DefaultWriteConcern
          concern = writeConcernDoc.get("w", nullValue) match {
            case wInt: BsonInt32     => concern.withW(wInt.intValue)
            case wString: BsonString => concern.withW(wString.getValue)
            case _                   => concern
          }
          concern = writeConcernDoc.get("wtimeout", nullValue) match {
            case wTimeout: BsonInt32 => concern.withWTimeout(wTimeout.getValue, TimeUnit.MILLISECONDS)
            case _                   => concern
          }
          concern = writeConcernDoc.get("fsync", nullValue) match {
            case wFsync: BsonBoolean => concern.withJournal(wFsync.getValue)
            case _                   => concern
          }
          concern = writeConcernDoc.get("j", nullValue) match {
            case wJ: BsonBoolean => concern.withJournal(wJ.getValue)
            case _               => concern
          }
          concern
        case Failure(e) => WriteConcern.valueOf(json)
      }
      case Some(name) => WriteConcern.valueOf(name)
      case _          => default
    }
    require(Option(writeConcern).isDefined, s"WriteConcern (${writeConcernOption.get}) is not valid")
    writeConcern
  }
  // scalastyle:on cyclomatic.complexity

}

/**
 * Write Configuration for writes to MongoDB
 *
 * @param databaseName the database name
 * @param collectionName the collection name
 * @param writeConcern the write concern
 * @since 1.0
 */
case class WriteConfig(databaseName: String, collectionName: String,
                       writeConcern: WriteConcern = WriteConfig.DefaultWriteConcern) extends CollectionConfig {

  /**
   * Creates a new `WriteConfig` with the options applied
   *
   * *Note:* The `WriteConfig` options should not have the "mongodb.output." property prefix
   *
   * @param options a map of options to be applied to the `WriteConfig`
   * @return an updated `WriteConfig`
   */
  def withOptions(options: scala.collection.Map[String, String]): WriteConfig = {
    WriteConfig(
      databaseName = options.getOrElse("databaseName", databaseName),
      collectionName = options.getOrElse("collectionName", collectionName),
      writeConcern = WriteConfig.getWriteConcern(options.get("writeConcern"), writeConcern)
    )
  }

  /**
   * Creates a map of options representing the  `WriteConfig`
   *
   * @return the map representing the  `WriteConfig`
   */
  def asOptions: Map[String, String] =
    Map("databaseName" -> databaseName, "collectionName" -> collectionName, "writeConcern" -> writeConcern.asDocument().toJson)

  /**
   * Creates a new `WriteConfig` with the options applied
   *
   * *Note:* The `WriteConfig` options should not have the "mongodb.output." property prefix
   *
   * @param options a map of options to be applied to the `WriteConfig`
   * @return an updated `WriteConfig`
   */
  def withJavaOptions(options: java.util.Map[String, String]): WriteConfig = withOptions(options.asScala)

  /**
   * Creates a map of options representing the  `WriteConfig`
   *
   * @return the map representing the  `WriteConfig`
   */
  def asJavaOptions: java.util.Map[String, String] = asOptions.asJava

}
