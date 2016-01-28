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

import org.apache.spark.SparkConf

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
    require(Option(writeConcern).isDefined, s"WriteConcern (${sparkConf.get(writeConcernProperty)}) is not valid")

    def writeConcern = sparkConf.getOption(writeConcernProperty) match {
      case Some(name) => WriteConcern.valueOf(name)
      case None       => DefaultWriteConcern
    }

    WriteConfig(
      databaseName = sparkConf.get(databaseNameProperty),
      collectionName = sparkConf.get(collectionNameProperty),
      writeConcern = writeConcern
    )
  }

}

/**
 * Write Configuration for writes to MongoDB
 *
 * @param databaseName the database name
 * @param collectionName the collection name
 * @param writeConcern the write concern
 *
 * @since 1.0
 */
case class WriteConfig(databaseName: String, collectionName: String, writeConcern: WriteConcern = WriteConfig.DefaultWriteConcern)
