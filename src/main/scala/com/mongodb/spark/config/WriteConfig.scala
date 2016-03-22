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
import com.mongodb.WriteConcern
import org.apache.spark.api.java.JavaSparkContext

/**
 * The `WriteConfig` companion object
 *
 * @since 1.0
 */
object WriteConfig extends MongoOutputConfig {

  type Self = WriteConfig

  /**
   * Creates a WriteConfig
   *
   * @param databaseName the database name
   * @param collectionName the collection name
   * @param writeConcern the WriteConcern to use
   * @return the write config
   */
  def apply(databaseName: String, collectionName: String, writeConcern: WriteConcern): WriteConfig =
    new WriteConfig(databaseName, collectionName, WriteConcernConfig(writeConcern))

  override def apply(options: collection.Map[String, String], default: Option[WriteConfig]): WriteConfig = {
    val cleanedOptions = prefixLessOptions(options)
    WriteConfig(
      databaseName = databaseName(databaseNameProperty, cleanedOptions, default.map(writeConf => writeConf.databaseName)),
      collectionName = collectionName(collectionNameProperty, cleanedOptions, default.map(writeConf => writeConf.collectionName)),
      writeConcernConfig = WriteConcernConfig(cleanedOptions, default.map(writeConf => writeConf.writeConcernConfig))
    )
  }

  override def create(javaSparkContext: JavaSparkContext): WriteConfig = apply(javaSparkContext.getConf)

  override def create(sparkConf: SparkConf): WriteConfig = apply(sparkConf)

  override def create(options: util.Map[String, String]): WriteConfig = apply(options.asScala)

  override def create(options: util.Map[String, String], default: WriteConfig): WriteConfig = apply(options.asScala, Option(default))

}

/**
 * Write Configuration for writes to MongoDB
 *
 * @param databaseName the database name
 * @param collectionName the collection name
 * @param writeConcernConfig the write concern configuration
 * @since 1.0
 */
case class WriteConfig(
    databaseName:                   String,
    collectionName:                 String,
    private val writeConcernConfig: WriteConcernConfig = WriteConcernConfig.Default
) extends MongoCollectionConfig with MongoSparkConfig {

  type Self = WriteConfig

  override def withOptions(options: collection.Map[String, String]): WriteConfig = WriteConfig(options, Some(this))

  override def asOptions: collection.Map[String, String] = Map("database" -> databaseName, "collection" -> collectionName) ++ writeConcernConfig.asOptions

  override def withJavaOptions(options: util.Map[String, String]): WriteConfig = withOptions(options.asScala)

  override def asJavaOptions: util.Map[String, String] = asOptions.asJava

  /**
   * The `WriteConcern` that this config represents
   *
   * @return the WriteConcern
   */
  def writeConcern: WriteConcern = writeConcernConfig.writeConcern
}
