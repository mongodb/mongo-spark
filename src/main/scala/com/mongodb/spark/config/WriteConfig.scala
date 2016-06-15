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
   * @param localThreshold the local threshold in milliseconds used when choosing among multiple MongoDB servers to send a request.
   *                       Only servers whose ping time is less than or equal to the server with the fastest ping time plus the local
   *                       threshold will be chosen.
   * @param writeConcern the WriteConcern to use
   * @return the write config
   */
  def apply(databaseName: String, collectionName: String, localThreshold: Int, writeConcern: WriteConcern): WriteConfig =
    WriteConfig(databaseName, collectionName, None, localThreshold, WriteConcernConfig(writeConcern))

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
  def apply(databaseName: String, collectionName: String, connectionString: String, localThreshold: Int, writeConcern: WriteConcern): WriteConfig =
    WriteConfig(databaseName, collectionName, Option(connectionString), localThreshold, WriteConcernConfig(writeConcern))

  override def apply(options: collection.Map[String, String], default: Option[WriteConfig]): WriteConfig = {
    val cleanedOptions = stripPrefix(options)
    val cachedConnectionString = connectionString(cleanedOptions)
    val defaultDatabase = default.map(conf => conf.databaseName).orElse(Option(cachedConnectionString.getDatabase))
    val defaultCollection = default.map(conf => conf.collectionName).orElse(Option(cachedConnectionString.getCollection))

    WriteConfig(
      databaseName = databaseName(databaseNameProperty, cleanedOptions, defaultDatabase),
      collectionName = collectionName(collectionNameProperty, cleanedOptions, defaultCollection),
      connectionString = cleanedOptions.get(mongoURIProperty).orElse(default.flatMap(conf => conf.connectionString)),
      localThreshold = getInt(cleanedOptions.get(localThresholdProperty), default.map(conf => conf.localThreshold),
        MongoSharedConfig.DefaultLocalThreshold),
      writeConcernConfig = WriteConcernConfig(cleanedOptions, default.map(writeConf => writeConf.writeConcernConfig))
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
    notNull("databaseName", databaseName)
    notNull("collectionName", collectionName)
    notNull("localThreshold", localThreshold)
    notNull("writeConcern", writeConcern)
    apply(databaseName, collectionName, connectionString, localThreshold, writeConcern)
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
 * @param databaseName the database name
 * @param collectionName the collection name
 * @param connectionString the optional connection string used in the creation of this configuration
 * @param localThreshold the local threshold in milliseconds used when choosing among multiple MongoDB servers to send a request.
 *                       Only servers whose ping time is less than or equal to the server with the fastest ping time plus the local
 *                       threshold will be chosen.
 * @param writeConcernConfig the write concern configuration
 * @since 1.0
 */
case class WriteConfig(
    databaseName:       String,
    collectionName:     String,
    connectionString:   Option[String]     = None,
    localThreshold:     Int                = MongoSharedConfig.DefaultLocalThreshold,
    writeConcernConfig: WriteConcernConfig = WriteConcernConfig.Default
) extends MongoCollectionConfig with MongoClassConfig {
  require(localThreshold >= 0, s"localThreshold ($localThreshold) must be greater or equal to 0")
  require(Try(connectionString.map(uri => new ConnectionString(uri))).isSuccess, s"Invalid uri: '${connectionString.get}'")

  type Self = WriteConfig

  override def withOption(key: String, value: String): WriteConfig = WriteConfig(this.asOptions + (key -> value))

  override def withOptions(options: collection.Map[String, String]): WriteConfig = WriteConfig(options, Some(this))

  override def asOptions: collection.Map[String, String] = {
    val options = Map("database" -> databaseName, "collection" -> collectionName, "localThreshold" -> localThreshold.toString) ++ writeConcernConfig.asOptions
    connectionString match {
      case Some(uri) => options + (WriteConfig.mongoURIProperty -> uri)
      case None      => options
    }
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
