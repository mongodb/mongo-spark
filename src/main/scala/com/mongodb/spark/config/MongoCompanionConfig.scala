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

import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.ConnectionString
import org.apache.spark.api.java.JavaSparkContext

/**
 * The Mongo configuration base trait
 *
 * Defines companion object helper methods for creating `MongoConfig` instances
 *
 * @since 1.0
 */
trait MongoCompanionConfig extends Serializable {

  /**
   * The type of the `MongoConfig`
   */
  type Self

  /**
   * The mongo URI property
   *
   * Represents a connection string.
   *
   * Any values set in the connection string will override any default values for the configuration.
   */
  val mongoURIProperty = MongoSharedConfig.mongoURIProperty

  /**
   * The configuration prefix string for the current configuration scope
   */
  val configPrefix: String

  /**
   * Create a configuration from the `sparkContext`
   *
   * Uses the prefixed properties that are set in the Spark configuration to create the config.
   *
   * @see [[configPrefix]]
   * @param sparkContext the spark context
   * @return the configuration
   */
  def apply(sparkContext: SparkContext): Self = apply(sparkContext.getConf)

  /**
   * Create a configuration from the `sparkConf`
   *
   * Uses the prefixed properties that are set in the Spark configuration to create the config.
   *
   * @see [[configPrefix]]
   * @param sparkConf the spark configuration
   * @return the configuration
   */
  def apply(sparkConf: SparkConf): Self = apply(sparkConf.getAll.filter(_._1.startsWith(configPrefix)).toMap)

  /**
   * Create a configuration from the values in the `Map`
   *
   * *Note:* Values in the map do not need to be prefixed with the [[configPrefix]].
   *
   * @param options a map of properties and their string values
   * @return the configuration
   */
  def apply(options: collection.Map[String, String]): Self = apply(options, None)

  /**
   * Create a configuration from the values in the `Map`, using the optional default configuration for any default values.
   *
   * *Note:* Values in the map do not need to be prefixed with the [[configPrefix]].
   *
   * @param options a map of properties and their string values
   * @param default the optional default configuration, used for determining the default values for the properties
   * @return the configuration
   */
  def apply(options: collection.Map[String, String], default: Option[Self]): Self

  /**
   * Create a configuration easily from the Java API using the `JavaSparkContext`
   *
   * Uses the prefixed properties that are set in the Spark configuration to create the config.
   *
   * @see [[configPrefix]]
   * @param javaSparkContext the java spark context
   * @return the configuration
   */
  def create(javaSparkContext: JavaSparkContext): Self

  /**
   * Create a configuration easily from the Java API using the `sparkConf`
   *
   * Uses the prefixed properties that are set in the Spark configuration to create the config.
   *
   * @see [[configPrefix]]
   * @param sparkConf the spark configuration
   * @return the configuration
   */
  def create(sparkConf: SparkConf): Self

  /**
   * Create a configuration easily from the Java API using the values in the `Map`
   *
   * *Note:* Values in the map do not need to be prefixed with the [[configPrefix]].
   *
   * @param options a map of properties and their string values
   * @return the configuration
   */
  def create(options: util.Map[String, String]): Self

  /**
   * Create a configuration easily from the Java API using the values in the `Map`, using the optional default configuration for any
   * default values.
   *
   * *Note:* Values in the map do not need to be prefixed with the [[configPrefix]].
   *
   * @param options a map of properties and their string values
   * @param default the optional default configuration, used for determining the default values for the properties
   * @return the configuration
   */
  def create(options: util.Map[String, String], default: Self): Self

  protected def prefixLessOptions(options: collection.Map[String, String]): collection.Map[String, String] =
    options.map(kv => (kv._1.toLowerCase.stripPrefix(configPrefix), kv._2))

  protected def getInt(newValue: Option[String], existingValue: Option[Int] = None, defaultValue: Int): Int = {
    newValue match {
      case Some(value) => value.toInt
      case None        => existingValue.getOrElse(defaultValue)
    }
  }

  protected def getString(newValue: Option[String], existingValue: Option[String] = None, defaultValue: String): String = {
    newValue match {
      case Some(value) => value
      case None        => existingValue.getOrElse(defaultValue)
    }
  }

  protected def getBoolean(newValue: Option[String], existingValue: Option[Boolean] = None, defaultValue: Boolean): Boolean = {
    newValue match {
      case Some(value) => value.toBoolean
      case None        => existingValue.getOrElse(defaultValue)
    }
  }

  protected def databaseName(databaseNameProperty: String, options: collection.Map[String, String], default: Option[String] = None): String = {
    val cleanedOptions = prefixLessOptions(options)
    val defaultDatabaseName = default.getOrElse(connectionString(cleanedOptions).getDatabase)
    Option(cleanedOptions.getOrElse(databaseNameProperty, defaultDatabaseName)) match {
      case Some(databaseName) => databaseName
      case None => throw new IllegalArgumentException(
        s"Missing database name. Set via the '$configPrefix$mongoURIProperty' or '$configPrefix$databaseNameProperty' property"
      )
    }
  }

  protected def collectionName(collectionNameProperty: String, options: collection.Map[String, String], default: Option[String] = None): String = {
    val cleanedOptions = prefixLessOptions(options)
    val defaultCollectionName = default.getOrElse(connectionString(cleanedOptions).getCollection)
    Option(cleanedOptions.getOrElse(collectionNameProperty, defaultCollectionName)) match {
      case Some(collectionName) => collectionName
      case None => throw new IllegalArgumentException(
        s"Missing collection name. Set via the '$configPrefix$mongoURIProperty' or '$configPrefix$collectionNameProperty' property"
      )
    }
  }

  protected def connectionString(options: collection.Map[String, String]) =
    new ConnectionString(options.getOrElse(mongoURIProperty, DefaultConnectionString))

  private val DefaultConnectionString = "mongodb://localhost/"

}
