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

import scala.util.Try

import org.apache.spark.{SparkConf, SparkContext}

import com.mongodb.ConnectionString
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

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
   * Create a configuration from the `sqlContext`
   *
   * Uses the prefixed properties that are set in the Spark configuration to create the config.
   *
   * @see [[configPrefix]]
   * @param sparkSession the SparkSession
   * @return the configuration
   */
  def apply(sparkSession: SparkSession): Self = apply(sparkSession.sparkContext.getConf)

  /**
   * Create a configuration from the `sqlContext`
   *
   * Uses the prefixed properties that are set in the Spark configuration to create the config.
   *
   * @see [[configPrefix]]
   * @param sqlContext the SQL context
   * @return the configuration
   */
  @deprecated("As of Spark 2.0 SQLContext was replaced by SparkSession. Use the SparkSession method instead", "2.0.0")
  def apply(sqlContext: SQLContext): Self = apply(sqlContext.sparkSession)

  /**
   * Create a configuration from the `sparkConf`
   *
   * Uses the prefixed properties that are set in the Spark configuration to create the config.
   *
   * @see [[configPrefix]]
   * @param sparkConf the spark configuration
   * @return the configuration
   */
  def apply(sparkConf: SparkConf): Self = apply(sparkConf, Map.empty[String, String])

  /**
   * Create a configuration from the `sparkConf`
   *
   * Uses the prefixed properties that are set in the Spark configuration to create the config.
   *
   * @see [[configPrefix]]
   * @param sparkConf the spark configuration
   * @param options overloaded parameters
   * @return the configuration
   */
  def apply(sparkConf: SparkConf, options: collection.Map[String, String]): Self =
    apply(getOptionsFromConf(sparkConf) ++ stripPrefix(options))

  /**
   * Create a configuration from the values in the `Map`
   *
   * '''Note:''' Values in the map do not need to be prefixed with the [[configPrefix]].
   *
   * @param options a map of properties and their string values
   * @return the configuration
   */
  def apply(options: collection.Map[String, String]): Self = {
    apply(options, None)
  }

  /**
   * Create a configuration from the values in the `Map`, using the optional default configuration for any default values.
   *
   * '''Note:''' Values in the map do not need to be prefixed with the [[configPrefix]].
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
   * Create a configuration easily from the Java API using the `JavaSparkContext`
   *
   * Uses the prefixed properties that are set in the Spark configuration to create the config.
   *
   * @see [[configPrefix]]
   * @param sparkSession the SparkSession
   * @return the configuration
   */
  def create(sparkSession: SparkSession): Self

  /**
   * Create a configuration easily from the Java API using the `JavaSparkContext`
   *
   * Uses the prefixed properties that are set in the Spark configuration to create the config.
   *
   * @see [[configPrefix]]
   * @param sqlContext the SQL context
   * @return the configuration
   */
  @deprecated("As of Spark 2.0 SQLContext was replaced by SparkSession. Use the SparkSession method instead", "2.0.0")
  def create(sqlContext: SQLContext): Self

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
   * Create a configuration from the `sparkConf`
   *
   * Uses the prefixed properties that are set in the Spark configuration to create the config.
   *
   * @see [[configPrefix]]
   * @param sparkConf the spark configuration
   * @param options overloaded parameters
   * @return the configuration
   */
  def create(sparkConf: SparkConf, options: util.Map[String, String]): Self

  /**
   * Create a configuration easily from the Java API using the values in the `Map`
   *
   * '''Note:''' Values in the map do not need to be prefixed with the [[configPrefix]].
   *
   * @param options a map of properties and their string values
   * @return the configuration
   */
  def create(options: util.Map[String, String]): Self

  /**
   * Create a configuration easily from the Java API using the values in the `Map`, using the optional default configuration for any
   * default values.
   *
   * '''Note:''' Values in the map do not need to be prefixed with the [[configPrefix]].
   *
   * @param options a map of properties and their string values
   * @param default the optional default configuration, used for determining the default values for the properties
   * @return the configuration
   */
  def create(options: util.Map[String, String], default: Self): Self

  /**
   * Strip the prefix from options
   *
   * @param options options that may contain the prefix
   * @return prefixLess options
   */
  def stripPrefix(options: collection.Map[String, String]): collection.Map[String, String] =
    options.map(kv => (kv._1.toLowerCase.stripPrefix(configPrefix), kv._2))

  /**
   * Gets an options map from the `SparkConf`
   *
   * @param sparkConf the SparkConf
   * @return the options
   */
  def getOptionsFromConf(sparkConf: SparkConf): collection.Map[String, String] =
    stripPrefix(sparkConf.getAll.filter(_._1.startsWith(configPrefix)).toMap)

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
    val cleanedOptions = stripPrefix(options)
    cleanedOptions.get(databaseNameProperty).orElse(default) match {
      case Some(databaseName) => databaseName
      case None => throw new IllegalArgumentException(
        s"Missing database name. Set via the '$configPrefix$mongoURIProperty' or '$configPrefix$databaseNameProperty' property"
      )
    }
  }

  protected def collectionName(collectionNameProperty: String, options: collection.Map[String, String], default: Option[String] = None): String = {
    val cleanedOptions = stripPrefix(options)
    cleanedOptions.get(collectionNameProperty).orElse(default) match {
      case Some(collectionName) => collectionName
      case None => throw new IllegalArgumentException(
        s"Missing collection name. Set via the '$configPrefix$mongoURIProperty' or '$configPrefix$collectionNameProperty' property"
      )
    }
  }

  protected def connectionString(options: collection.Map[String, String]) = {
    val uri = options.getOrElse(mongoURIProperty, DefaultConnectionString)
    val tryConnectionString = Try(new ConnectionString(uri))
    require(tryConnectionString.isSuccess, s"Invalid uri: '$uri'")
    tryConnectionString.get
  }

  private val DefaultConnectionString = "mongodb://localhost/"

}
