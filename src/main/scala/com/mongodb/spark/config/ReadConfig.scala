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
import org.apache.spark.sql.SQLContext

import com.mongodb.spark.rdd.partitioner.{DefaultMongoPartitioner, MongoPartitioner}
import com.mongodb.spark.{LoggingTrait, notNull}
import com.mongodb.{ConnectionString, ReadConcern, ReadPreference}

/**
 * The `ReadConfig` companion object
 *
 * $inputProperties
 *
 * @since 1.0
 */
object ReadConfig extends MongoInputConfig with LoggingTrait {

  type Self = ReadConfig

  private val DefaultSampleSize: Int = 1000
  private val DefaultPartitioner = DefaultMongoPartitioner
  private val DefaultPartitionerOptions = Map.empty[String, String]
  private val DefaultPartitionerPath = "com.mongodb.spark.rdd.partitioner."
  private val DefaultRegisterSQLHelperFunctions = false

  override def apply(options: collection.Map[String, String], default: Option[ReadConfig]): ReadConfig = {
    val cleanedOptions = stripPrefix(options)
    val cachedConnectionString = connectionString(cleanedOptions)
    val defaultDatabase = default.map(conf => conf.databaseName).orElse(Option(cachedConnectionString.getDatabase))
    val defaultCollection = default.map(conf => conf.collectionName).orElse(Option(cachedConnectionString.getCollection))
    val defaultPartitioner = default.map(conf => conf.partitioner).getOrElse(DefaultPartitioner)
    val defaultPartitionerOptions = default.map(conf => conf.partitionerOptions).getOrElse(DefaultPartitionerOptions)
    val partitionerOptions = defaultPartitionerOptions ++ getPartitionerOptions(cleanedOptions)

    new ReadConfig(
      databaseName = databaseName(databaseNameProperty, cleanedOptions, defaultDatabase),
      collectionName = collectionName(collectionNameProperty, cleanedOptions, defaultCollection),
      connectionString = cleanedOptions.get(mongoURIProperty).orElse(default.flatMap(conf => conf.connectionString)),
      sampleSize = getInt(cleanedOptions.get(sampleSizeProperty), default.map(conf => conf.sampleSize), DefaultSampleSize),
      partitioner = cleanedOptions.get(partitionerProperty).map(getPartitioner).getOrElse(defaultPartitioner),
      partitionerOptions = partitionerOptions,
      localThreshold = getInt(cleanedOptions.get(localThresholdProperty), default.map(conf => conf.localThreshold), MongoSharedConfig.DefaultLocalThreshold),
      readPreferenceConfig = ReadPreferenceConfig(cleanedOptions, default.map(conf => conf.readPreferenceConfig)),
      readConcernConfig = ReadConcernConfig(cleanedOptions, default.map(conf => conf.readConcernConfig)),
      registerSQLHelperFunctions = getBoolean(
        cleanedOptions.get(registerSQLHelperFunctions),
        default.map(conf => conf.registerSQLHelperFunctions), DefaultRegisterSQLHelperFunctions
      )
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
   * @param partitioner the class name of the partitioner to use to create partitions
   * @param partitionerOptions the configuration options for the partitioner
   * @param localThreshold the local threshold in milliseconds used when choosing among multiple MongoDB servers to send a request.
   *                       Only servers whose ping time is less than or equal to the server with the fastest ping time plus the local
   *                       threshold will be chosen.
   * @param readPreference the readPreference configuration
   * @param readConcern the readConcern configuration
   * @since 1.0
   */
  def create(databaseName: String, collectionName: String, connectionString: String, sampleSize: Int,
             partitioner: String, partitionerOptions: util.Map[String, String], localThreshold: Int, readPreference: ReadPreference,
             readConcern: ReadConcern): ReadConfig = {
    create(databaseName, collectionName, connectionString, sampleSize, partitioner, partitionerOptions, localThreshold, readPreference,
      readConcern, DefaultRegisterSQLHelperFunctions)
  }

  /**
   * Read Configuration used when reading data from MongoDB
   *
   * @param databaseName the database name
   * @param collectionName the collection name
   * @param connectionString the optional connection string used in the creation of this configuration
   * @param sampleSize a positive integer sample size to draw from the collection when inferring the schema
   * @param partitioner the class name of the partitioner to use to create partitions
   * @param partitionerOptions the configuration options for the partitioner
   * @param localThreshold the local threshold in milliseconds used when choosing among multiple MongoDB servers to send a request.
   *                       Only servers whose ping time is less than or equal to the server with the fastest ping time plus the local
   *                       threshold will be chosen.
   * @param readPreference the readPreference configuration
   * @param readConcern the readConcern configuration
   * @since 1.0
   */
  def create(databaseName: String, collectionName: String, connectionString: String, sampleSize: Int,
             partitioner: String, partitionerOptions: util.Map[String, String], localThreshold: Int,
             readPreference: ReadPreference, readConcern: ReadConcern, registerHelperFunctions: Boolean): ReadConfig = {
    notNull("databaseName", databaseName)
    notNull("collectionName", collectionName)
    notNull("partitioner", partitioner)
    notNull("readPreference", readPreference)
    notNull("readConcern", readConcern)
    notNull("registerHelperFunctions", registerHelperFunctions)

    new ReadConfig(databaseName, collectionName, Option(connectionString), sampleSize, getPartitioner(partitioner),
      getPartitionerOptions(partitionerOptions.asScala), localThreshold, ReadPreferenceConfig(readPreference),
      ReadConcernConfig(readConcern), registerHelperFunctions)
  }

  // scalastyle:on parameter.number

  override def create(javaSparkContext: JavaSparkContext): ReadConfig = {
    notNull("javaSparkContext", javaSparkContext)
    apply(javaSparkContext.getConf)
  }

  override def create(sqlContext: SQLContext): ReadConfig = {
    notNull("sqlContext", sqlContext)
    apply(sqlContext)
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

  override def create(sparkConf: SparkConf, options: util.Map[String, String]): ReadConfig = {
    notNull("sparkConf", sparkConf)
    notNull("options", options)
    apply(sparkConf, options.asScala)
  }

  private def getPartitioner(partitionerName: String): MongoPartitioner = {
    val partitionerClassName = if (partitionerName.contains(".")) partitionerName else s"$DefaultPartitionerPath$partitionerName"
    val parsedPartitioner = Try({
      val clazz = Class.forName(partitionerClassName)
      partitionerClassName.endsWith("$") match {
        case true  => clazz.getField("MODULE$").get(clazz).asInstanceOf[MongoPartitioner]
        case false => clazz.newInstance().asInstanceOf[MongoPartitioner]
      }
    })
    if (parsedPartitioner.isFailure) {
      logWarning(s"Could not load the partitioner: '$partitionerName'. Please check the namespace is correct.")
      throw parsedPartitioner.failed.get
    }
    parsedPartitioner.get
  }

  private def getPartitionerOptions(options: collection.Map[String, String]): collection.Map[String, String] = {
    stripPrefix(options).map(kv => (kv._1.toLowerCase, kv._2))
      .filter(kv => kv._1.startsWith(partitionerOptionsProperty)).map(kv => (kv._1.stripPrefix(s"$partitionerOptionsProperty."), kv._2))
  }
}

/**
 * Read Configuration used when reading data from MongoDB
 *
 * @param databaseName the database name
 * @param collectionName the collection name
 * @param connectionString the optional connection string used in the creation of this configuration
 * @param sampleSize a positive integer sample size to draw from the collection when inferring the schema
 * @param partitioner the class name of the partitioner to use to create partitions
 * @param partitionerOptions the configuration options for the partitioner
 * @param localThreshold the local threshold in milliseconds used when choosing among multiple MongoDB servers to send a request.
 *                       Only servers whose ping time is less than or equal to the server with the fastest ping time plus the local
 *                       threshold will be chosen.
 * @param readPreferenceConfig the readPreference configuration
 * @param readConcernConfig the readConcern configuration
 * @since 1.0
 */
case class ReadConfig(
    databaseName:               String,
    collectionName:             String,
    connectionString:           Option[String]                 = None,
    sampleSize:                 Int                            = ReadConfig.DefaultSampleSize,
    partitioner:                MongoPartitioner               = ReadConfig.DefaultPartitioner,
    partitionerOptions:         collection.Map[String, String] = ReadConfig.DefaultPartitionerOptions,
    localThreshold:             Int                            = MongoSharedConfig.DefaultLocalThreshold,
    readPreferenceConfig:       ReadPreferenceConfig           = ReadPreferenceConfig(),
    readConcernConfig:          ReadConcernConfig              = ReadConcernConfig(),
    registerSQLHelperFunctions: Boolean                        = ReadConfig.DefaultRegisterSQLHelperFunctions
) extends MongoCollectionConfig with MongoClassConfig {
  require(Try(connectionString.map(uri => new ConnectionString(uri))).isSuccess, s"Invalid uri: '${connectionString.get}'")
  require(sampleSize > 0, s"sampleSize ($sampleSize) must be greater than 0")
  require(localThreshold >= 0, s"localThreshold ($localThreshold) must be greater or equal to 0")

  type Self = ReadConfig

  override def withOption(key: String, value: String): ReadConfig = ReadConfig(this.asOptions + (key -> value))

  override def withOptions(options: collection.Map[String, String]): ReadConfig = ReadConfig(options, Some(this))

  override def asOptions: collection.Map[String, String] = {
    val options = Map(
      ReadConfig.databaseNameProperty -> databaseName,
      ReadConfig.collectionNameProperty -> collectionName,
      ReadConfig.sampleSizeProperty -> sampleSize.toString,
      ReadConfig.partitionerProperty -> partitioner.getClass.getName,
      ReadConfig.localThresholdProperty -> localThreshold.toString
    ) ++ partitionerOptions.map(kv => (s"${ReadConfig.partitionerOptionsProperty}.${kv._1}".toLowerCase, kv._2)) ++
      readPreferenceConfig.asOptions ++ readConcernConfig.asOptions

    connectionString match {
      case Some(uri) => options + (ReadConfig.mongoURIProperty -> uri)
      case None      => options
    }
  }

  override def withOptions(options: util.Map[String, String]): ReadConfig = withOptions(options.asScala)

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
