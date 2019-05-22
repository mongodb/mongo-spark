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

import com.mongodb.client.model.Collation
import com.mongodb.spark.rdd.partitioner.{DefaultMongoPartitioner, MongoPartitioner}
import com.mongodb.spark.{LoggingTrait, notNull}
import com.mongodb.{ConnectionString, MongoClient, ReadConcern, ReadPreference}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.bson.BsonDocument
import org.bson.conversions.Bson

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

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
  private val DefaultSamplePoolSize: Int = 10000
  private val DefaultPartitioner = DefaultMongoPartitioner
  private val DefaultPartitionerOptions = Map.empty[String, String]
  private val DefaultPartitionerPath = "com.mongodb.spark.rdd.partitioner."
  private val DefaultCollation = Collation.builder().build()
  private val DefaultHint = new BsonDocument()
  private val DefaultRegisterSQLHelperFunctions = false
  private val DefaultInferSchemaMapTypesEnabled = true
  private val DefaultInferSchemaMapTypesMinimumKeys = 250
  private val DefaultPipelineIncludeNullFilters = true
  private val DefaultPipelineIncludeFiltersAndProjections = true
  private val DefaultBatchSize = None

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
      aggregationConfig = AggregationConfig(cleanedOptions, default.map(conf => conf.aggregationConfig)),
      registerSQLHelperFunctions = getBoolean(
        cleanedOptions.get(registerSQLHelperFunctionsProperty),
        default.map(conf => conf.registerSQLHelperFunctions), DefaultRegisterSQLHelperFunctions
      ),
      inferSchemaMapTypesEnabled = getBoolean(
        cleanedOptions.get(inferSchemaMapTypeEnabledProperty),
        default.map(conf => conf.inferSchemaMapTypesEnabled),
        DefaultInferSchemaMapTypesEnabled
      ),
      inferSchemaMapTypesMinimumKeys = getInt(
        cleanedOptions.get(inferSchemaMapTypeMinimumKeysProperty),
        default.map(conf => conf.inferSchemaMapTypesMinimumKeys),
        DefaultInferSchemaMapTypesMinimumKeys
      ),
      pipelineIncludeNullFilters = getBoolean(
        cleanedOptions.get(pipelineIncludeNullFiltersProperty),
        default.map(conf => conf.pipelineIncludeNullFilters),
        DefaultPipelineIncludeNullFilters
      ),
      pipelineIncludeFiltersAndProjections = getBoolean(
        cleanedOptions.get(pipelineIncludeFiltersAndProjectionsProperty),
        default.map(conf => conf.pipelineIncludeFiltersAndProjections),
        DefaultPipelineIncludeFiltersAndProjections
      ),
      samplePoolSize = getInt(cleanedOptions.get(samplePoolSizeProperty), default.map(conf => conf.samplePoolSize), DefaultSamplePoolSize),
      batchSize = cleanedOptions.get(batchSizeProperty).map(_.toInt).orElse(default.flatMap(conf => conf.batchSize))
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
   * @param registerSQLHelperFunctions true to register sql helper functions
   * @since 1.0
   */
  def create(databaseName: String, collectionName: String, connectionString: String, sampleSize: Int,
             partitioner: String, partitionerOptions: util.Map[String, String], localThreshold: Int,
             readPreference: ReadPreference, readConcern: ReadConcern, registerSQLHelperFunctions: Boolean): ReadConfig = {
    create(databaseName, collectionName, connectionString, sampleSize, partitioner, partitionerOptions, localThreshold, readPreference,
      readConcern, DefaultCollation, DefaultHint, registerSQLHelperFunctions)
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
   * @param collation the collation
   * @param hint      the aggregation hint
   * @since 2.3
   */
  def create(databaseName: String, collectionName: String, connectionString: String, sampleSize: Int,
             partitioner: String, partitionerOptions: util.Map[String, String], localThreshold: Int,
             readPreference: ReadPreference, readConcern: ReadConcern, collation: Collation, hint: BsonDocument): ReadConfig = {
    create(databaseName, collectionName, connectionString, sampleSize, partitioner, partitionerOptions, localThreshold, readPreference,
      readConcern, collation, hint, DefaultRegisterSQLHelperFunctions)
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
   * @param localThreshold                 the local threshold in milliseconds used when choosing among multiple MongoDB servers to send a request.
   *                                       Only servers whose ping time is less than or equal to the server with the fastest ping time plus the local
   *                                       threshold will be chosen.
   * @param readPreference                 the readPreference configuration
   * @param readConcern                    the readConcern configuration
   * @param collation the collation
   * @param hint      the aggregation hint
   * @param registerSQLHelperFunctions     true to register sql helper functions
   * @return the ReadConfig
   * @since 2.3
   */
  def create(databaseName: String, collectionName: String, connectionString: String, sampleSize: Int,
             partitioner: String, partitionerOptions: util.Map[String, String], localThreshold: Int,
             readPreference: ReadPreference, readConcern: ReadConcern, collation: Collation, hint: BsonDocument,
             registerSQLHelperFunctions: Boolean): ReadConfig = {
    create(databaseName, collectionName, connectionString, sampleSize, partitioner, partitionerOptions, localThreshold, readPreference,
      readConcern, collation, hint, registerSQLHelperFunctions, DefaultInferSchemaMapTypesEnabled, DefaultInferSchemaMapTypesMinimumKeys,
      DefaultPipelineIncludeNullFilters, DefaultPipelineIncludeFiltersAndProjections)
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
   * @param localThreshold                 the local threshold in milliseconds used when choosing among multiple MongoDB servers to send a request.
   *                                       Only servers whose ping time is less than or equal to the server with the fastest ping time plus the local
   *                                       threshold will be chosen.
   * @param readPreference                 the readPreference configuration
   * @param readConcern                    the readConcern configuration
   * @param collation the collation
   * @param hint      the aggregation hint
   * @param registerSQLHelperFunctions     true to register sql helper functions
   * @param inferSchemaMapTypesEnabled     true to detect MapTypes when inferring Schema.
   * @param inferSchemaMapTypesMinimumKeys the minimum number of keys before a document can be inferred as a MapType.
   * @return the ReadConfig
   * @since 2.3
   */
  def create(databaseName: String, collectionName: String, connectionString: String, sampleSize: Int,
             partitioner: String, partitionerOptions: util.Map[String, String], localThreshold: Int,
             readPreference: ReadPreference, readConcern: ReadConcern, collation: Collation, hint: BsonDocument,
             registerSQLHelperFunctions: Boolean, inferSchemaMapTypesEnabled: Boolean, inferSchemaMapTypesMinimumKeys: Int): ReadConfig = {
    create(databaseName, collectionName, connectionString, sampleSize, partitioner, partitionerOptions, localThreshold, readPreference,
      readConcern, collation, hint, registerSQLHelperFunctions, inferSchemaMapTypesEnabled, inferSchemaMapTypesMinimumKeys,
      DefaultPipelineIncludeNullFilters, DefaultPipelineIncludeFiltersAndProjections)
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
   * @param collation the collation
   * @param hint      the aggregation hint
   * @param registerSQLHelperFunctions     true to register sql helper functions
   * @param inferSchemaMapTypesEnabled     true to detect MapTypes when inferring Schema.
   * @param inferSchemaMapTypesMinimumKeys the minimum number of keys before a document can be inferred as a MapType.
   * @param pipelineIncludeNullFilters true to include and push down null and exists filters into the pipeline when using sql.
   * @param pipelineIncludeFiltersAndProjections true to push down filters and projections into the pipeline when using sql.
   * @return the ReadConfig
   * @since 2.3
   */
  def create(databaseName: String, collectionName: String, connectionString: String, sampleSize: Int,
             partitioner: String, partitionerOptions: util.Map[String, String], localThreshold: Int,
             readPreference: ReadPreference, readConcern: ReadConcern, collation: Collation, hint: BsonDocument,
             registerSQLHelperFunctions: Boolean, inferSchemaMapTypesEnabled: Boolean, inferSchemaMapTypesMinimumKeys: Int,
             pipelineIncludeNullFilters: Boolean, pipelineIncludeFiltersAndProjections: Boolean): ReadConfig = {
    notNull("databaseName", databaseName)
    notNull("collectionName", collectionName)
    notNull("partitioner", partitioner)
    notNull("readPreference", readPreference)
    notNull("readConcern", readConcern)
    notNull("collation", collation)
    notNull("hint", hint)
    notNull("registerHelperFunctions", registerSQLHelperFunctions)
    notNull("inferSchemaMapTypesEnabled", inferSchemaMapTypesEnabled)
    notNull("inferSchemaMapTypesMinimumKeys", inferSchemaMapTypesMinimumKeys)
    notNull("pipelineIncludeNullFilters", pipelineIncludeNullFilters)
    notNull("pipelineIncludeFiltersAndProjections", pipelineIncludeFiltersAndProjections)

    new ReadConfig(databaseName, collectionName, Option(connectionString), sampleSize, getPartitioner(partitioner),
      getPartitionerOptions(partitionerOptions.asScala), localThreshold, ReadPreferenceConfig(readPreference),
      ReadConcernConfig(readConcern), AggregationConfig(collation, hint), registerSQLHelperFunctions, inferSchemaMapTypesEnabled,
      inferSchemaMapTypesMinimumKeys, pipelineIncludeNullFilters, pipelineIncludeFiltersAndProjections)
  }

  // scalastyle:on parameter.number

  override def create(javaSparkContext: JavaSparkContext): ReadConfig = {
    notNull("javaSparkContext", javaSparkContext)
    apply(javaSparkContext.getConf)
  }

  @deprecated("As of Spark 2.0 SQLContext was replaced by SparkSession. Use the SparkSession method instead", "2.0.0")
  override def create(sqlContext: SQLContext): ReadConfig = {
    notNull("sqlContext", sqlContext)
    create(sqlContext.sparkSession)
  }

  override def create(sparkSession: SparkSession): ReadConfig = {
    notNull("sparkSession", sparkSession)
    apply(sparkSession)
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
      if (partitionerClassName.endsWith("$")) {
        clazz.getField("MODULE$").get(clazz).asInstanceOf[MongoPartitioner]
      } else {
        clazz.newInstance().asInstanceOf[MongoPartitioner]
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
 * @param aggregationConfig the aggregation configuration
 * @param registerSQLHelperFunctions true to register sql helper functions
 * @param inferSchemaMapTypesEnabled true to detect MapTypes when inferring Schema.
 * @param inferSchemaMapTypesMinimumKeys the minimum number of keys before a document can be inferred as a MapType.
 * @param pipelineIncludeNullFilters true to include and push down null and exists filters into the pipeline when using sql.
 * @param pipelineIncludeFiltersAndProjections true to push down filters and projections into the pipeline when using sql.
 * @param samplePoolSize the size of the pool to take a sample from, used when there is no `\$sample` support or if there is a pushed down aggregation
 * @param batchSize the optional size for the internal batches used within the cursor
 * @since 1.0
 */
case class ReadConfig(
    databaseName:                         String,
    collectionName:                       String,
    connectionString:                     Option[String]                 = None,
    sampleSize:                           Int                            = ReadConfig.DefaultSampleSize,
    partitioner:                          MongoPartitioner               = ReadConfig.DefaultPartitioner,
    partitionerOptions:                   collection.Map[String, String] = ReadConfig.DefaultPartitionerOptions,
    localThreshold:                       Int                            = MongoSharedConfig.DefaultLocalThreshold,
    readPreferenceConfig:                 ReadPreferenceConfig           = ReadPreferenceConfig(),
    readConcernConfig:                    ReadConcernConfig              = ReadConcernConfig(),
    aggregationConfig:                    AggregationConfig              = AggregationConfig(),
    registerSQLHelperFunctions:           Boolean                        = ReadConfig.DefaultRegisterSQLHelperFunctions,
    inferSchemaMapTypesEnabled:           Boolean                        = ReadConfig.DefaultInferSchemaMapTypesEnabled,
    inferSchemaMapTypesMinimumKeys:       Int                            = ReadConfig.DefaultInferSchemaMapTypesMinimumKeys,
    pipelineIncludeNullFilters:           Boolean                        = ReadConfig.DefaultPipelineIncludeNullFilters,
    pipelineIncludeFiltersAndProjections: Boolean                        = ReadConfig.DefaultPipelineIncludeFiltersAndProjections,
    samplePoolSize:                       Int                            = ReadConfig.DefaultSamplePoolSize,
    batchSize:                            Option[Int]                    = ReadConfig.DefaultBatchSize
) extends MongoCollectionConfig with MongoClassConfig {
  require(Try(connectionString.map(uri => new ConnectionString(uri))).isSuccess, s"Invalid uri: '${connectionString.get}'")
  require(sampleSize > 0, s"sampleSize ($sampleSize) must be greater than 0")
  require(localThreshold >= 0, s"localThreshold ($localThreshold) must be greater or equal to 0")
  require(batchSize.getOrElse(2) > 1, s"batchSize (${batchSize.get} must be greater than 1.")

  type Self = ReadConfig

  override def withOption(key: String, value: String): ReadConfig = ReadConfig(this.asOptions + (key -> value))

  override def withOptions(options: collection.Map[String, String]): ReadConfig = ReadConfig(options, Some(this))

  override def asOptions: collection.Map[String, String] = {
    val options: mutable.Map[String, String] = mutable.Map(
      ReadConfig.databaseNameProperty -> databaseName,
      ReadConfig.collectionNameProperty -> collectionName,
      ReadConfig.sampleSizeProperty -> sampleSize.toString,
      ReadConfig.samplePoolSizeProperty -> samplePoolSize.toString,
      ReadConfig.partitionerProperty -> partitioner.getClass.getName,
      ReadConfig.localThresholdProperty -> localThreshold.toString,
      ReadConfig.registerSQLHelperFunctionsProperty -> registerSQLHelperFunctions.toString,
      ReadConfig.inferSchemaMapTypeEnabledProperty -> inferSchemaMapTypesEnabled.toString,
      ReadConfig.inferSchemaMapTypeMinimumKeysProperty -> inferSchemaMapTypesMinimumKeys.toString,
      ReadConfig.pipelineIncludeNullFiltersProperty -> pipelineIncludeNullFilters.toString,
      ReadConfig.pipelineIncludeFiltersAndProjectionsProperty -> pipelineIncludeFiltersAndProjections.toString
    )

    partitionerOptions.map(kv => options += s"${ReadConfig.partitionerOptionsProperty}.${kv._1}".toLowerCase -> kv._2)
    connectionString.map(uri => options += ReadConfig.mongoURIProperty -> uri)
    batchSize.map(size => options += ReadConfig.batchSizeProperty -> size.toString)
    options ++= readPreferenceConfig.asOptions
    options ++= readConcernConfig.asOptions
    options ++= aggregationConfig.asOptions

    options.toMap
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

  /**
   * Returns a copy with the specified aggregation pipeline applied to the aggregation config
   *
   * @param pipeline the aggregation pipeline to use
   * @return the updated Read Config
   */
  def withPipeline[B <: Bson](pipeline: Seq[B]): ReadConfig = {
    val pipelineString = if (pipeline.isEmpty) {
      None
    } else {
      Some(pipeline.map(x => x.toBsonDocument(classOf[BsonDocument], MongoClient.getDefaultCodecRegistry).toJson()).mkString("[", ",", "]"))
    }
    copy(aggregationConfig = aggregationConfig.copy(pipelineString = pipelineString))
  }

  /**
   * @return the aggregation pipeline to use from the aggregation config or an empty list
   */
  def pipeline: List[BsonDocument] = aggregationConfig.pipeline.getOrElse(List.empty[BsonDocument])
}
