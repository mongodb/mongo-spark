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

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf

/**
 * The `PartitionConfig` companion object
 *
 * @since 1.0
 */
object PartitionConfig {

  // Property names
  val maxChunkSizeProperty = "mongodb.partition.maxChunkSize"
  val splitKeyProperty = "mongodb.partition.splitKey"
  val shardedConnectDirectlyProperty = "mongodb.partition.shardedConnectDirectly"
  val shardedConnectToMongosOnlyProperty = "mongodb.partition.shardedConnectToMongosOnly"

  // Whitelist for allowed Read environment variables
  val Properties = Set(
    maxChunkSizeProperty,
    splitKeyProperty,
    shardedConnectDirectlyProperty,
    shardedConnectToMongosOnlyProperty
  )

  private val DefaultMaxChunkSize = 64 // 64 MB
  private val DefaultSplitKey = "_id"
  private val DefaultShardingConnectDirectly = false
  private val DefaultShardingConnectToMongos = true

  /**
   * Creates the `PartitionConfig` from settings in the `SparkConf`
   *
   * *Note:* Users the [[ReadConfig.databaseNameProperty]] and [[ReadConfig.collectionNameProperty]] to determine which collection to partition.
   *
   * @param sparkConf the spark configuration
   * @return the PartitionConfig
   */
  def apply(sparkConf: SparkConf): PartitionConfig = {
    require(sparkConf.contains(ReadConfig.databaseNameProperty), s"Missing '${ReadConfig.databaseNameProperty}' property in the SparkConf")
    require(sparkConf.contains(ReadConfig.collectionNameProperty), s"Missing '${ReadConfig.collectionNameProperty}' property in the SparkConf")

    PartitionConfig(
      databaseName = sparkConf.get(ReadConfig.databaseNameProperty),
      collectionName = sparkConf.get(ReadConfig.collectionNameProperty),
      maxChunkSize = sparkConf.getInt(maxChunkSizeProperty, DefaultMaxChunkSize),
      splitKey = sparkConf.get(splitKeyProperty, DefaultSplitKey),
      shardedConnectDirectly = sparkConf.getBoolean(shardedConnectDirectlyProperty, DefaultShardingConnectDirectly),
      shardedConnectToMongos = sparkConf.getBoolean(shardedConnectToMongosOnlyProperty, DefaultShardingConnectToMongos)
    )
  }

  /**
   * Creates the `PartitionConfig` from settings in the `SparkConf`
   *
   * @param sparkConf the spark configuration
   * @return the PartitionConfig
   */
  def create(sparkConf: SparkConf): PartitionConfig = apply(sparkConf)

}

/**
 * The Partition Configuration
 *
 * @param databaseName the database name
 * @param collectionName the collection name
 * @param maxChunkSize the maximum chunkSize for non-sharded collections
 * @param splitKey the key to split the collection by for non-sharded collections or the "shard key" for sharded collection
 * @param shardedConnectDirectly for sharded collections connect directly to the shard when reading the data.
 *                                *Caution:* If [[shardedConnectToMongos]] is set to false then the balancer must be off to ensure that
 *                                there are no duplicated documents.
 * @param shardedConnectToMongos for sharded collections only read data via mongos. Used inconjunction with [[shardedConnectDirectly]].
 *                                Ensures that there are no duplicated chunks by connecting via a mongos.
 * @since 1.0
 */
case class PartitionConfig(
    databaseName:           String,
    collectionName:         String,
    maxChunkSize:           Int     = PartitionConfig.DefaultMaxChunkSize,
    splitKey:               String  = PartitionConfig.DefaultSplitKey,
    shardedConnectDirectly: Boolean = PartitionConfig.DefaultShardingConnectDirectly,
    shardedConnectToMongos: Boolean = PartitionConfig.DefaultShardingConnectToMongos
) extends CollectionConfig {

  require(maxChunkSize > 0, s"maxChunkSize ($maxChunkSize) must be greater than 0")

  /**
   * Creates a new `PartitionConfig` with the options applied
   *
   * *Note:* The `PartitionConfig` options should not have the "mongodb.partition." property prefix
   *
   * @param options a map of options to be applied to the `PartitionConfig`
   * @return an updated `PartitionConfig`
   *
   */
  def withOptions(options: scala.collection.Map[String, String]): PartitionConfig = {
    PartitionConfig(
      databaseName = options.getOrElse("databaseName", databaseName),
      collectionName = options.getOrElse("collectionName", collectionName),
      maxChunkSize = options.get("maxChunkSize") match {
        case Some(size) => size.toInt
        case None       => maxChunkSize
      },
      splitKey = options.getOrElse("splitKey", splitKey),
      shardedConnectDirectly = options.get("shardedConnectDirectly") match {
        case Some(connectDirect) => connectDirect.toBoolean
        case None                => shardedConnectDirectly
      },
      shardedConnectToMongos = options.get("shardedConnectToMongos") match {
        case Some(connectToMongos) => connectToMongos.toBoolean
        case None                  => shardedConnectToMongos
      }
    )
  }

  /**
   * Creates a map of options representing the  `PartitionConfig`
   *
   * @return the map representing the `PartitionConfig`
   */
  def asOptions: Map[String, String] = Map("databaseName" -> databaseName, "collectionName" -> collectionName,
    "maxChunkSize" -> maxChunkSize.toString, "splitKey" -> splitKey, "shardedConnectDirectly" -> shardedConnectDirectly.toString,
    "shardedConnectToMongos" -> shardedConnectToMongos.toString)

  /**
   * Creates a new `PartitionConfig` with the options applied
   *
   * *Note:* The `PartitionConfig` options should not have the "mongodb.partition." property prefix
   *
   * @param options a map of options to be applied to the `PartitionConfig`
   * @return an updated `PartitionConfig`
   */
  def withJavaOptions(options: java.util.Map[String, String]): PartitionConfig = withOptions(options.asScala)

  /**
   * Creates a map of options representing the  `PartitionConfig`
   *
   * @return the map representing the `PartitionConfig`
   */
  def asJavaOptions: java.util.Map[String, String] = asOptions.asJava
}
