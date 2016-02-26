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

import org.scalatest.{FlatSpec, Matchers}

import org.apache.spark.SparkConf

class PartitionConfigSpec extends FlatSpec with Matchers {

  "PartitionConfig" should "have the expected defaults" in {
    val partitionConfig = PartitionConfig("db", "collection")
    val expectedPartitionConfig = PartitionConfig("db", "collection", 64, "_id", shardedConnectDirectly = false, shardedConnectToMongos = true) // scalastyle:ignore

    partitionConfig should equal(expectedPartitionConfig)
  }

  it should "be creatable from SparkConfig" in {
    val expectedPartitionConfig = PartitionConfig("db", "collection", 1, "_id", shardedConnectDirectly = true, shardedConnectToMongos = false) // scalastyle:ignore

    PartitionConfig(sparkConf) should equal(expectedPartitionConfig)
  }

  it should "be able mixin user parameters" in {
    val expectedPartitionConfig = PartitionConfig("db", "collection", 10, "ID", shardedConnectDirectly = false, shardedConnectToMongos = true) // scalastyle:ignore

    PartitionConfig(sparkConf).withOptions(
      Map(
        "maxChunkSize" -> "10",
        "splitKey" -> "ID",
        "sampleSize" -> "200",
        "shardedConnectDirectly" -> "false",
        "shardedConnectToMongos" -> "true"
      )
    ) should equal(expectedPartitionConfig)
  }

  it should "be able to create a map" in {
    val partitionConfig = PartitionConfig("dbName", "collName", 10, "ID", shardedConnectDirectly = true, shardedConnectToMongos = false) // scalastyle:ignore
    val expectedPartitionConfigMap = Map(
      "databaseName" -> "dbName",
      "collectionName" -> "collName",
      "maxChunkSize" -> "10",
      "splitKey" -> "ID",
      "shardedConnectDirectly" -> "true",
      "shardedConnectToMongos" -> "false"
    )

    partitionConfig.asOptions should equal(expectedPartitionConfigMap)
  }

  it should "validate the values" in {
    an[IllegalArgumentException] should be thrownBy PartitionConfig(new SparkConf().set("mongodb.input.collectionName", "coll"))
    an[IllegalArgumentException] should be thrownBy PartitionConfig(new SparkConf().set("mongodb.input.databaseName", "db"))
    an[IllegalArgumentException] should be thrownBy PartitionConfig("db", "collection", maxChunkSize = -1)
  }

  val sparkConf = new SparkConf()
    .set("mongodb.input.databaseName", "db")
    .set("mongodb.input.collectionName", "collection")
    .set("mongodb.partition.maxChunkSize", "1")
    .set("mongodb.partition.splitKey", "_id")
    .set("mongodb.partition.shardedConnectDirectly", "true")
    .set("mongodb.partition.shardedConnectToMongosOnly", "false")

}
