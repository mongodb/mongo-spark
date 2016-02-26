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

package com.mongodb.spark.rdd.partitioner

import org.scalatest.FlatSpec

import org.bson.{BsonDocument, BsonMaxKey, BsonMinKey, Document}
import com.mongodb.ServerAddress
import com.mongodb.spark.RequiresMongoDB

class MongoShardedPartitionerSpec extends FlatSpec with RequiresMongoDB {

  "MongoShardedPartitioner" should "partition the database as expected" in {
    if (!isSharded) cancel("Not a Sharded MongoDB")
    loadSampleDataIntoShardedCollection(5) // scalastyle:ignore

    MongoShardedPartitioner.partitions(mongoConnector, partitionConfig).length should (be >= 5 and be <= 10)
  }

  it should "fallback to a single partition for a non sharded collections" in {
    if (!isSharded) cancel("Not a Sharded MongoDB")
    loadSampleData(5) // scalastyle:ignore

    MongoShardedPartitioner.partitions(mongoConnector, partitionConfig).length should equal(1)
  }

  it should "have a default bounds of min to max key" in {
    if (!isSharded) cancel("Not a Sharded MongoDB")
    val expectedBounds: BsonDocument = new BsonDocument(partitionConfig.splitKey, new BsonDocument("$gte", new BsonMinKey).append("$lt", new BsonMaxKey))
    shardCollection()
    collection.insertOne(new Document())

    MongoShardedPartitioner.partitions(mongoConnector, partitionConfig)(0).queryBounds should equal(expectedBounds)
  }

  it should "connect directly to the mongos if shardedConnectDirectly and shardedConnectToMongos" in {
    val preferredLocations = MongoShardedPartitioner.getPreferredLocations(shardedConnectDirectly = true, shardedConnectToMongos = true,
      shardsMap, mongosMap, "shard0001")
    preferredLocations should contain theSameElementsInOrderAs Seq(new ServerAddress("sh1.example.com:27020"))
  }

  it should "connect directly to the shard if shardedConnectDirectly and not shardedConnectToMongos" in {
    val preferredLocations = MongoShardedPartitioner.getPreferredLocations(shardedConnectDirectly = true, shardedConnectToMongos = false,
      shardsMap, mongosMap, "shard0001")
    preferredLocations should contain theSameElementsInOrderAs Seq(new ServerAddress("sh1.example.com:27017"))
  }

  it should "not have a preferred location if not shardedConnectDirectly" in {
    val preferredLocations = MongoShardedPartitioner.getPreferredLocations(shardedConnectDirectly = false, shardedConnectToMongos = false,
      shardsMap, mongosMap, "shard0001")
    preferredLocations should equal(Nil)
  }

  it should "prefer any mongos if shardedConnectDirectly and shardedConnectToMongos but mongos host not found" in {
    val preferredLocations = MongoShardedPartitioner.getPreferredLocations(shardedConnectDirectly = true, shardedConnectToMongos = true,
      shardsMap, mongosMap, "shard0003")
    preferredLocations should contain theSameElementsInOrderAs mongosMap.values.map(new ServerAddress(_)).toSeq
  }

  val shardsMap = Map("shard0000" -> "sh0.example.com:27017", "shard0001" -> "sh1.example.com:27017")
  val mongosMap = Map("sh0.example.com" -> "sh0.example.com:27020", "sh1.example.com" -> "sh1.example.com:27020")

}
