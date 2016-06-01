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

import org.bson.{BsonDocument, BsonMaxKey, BsonMinKey, Document}
import com.mongodb.MongoException
import com.mongodb.spark.RequiresMongoDB

import org.scalatest.prop.PropertyChecks

class MongoShardedPartitionerSpec extends RequiresMongoDB with PropertyChecks {

  "MongoShardedPartitioner" should "partition the database as expected" in {
    if (!isSharded) cancel("Not a Sharded MongoDB")
    loadSampleDataIntoShardedCollection(5) // scalastyle:ignore

    val partitions = MongoShardedPartitioner.partitions(mongoConnector, readConfig)
    partitions.length should be >= 2
    partitions.head.locations should not be empty
  }

  it should "fallback to a single partition for a non sharded collections" in {
    if (!isSharded) cancel("Not a Sharded MongoDB")
    loadSampleData(5) // scalastyle:ignore

    MongoShardedPartitioner.partitions(mongoConnector, readConfig).length should equal(1)
  }

  it should "have a default bounds of min to max key" in {
    if (!isSharded) cancel("Not a Sharded MongoDB")
    val expectedBounds: BsonDocument = new BsonDocument(readConfig.splitKey, new BsonDocument("$gte", new BsonMinKey).append("$lt", new BsonMaxKey))
    shardCollection()
    collection.insertOne(new Document())

    MongoShardedPartitioner.partitions(mongoConnector, readConfig)(0).queryBounds should equal(expectedBounds)
  }

  it should "calculate the expected hosts for a single node shard" in {
    MongoShardedPartitioner.getHosts("sh0.example.com:27018") should contain theSameElementsInOrderAs Seq("sh0.example.com")
  }

  it should "calculate the expected hosts for a multi node shard" in {
    val hosts = MongoShardedPartitioner.getHosts("tic/sh0.rs1.example.com:27018,sh0.rs2.example.com:27018,sh0.rs3.example.com:27018")
    hosts should contain theSameElementsInOrderAs Seq("sh0.rs1.example.com", "sh0.rs2.example.com", "sh0.rs3.example.com")
  }

  it should "return distinct hosts" in {
    val hosts = MongoShardedPartitioner.getHosts("tic/sh0.example.com:27018,sh0.example.com:27019,sh0.example.com:27020")
    hosts should contain theSameElementsInOrderAs Seq("sh0.example.com")
  }

  it should "throw an error with invalid hosts" in {
    an[MongoException] should be thrownBy MongoShardedPartitioner.getHosts("alpha::12")
    an[MongoException] should be thrownBy MongoShardedPartitioner.getHosts("alpha:brava")
  }

  it should "calculate the expected Partitions" in {
    forAll(shardsMaps) { (shardMap: Map[String, Seq[String]]) =>

      val chunks: Seq[BsonDocument] =
        """ { _id: "a", shard: "tic", min: { _id: { "$minKey" : 1 } }, max: { _id: { "$numberLong" : "0" } } }
        |{ _id: "b", shard: "tac", min: { _id: { "$numberLong" : "0" } } , max: { _id : { "$numberLong" : "500000000000" } } }
        |{ _id: "c", shard: "toe", min: { _id: { "$numberLong" : "500000000000" } }, "max" : { "_id" : { "$maxKey" : 1 } } }
    """.trim.stripMargin.split("[\\r\\n]+").map(BsonDocument.parse)

      val expectedPartitions = Array(
        MongoPartition(
          0,
          BsonDocument.parse("""{_id: {$gte: { "$minKey" : 1 }, $lt: { "$numberLong" : "0" } } } }"""),
          shardMap.getOrElse("tic", Nil)
        ),
        MongoPartition(
          1,
          BsonDocument.parse("""{_id: {$gte: { "$numberLong" : "0" }, $lt: { "$numberLong" : "500000000000" } } } }"""),
          shardMap.getOrElse("tac", Nil)
        ),
        MongoPartition(
          2,
          BsonDocument.parse("""{_id: {$gte: { "$numberLong" : "500000000000" }, $lt: { "$maxKey" : 1  } } } }"""),
          shardMap.getOrElse("toe", Nil)
        )
      )
      MongoShardedPartitioner.generatePartitions(chunks, "_id", shardMap) should be(expectedPartitions)
    }

  }

  val shardsMaps = Table(
    "shardMaps",
    Map.empty[String, Seq[String]],
    Map("tic" -> Seq("tic.example.com"), "tac" -> Seq("tac.example.com"), "toe" -> Seq("toe.example.com")),
    Map(
      "tic" -> Seq("tic.rs1.example.com", "tic.rs2.example.com", "tic.rs3.example.com"),
      "tac" -> Seq("tac.rs1.example.com", "tac.rs2.example.com", "tac.rs3.example.com"),
      "toe" -> Seq("toe.rs1.example.com", "toe.rs2.example.com", "toe.rs3.example.com")
    )
  )

}
