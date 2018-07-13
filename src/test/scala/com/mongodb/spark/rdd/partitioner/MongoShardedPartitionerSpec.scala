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

import scala.collection.JavaConverters._
import org.bson.{BsonDocument, Document}
import com.mongodb.spark.{MongoConnector, RequiresMongoDB}

import org.scalatest.prop.PropertyChecks

class MongoShardedPartitionerSpec extends RequiresMongoDB with PropertyChecks {
  private val pipeline = Array.empty[BsonDocument]

  "MongoShardedPartitioner" should "partition the database as expected" in {
    if (!isSharded) cancel("Not a Sharded MongoDB")
    loadSampleDataIntoShardedCollection(5) // scalastyle:ignore

    val locations = PartitionerHelper.locations(MongoConnector(sparkConf))
    val partitions = MongoShardedPartitioner.partitions(mongoConnector, readConfig, pipeline)
    val zipped: Array[(MongoPartition, MongoPartition)] = partitions zip partitions.tail
    zipped.foreach({
      case (lt, gte) => lt.queryBounds.getDocument("_id").get("$lt") should equal(gte.queryBounds.getDocument("_id").get("$gte"))
    })
    partitions.head.locations should equal(locations)
  }

  it should "partition the database as expected with compound shard keys" in {
    if (!isSharded) cancel("Not a Sharded MongoDB")
    loadSampleDataIntoShardedCollection(5) // scalastyle:ignore

    val shardKeys = BsonDocument.parse("{_id: 1, pk: 1}")
    val shardKeyFields = shardKeys.keySet().asScala
    val locations = PartitionerHelper.locations(MongoConnector(sparkConf))
    val partitions = MongoShardedPartitioner.partitions(mongoConnector, readConfig.withOption("partitionerOptions.shardKey", shardKeys.toJson), pipeline)
    val zipped: Array[(MongoPartition, MongoPartition)] = partitions zip partitions.tail
    zipped.foreach({
      case (lt, gte) => {
        shardKeyFields.foreach { k => lt.queryBounds.getDocument(k).get("$lt") should equal(gte.queryBounds.getDocument(k).get("$gte")) }
      }
    })
    partitions.head.locations should equal(locations)
  }

  it should "have a default bounds of min to max key" in {
    if (!isSharded) cancel("Not a Sharded MongoDB")
    shardCollection()
    collection.insertOne(new Document())

    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoShardedPartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }

  it should "handle no  collection" in {
    if (!isSharded) cancel("Not a Sharded MongoDB")
    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoShardedPartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }

  it should "handle an empty collection" in {
    if (!isSharded) cancel("Not a Sharded MongoDB")
    shardCollection()
    collection.insertOne(new Document())
    collection.deleteMany(new Document())
    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoShardedPartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
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
          BsonDocument.parse("""{_id: {$lt: { "$numberLong" : "0" } } } }"""),
          shardMap.getOrElse("tic", Nil)
        ),
        MongoPartition(
          1,
          BsonDocument.parse("""{_id: {$gte: { "$numberLong" : "0" }, $lt: { "$numberLong" : "500000000000" } } } }"""),
          shardMap.getOrElse("tac", Nil)
        ),
        MongoPartition(
          2,
          BsonDocument.parse("""{_id: {$gte: { "$numberLong" : "500000000000" } } } }"""),
          shardMap.getOrElse("toe", Nil)
        )
      )
      MongoShardedPartitioner.generatePartitions(chunks, "_id", shardMap) should be(expectedPartitions)
    }
  }

  it should "calculate the expected Partitions for compound shardKey" in {
    forAll(shardsMaps) { (shardMap: Map[String, Seq[String]]) =>
      val shardKey = "{ shardKey1: 1, shardKey2: 1 }"
      val chunks: Seq[BsonDocument] =
        """ { _id: "a", shard: "tic", min: { shardKey1: { "$minKey" : 1 }, shardKey2: { "$minKey": 1} }, max: { shardKey1: 0, shardKey2: 10 } }
          |{ _id: "b", shard: "tac", min: { shardKey1: 0, shardKey2: 10 } , max: { shardKey1: 5000000, shardKey2: 6000000 } } }
          |{ _id: "c", shard: "toe", min: { shardKey1: 5000000, shardKey2: 6000000 }, "max" : { shardKey1: { "$maxKey" : 1 }, shardKey2: { "$maxKey": 1} } }
        """.trim.stripMargin.split("[\\r\\n]+").map(BsonDocument.parse)

      val expectedPartitions = Array(
        MongoPartition(
          0,
          BsonDocument.parse("""{shardKey1: { $lt: 0 }, shardKey2: { $lt: 10 } }"""),
          shardMap.getOrElse("tic", Nil)
        ),
        MongoPartition(
          1,
          BsonDocument.parse("""{shardKey1: { $gte: 0, $lt: 5000000 }, shardKey2: { $gte: 10, $lt: 6000000 } }"""),
          shardMap.getOrElse("tac", Nil)
        ),
        MongoPartition(
          2,
          BsonDocument.parse("""{shardKey1: { $gte: 5000000 }, shardKey2: {$gte: 6000000 } }"""),
          shardMap.getOrElse("toe", Nil)
        )
      )
      MongoShardedPartitioner.generatePartitions(chunks, shardKey, shardMap) should be(expectedPartitions)
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
