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

import org.bson.{BsonDocument, Document}
import com.mongodb.spark.{MongoConnector, RequiresMongoDB}

class MongoSplitVectorPartitionerSpec extends RequiresMongoDB {

  private val partitionKey = "_id"
  private val pipeline = Array.empty[BsonDocument]

  // scalastyle:off magic.number
  "MongoSplitVectorPartitioner" should "partition the database as expected" in {
    if (isSharded) cancel("Sharded MongoDB")
    loadSampleData(5)

    val locations = PartitionerHelper.locations(MongoConnector(sparkConf))
    val partitions = MongoSplitVectorPartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("partitionSizeMB" -> "1")), pipeline
    )
    val zipped: Array[(MongoPartition, MongoPartition)] = partitions zip partitions.tail
    zipped.foreach({
      case (lt, gte) => lt.queryBounds.get("lt") should equal(gte.queryBounds.get("gte"))
    })
    partitions.length should be >= 5
    partitions.head.locations should equal(locations)

    MongoSplitVectorPartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("partitionSizeMB" -> "10")), pipeline
    ).length shouldBe 1
  }
  // scalastyle:on magic.number

  it should "have a default bounds of min to max key" in {
    collection.insertOne(new Document())
    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoSplitVectorPartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }

  it should "handle no collection" in {
    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoSplitVectorPartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }

  it should "handle an empty collection" in {
    collection.insertOne(new Document())
    collection.deleteMany(new Document())
    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoSplitVectorPartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }
}
