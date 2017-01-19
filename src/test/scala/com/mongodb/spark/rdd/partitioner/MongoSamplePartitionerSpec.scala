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

import org.bson._
import com.mongodb.spark.{MongoConnector, RequiresMongoDB}

class MongoSamplePartitionerSpec extends RequiresMongoDB {

  private val partitionKey = "_id"
  private val pipeline = Array.empty[BsonDocument]

  // scalastyle:off magic.number
  "MongoSamplePartitioner" should "partition the database as expected" in {
    if (!serverAtLeast(3, 2)) cancel("MongoDB < 3.2")
    loadSampleData(10)

    val rightHandBoundaries = (1 to 100 by 10).map(x => new BsonString(f"$x%05d"))
    val locations = PartitionerHelper.locations(MongoConnector(sparkConf))
    val expectedPartitions = PartitionerHelper.createPartitions(partitionKey, rightHandBoundaries, locations)
    val partitions = MongoSamplePartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("partitionSizeMB" -> "1")), pipeline
    )

    partitions should equal(expectedPartitions)

    val singlePartition = PartitionerHelper.createPartitions(partitionKey, Seq.empty[BsonValue], locations)
    val largerSizedPartitions = MongoSamplePartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("partitionSizeMB" -> "10")), pipeline
    )
    largerSizedPartitions should equal(singlePartition)
  }
  // scalastyle:on magic.number

  it should "have a default bounds of min to max key" in {
    if (!serverAtLeast(3, 2)) cancel("MongoDB < 3.2")
    collection.insertOne(new Document())
    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoSamplePartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }

  it should "handle no collection" in {
    if (!serverAtLeast(3, 2)) cancel("MongoDB < 3.2")
    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoSamplePartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }

  it should "handle an empty collection" in {
    if (!serverAtLeast(3, 2)) cancel("MongoDB < 3.2")
    collection.insertOne(new Document())
    collection.deleteMany(new Document())
    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoSamplePartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }
}
