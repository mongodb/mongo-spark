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
import scala.util.Random

import org.bson._
import com.mongodb.spark.exceptions.MongoPartitionerException
import com.mongodb.spark.{MongoConnector, RequiresMongoDB}

class MongoPaginateBySizePartitionerSpec extends RequiresMongoDB {

  private val partitionKey = "_id"
  private val pipeline = Array.empty[BsonDocument]

  // scalastyle:off magic.number
  "MongoPaginateBySizePartitioner" should "partition the database as expected" in {
    if (!serverAtLeast(3, 2)) cancel("Testing on wiretiger only, so to have predictable partition sizes.")
    loadSampleData(10)

    val rightHandBoundaries = (1 to 100 by 10).map(x => new BsonString(f"$x%05d"))
    val locations = PartitionerHelper.locations(MongoConnector(sparkConf))
    val expectedPartitions = PartitionerHelper.createPartitions(partitionKey, rightHandBoundaries, locations)
    val partitions = MongoPaginateBySizePartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("partitionSizeMB" -> "1")), pipeline
    )

    partitions should equal(expectedPartitions)

    val singlePartition = PartitionerHelper.createPartitions(partitionKey, Seq.empty[BsonValue], locations)
    val largerSizedPartitions = MongoPaginateBySizePartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("partitionSizeMB" -> "10")), pipeline
    )
    largerSizedPartitions should equal(singlePartition)
  }
  // scalastyle:on magic.number

  it should "have a default bounds of min to max key" in {
    collection.insertOne(new Document())
    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoPaginateBySizePartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }

  it should "handle no collection" in {
    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoPaginateBySizePartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }

  it should "handle an empty collection" in {
    collection.insertOne(new Document())
    collection.deleteMany(new Document())
    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoPaginateBySizePartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }

  it should "throw a partitioner error if duplicate partitions are found" in {
    val sampleString: String = Random.alphanumeric.take(1000 * 1000).mkString
    collection.insertMany((1 to 100).map(i => new Document("a", 1).append("s", sampleString)).toList.asJava)
    a[MongoPartitionerException] should be thrownBy MongoPaginateBySizePartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("partitionKey" -> "a", "partitionSizeMB" -> "1")), pipeline
    )
  }
}

