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

import org.bson._
import com.mongodb.spark._
import com.mongodb.spark.exceptions.MongoPartitionerException

class MongoPaginateByCountPartitionerSpec extends RequiresMongoDB {

  private val partitionKey = "_id"
  private val pipeline = Array.empty[BsonDocument]

  "MongoPaginateByCountPartitioner" should "partition the database as expected" in withSparkContext() { sc =>
    sc.parallelize((0 to 1000).map(i => BsonDocument.parse(s"{ _id: $i }"))).saveToMongoDB()

    val rightHandBoundaries = (0 to 1000 by 100).map(x => new BsonInt32(x))
    val locations = PartitionerHelper.locations(MongoConnector(sparkConf))
    val expectedPartitions = PartitionerHelper.createPartitions(partitionKey, rightHandBoundaries, locations)
    val partitions = MongoPaginateByCountPartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("numberOfPartitions" -> "10")), pipeline
    )

    partitions should equal(expectedPartitions)
  }

  it should "handle fewer documents than partitions" in withSparkContext() { sc =>
    sc.parallelize((0 to 10).map(i => BsonDocument.parse(s"{ _id: $i }"))).saveToMongoDB()

    val rightHandBoundaries = (0 to 10).map(x => new BsonInt32(x))
    val locations = PartitionerHelper.locations(MongoConnector(sparkConf))
    val expectedPartitions = PartitionerHelper.createPartitions(partitionKey, rightHandBoundaries, locations)
    val partitions = MongoPaginateByCountPartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("numberOfPartitions" -> "100")), pipeline
    )
    partitions should equal(expectedPartitions)
  }

  it should "handle no collection" in {
    val expectedPartitions = MongoPaginateByCountPartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoPaginateByCountPartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }

  it should "throw a partitioner error if duplicate partitions are found" in {
    collection.insertMany((1 to 100).map(i => new Document("a", 1)).toList.asJava)
    a[MongoPartitionerException] should be thrownBy MongoPaginateByCountPartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("partitionKey" -> "a")), pipeline
    )
  }
}
