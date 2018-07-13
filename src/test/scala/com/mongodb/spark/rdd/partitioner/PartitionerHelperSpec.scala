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

import org.bson.{BsonDocument, BsonInt32}
import com.mongodb.spark.RequiresMongoDB

class PartitionerHelperSpec extends RequiresMongoDB {

  "PartitionerHelper" should "create the expected partitions query" in {
    val query = PartitionerHelper.createBoundaryQuery("_id", new BsonInt32(1), new BsonInt32(2))
    query should equal(BsonDocument.parse("{_id: {$gte: 1, $lt: 2 }}"))
  }

  it should "create the correct partitions" in {
    val partitions = PartitionerHelper.createPartitions("_id", Seq(new BsonInt32(10), new BsonInt32(20), new BsonInt32(30)), Seq("localhost")) // scalastyle:ignore
    val expectedPartitions = Array(
      MongoPartition(0, BsonDocument.parse("{_id: {$lt: 10 }}"), Seq("localhost")),
      MongoPartition(1, BsonDocument.parse("{_id: {$gte: 10, $lt: 20 }}"), Seq("localhost")),
      MongoPartition(2, BsonDocument.parse("{_id: {$gte: 20, $lt: 30 }}"), Seq("localhost")),
      MongoPartition(3, BsonDocument.parse("{_id: {$gte: 30 }}"), Seq("localhost"))
    )
    partitions should be(expectedPartitions)
  }

  it should "create a single partition when addMinMax = false" in {
    val partitions = PartitionerHelper.createPartitions("_id", Seq(new BsonInt32(5)), Seq("localhost"), addMinMax = false) // scalastyle:ignore
    val expectedPartitions = Array(
      MongoPartition(0, BsonDocument.parse("{_id: {$gte: 5, $lt: 5 }}"), Seq("localhost"))
    )
    partitions should be(expectedPartitions)
  }
}
