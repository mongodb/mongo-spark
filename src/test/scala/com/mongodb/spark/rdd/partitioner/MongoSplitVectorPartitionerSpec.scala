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
import com.mongodb.spark.RequiresMongoDB

class MongoSplitVectorPartitionerSpec extends FlatSpec with RequiresMongoDB {

  // scalastyle:off magic.number
  "MongoSplitVectorPartitioner" should "partition the database as expected" in {
    if (isSharded) cancel("Sharded MongoDB")
    loadSampleData(5)

    MongoSplitVectorPartitioner.partitions(mongoConnector, readConfig.copy(maxChunkSize = 3)).length should be >= 2
    MongoSplitVectorPartitioner.partitions(mongoConnector, readConfig.copy(maxChunkSize = 7)).length shouldBe 1
  }
  // scalastyle:on magic.number

  it should "have a default bounds of min to max key" in {
    val expectedBounds: BsonDocument = new BsonDocument(readConfig.splitKey, new BsonDocument("$gte", new BsonMinKey).append("$lt", new BsonMaxKey))
    collection.insertOne(new Document())

    MongoSplitVectorPartitioner.partitions(mongoConnector, readConfig)(0).queryBounds should equal(expectedBounds)
  }
}
