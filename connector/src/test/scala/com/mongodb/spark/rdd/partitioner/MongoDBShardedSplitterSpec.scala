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

import org.bson.Document
import org.bson.types.{MaxKey, MinKey}
import com.mongodb.spark.RequiresMongoDB

class MongoDBShardedSplitterSpec extends FlatSpec with RequiresMongoDB {

  "MongoDBStandaloneSplitter" should "split the database as expected" in {
    if (!isSharded) cancel("Not a Sharded MongoDB")
    loadSampleDataIntoShardedCollection(collectionName, 5)

    MongoShardedSplitter(mongoConnector, "_id").bounds().size should (be >= 5 and be <= 10)
  }

  it should "have a default bounds of min to max key" in {
    if (!isSharded) cancel("Not a Sharded MongoDB")
    shardCollection(collectionName)
    collection.insertOne(new Document())

    val expectedBounds: Document = new Document("_id", new Document("$gte", new MinKey).append("$lt", new MaxKey))
    MongoShardedSplitter(mongoConnector, "_id").bounds() should contain theSameElementsAs Seq(expectedBounds)
  }
}
