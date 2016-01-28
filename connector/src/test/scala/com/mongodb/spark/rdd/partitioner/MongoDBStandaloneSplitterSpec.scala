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

import org.bson.{BsonMaxKey, BsonMinKey, BsonDocument, Document}
import org.bson.types.{MaxKey, MinKey}
import com.mongodb.spark.RequiresMongoDB
import com.mongodb.spark.conf.ReadConfig

class MongoDBStandaloneSplitterSpec extends FlatSpec with RequiresMongoDB {

  // scalastyle:off magic.number
  "MongoDBStandaloneSplitter" should "split the database as expected" in {
    if (isSharded) cancel("Sharded MongoDB")
    loadSampleData(5)

    MongoStandaloneSplitter(mongoConnector, readConfig.copy(maxChunkSize = 4)).bounds().size shouldBe 3
    MongoStandaloneSplitter(mongoConnector, readConfig.copy(maxChunkSize = 5)).bounds().size shouldBe 1
  }
  // scalastyle:on magic.number

  it should "have a default bounds of min to max key" in {
    collection.insertOne(new Document())

    val expectedBounds: BsonDocument = new BsonDocument("_id", new BsonDocument("$gte", new BsonMinKey).append("$lt", new BsonMaxKey))
    MongoStandaloneSplitter(mongoConnector, readConfig.copy(maxChunkSize = 1)).bounds() should contain theSameElementsAs Seq(expectedBounds)
  }
}
