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

package com.mongodb.spark

import org.scalatest.FlatSpec

import org.bson.{BsonDocument, Document}
import com.mongodb.client.model.{Aggregates, Filters}
import com.mongodb.spark.rdd.MongoRDD

class MongoRDDSpec extends FlatSpec with RequiresMongoDB {

  "MongoRDD" should "be easily created from the SparkContext" in withSparkContext() { sc =>
    sc.parallelize(Seq(Document.parse("{counter: 0}"), Document.parse("{counter: 1}"), Document.parse("{counter: 2}"))).saveToMongoDB()
    val mongoRDD: MongoRDD[Document] = sc.loadFromMongoDB()

    mongoRDD.count() shouldBe 3
    mongoRDD.map(x => x.getInteger("counter")).collect() should contain theSameElementsInOrderAs Seq(0, 1, 2)
  }

  it should "be able to handle non existent collections" in withSparkContext() { sc =>
    sc.loadFromMongoDB().count() shouldBe 0
  }

  it should "be able to query via a pipeline" in withSparkContext() { sc =>
    sc.parallelize(Seq(Document.parse("{counter: 0}"), Document.parse("{counter: 1}"), Document.parse("{counter: 2}"))).saveToMongoDB()

    sc.loadFromMongoDB().withPipeline(List(Document.parse("{$match: { counter: {$gt: 0}}}"))).count() shouldBe 2
    sc.loadFromMongoDB().withPipeline(List(BsonDocument.parse("{$match: { counter: {$gt: 0}}}"))).count() shouldBe 2
    sc.loadFromMongoDB().withPipeline(List(Aggregates.`match`(Filters.gt("counter", 0)))).count() shouldBe 2
  }

  it should "be able to handle different collection types" in withSparkContext() { sc =>
    sc.parallelize(Seq(BsonDocument.parse("{counter: 0}"), BsonDocument.parse("{counter: 1}"), BsonDocument.parse("{counter: 2}"))).saveToMongoDB()

    val mongoRDD: MongoRDD[BsonDocument] = sc.loadFromMongoDB[BsonDocument]()
    mongoRDD.count() shouldBe 3
  }
}
