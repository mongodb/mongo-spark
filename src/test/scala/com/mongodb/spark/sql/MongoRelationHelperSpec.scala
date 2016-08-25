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

package com.mongodb.spark.sql

import org.apache.spark.sql.sources._

import org.bson.BsonDocument
import org.bson.conversions.Bson
import com.mongodb.MongoClient
import com.mongodb.spark.RequiresMongoDB

import org.scalatest.prop.PropertyChecks

class MongoRelationHelperSpec extends RequiresMongoDB with PropertyChecks {
  // scalastyle:off magic.number
  "createPipeline" should "create an empty pipeline if no projection or filters" in {
    MongoRelationHelper.createPipeline(Array.empty[String], Array.empty[Filter]) shouldBe empty
  }

  it should "project the required fields" in {
    MongoRelationHelper.createPipeline(Array("_id", "myField"), Array.empty[Filter]).toBson should
      equal(List("""{$project: {_id: 1, myField: 1}}""".toBson))
  }

  it should "explicitly exclude _id from the projection if not required" in {
    MongoRelationHelper.createPipeline(Array("myId", "myField"), Array.empty[Filter]).toBson should
      equal(List("""{$project: {myId: 1, myField: 1, _id: 0}}""".toBson))
  }

  it should "handle spark Filters" in {
    forAll(filters) { (filter: Filter, expected: String) =>
      MongoRelationHelper.createPipeline(Array.empty[String], Array(filter)).toBson should equal(Seq(expected.toBson))
    }
  }

  it should "and multiple spark Filters" in {
    MongoRelationHelper.createPipeline(Array.empty[String], Array(GreaterThan("f", 5), LessThan("f", 10))).toBson should
      equal(Seq("{$match: {f: {$gt: 5, $lt: 10}}}".toBson))
  }

  val filters = Table(
    ("filter", "expected"),
    (EqualTo("f", 1), "{$match: {f: 1}}"),
    (EqualNullSafe("f", 1), "{$match: {f: 1}}"),
    (GreaterThan("f", 1), "{$match: {f: {$gt: 1}}}"),
    (GreaterThanOrEqual("f", 1), "{$match: {f: {$gte: 1}}}"),
    (LessThan("f", 1), "{$match: {f: {$lt: 1}}}"),
    (LessThanOrEqual("f", 1), "{$match: {f: {$lte: 1}}}"),
    (In("f", Array(1, 2, 3)), "{$match: {f: {$in: [1,2,3]}}}"),
    (IsNull("f"), "{$match: {f: null}}"),
    (IsNotNull("f"), "{$match: {f: {$exists: true, $ne: null}}}"),
    (StringStartsWith("f", "A"), """{$match: {f:  {$regex: "^A", $options: ""}}}"""),
    (StringEndsWith("f", "A"), """{$match: {f:  {$regex: "A$", $options: ""}}}"""),
    (StringContains("f", "A"), """{$match: {f:  {$regex: "A", $options: ""}}}"""),
    (Not(EqualTo("f", 1)), "{$match: {f: {$not: {$eq: 1}}}}"),
    (And(GreaterThan("f", 5), LessThan("f", 10)), "{$match: {f: {$gt: 5, $lt: 10}}}"),
    (Or(EqualTo("f", 1), EqualTo("f", 2)), "{$match: {$or: [{f: 1}, {f: 2}]}}")
  )

  implicit class PipelineHelpers(val pipeline: Seq[Bson]) {
    def toBson: Seq[BsonDocument] =
      pipeline.map(_.toBsonDocument(classOf[BsonDocument], MongoClient.getDefaultCodecRegistry))
  }

  implicit class JsonHelpers(val json: String) {
    def toBson: BsonDocument = BsonDocument.parse(json)
  }

  // scalastyle:on magic.number
}
