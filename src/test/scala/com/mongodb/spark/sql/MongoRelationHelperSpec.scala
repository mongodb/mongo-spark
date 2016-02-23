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

import scala.reflect.runtime.universe._

import org.scalatest.FlatSpec
import org.scalatest.prop.PropertyChecks

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import org.bson.conversions.Bson
import org.bson.{BsonDocument, Document}
import com.mongodb.MongoClient
import com.mongodb.spark.RequiresMongoDB

class MongoRelationHelperSpec extends FlatSpec with PropertyChecks with RequiresMongoDB {
  // scalastyle:off magic.number null
  case class Person(name: String, age: Int)
  case class Family(familyName: String, members: List[Person])
  case class NestedFamily(familyName: String, members: List[List[Person]])

  def schemaFor[T <: Product: TypeTag]: StructType = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]

  "documentToRow" should "convert a Document into a Row with the given schema" in {
    val schema: StructType = schemaFor[Person]
    val document: Document = Document.parse("{name: 'John', age: 18}")

    val row: Row = MongoRelationHelper.documentToRow(document, schema)
    row.toSeq should equal(Array("John", 18))
    row.schema should equal(schema)
  }

  it should "not prune the schema when using given a document with missing values" in {
    val schema: StructType = schemaFor[Person]
    val document: Document = Document.parse("{name: 'John'}")

    val row: Row = MongoRelationHelper.documentToRow(document, schema)
    row.toSeq should equal(Array("John", null))
    row.schema should equal(schema)
  }

  it should "prune the schema when limited by passed required columns" in {
    val schema: StructType = schemaFor[Person]
    val document: Document = Document.parse("{name: 'John', age: 18}")

    val row: Row = MongoRelationHelper.documentToRow(document, schema, Array("age"))
    row.toSeq should equal(Array(18))
    row.schema should equal(DataTypes.createStructType(Array(schema.fields(1))))
  }

  it should "ignore any extra data in the document that is not included in the schema" in {
    val schema: StructType = schemaFor[Person]
    val document: Document = Document.parse("{name: 'John', age: 18, height: 192}")

    val row: Row = MongoRelationHelper.documentToRow(document, schema)
    row.toSeq should equal(Array("John", 18))
    row.schema should equal(schema)
  }

  it should "handle nested schemas" in {
    val schema: StructType = schemaFor[Family]
    val document: Document = Document.parse(
      """{familyName: "Smith", members:[
        |{name: "James", age: 48},
        |{name: "Jane", age: 42},
        |{name: 'Jeremy', age: 18},
        |{name: 'John', age: 18}]}""".stripMargin
    )

    val row: Row = MongoRelationHelper.documentToRow(document, schema)
    row.getAs[String]("familyName") should equal("Smith")
    row.getSeq[Row](1).map(_.toSeq) should equal(Array(Array("James", 48), Array("Jane", 42), Array("Jeremy", 18), Array("John", 18)))
    row.schema should equal(schema)
  }

  "rowToDocument" should "convert a Row into a Document" in {
    val schema: StructType = schemaFor[Person]
    val document: Document = Document.parse("{name: 'John', age: 18}")

    val row: Row = MongoRelationHelper.documentToRow(document, schema)
    val rowToDocument: Document = MongoRelationHelper.rowToDocument(row)

    rowToDocument should equal(document)
  }

  it should "handle nested schemas" in {
    val schema: StructType = schemaFor[Family]
    val document: Document = Document.parse(
      """{familyName: "Smith", members:[
        |{name: "James", age: 48},
        |{name: "Jane", age: 42},
        |{name: 'Jeremy', age: 18},
        |{name: 'John', age: 18}]}""".stripMargin
    )

    val row: Row = MongoRelationHelper.documentToRow(document, schema)
    val rowToDocument: Document = MongoRelationHelper.rowToDocument(row)

    rowToDocument should equal(document)
  }

  it should "handle nested schemas within nested arrays" in {
    val schema: StructType = schemaFor[NestedFamily]
    val document: Document = Document.parse(
      """{familyName: "Smith", members:[
        |[{name: "James", age: 48}, {name: "Jane", age: 42}],
        |[{name: 'Jeremy', age: 18}, {name: 'John', age: 18}]]}""".stripMargin
    )

    val row: Row = MongoRelationHelper.documentToRow(document, schema)
    val rowToDocument: Document = MongoRelationHelper.rowToDocument(row)

    rowToDocument should equal(document)
  }

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
    (IsNotNull("f"), "{$match: {f: {$ne: null}}}"),
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

  // scalastyle:on magic.number null
}
