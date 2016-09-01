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

import scala.collection.Map
import scala.reflect.runtime.universe._

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.DataTypes.{IntegerType, StringType}
import org.apache.spark.sql.types._

import org.bson.BsonDocument
import com.mongodb.spark.RequiresMongoDB
import com.mongodb.spark.exceptions.MongoTypeConversionException
import com.mongodb.spark.sql.MapFunctions.{documentToRow, rowToDocument}

class MapFunctionsSpec extends RequiresMongoDB {

  // scalastyle:off magic.number null
  case class Person(name: String, age: Int)

  case class Family(familyName: String, members: List[Person])

  case class NestedFamily(familyName: String, members: List[List[Person]])

  case class MixedNumericsInt(num: Int)

  case class MixedNumericsLong(num: Long)

  case class MixedNumericsDouble(num: Double)

  def schemaFor[T <: Product: TypeTag]: StructType = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]

  "documentToRow" should "convert a Document into a Row with the given schema" in {
    val schema: StructType = schemaFor[Person]
    val document: BsonDocument = BsonDocument.parse("{name: 'John', age: 18}")

    val row: Row = documentToRow(document, schema)
    row.toSeq should equal(Array("John", 18))
    row.schema should equal(schema)
  }

  it should "not prune the schema when given a document with missing values" in {
    val schema: StructType = schemaFor[Person]
    val document: BsonDocument = BsonDocument.parse("{name: 'John'}")

    val row: Row = documentToRow(document, schema)
    row.toSeq should equal(Array("John", null))
    row.schema should equal(schema)
  }

  it should "prune the schema when limited by passed required columns" in {
    val schema: StructType = schemaFor[Person]
    val document: BsonDocument = BsonDocument.parse("{name: 'John', age: 18}")

    val row: Row = documentToRow(document, schema, Array("age"))
    row.toSeq should equal(Array(18))
    row.schema should equal(DataTypes.createStructType(Array(schema.fields(1))))
  }

  it should "ignore any extra data in the document that is not included in the schema" in {
    val schema: StructType = schemaFor[Person]
    val document: BsonDocument = BsonDocument.parse("{name: 'John', age: 18, height: 192}")

    val row: Row = documentToRow(document, schema)
    row.toSeq should equal(Array("John", 18))
    row.schema should equal(schema)
  }

  it should "handle nested schemas" in {
    val schema: StructType = schemaFor[Family]
    val document: BsonDocument = BsonDocument.parse(
      """{familyName: "Smith", members: [
        |{name: "James", age: 48},
        |{name: "Jane", age: 42},
        |{name: 'Jeremy', age: 18},
        |{name: 'John', age: 18}]}""".stripMargin
    )

    val row: Row = documentToRow(document, schema)
    row.getAs[String]("familyName") should equal("Smith")
    row.getSeq[Row](1).map(_.toSeq) should equal(Array(Array("James", 48), Array("Jane", 42), Array("Jeremy", 18), Array("John", 18)))
    row.schema should equal(schema)
  }

  it should "handle schemas containing maps" in {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = false),
      StructField("attributes", MapType(StringType, StringType), nullable = true)
    ))
    val document: BsonDocument = BsonDocument.parse(
      """{name: "Bilbo", attributes: {
         |"book": "The Hobbit",
         |"author": "J.R.R Tolkien"}}""".stripMargin
    )

    val expectedRow = new GenericRowWithSchema(Array("Bilbo", Map("book" -> "The Hobbit", "author" -> "J.R.R Tolkien")), schema)
    val row: Row = documentToRow(document, schema)
    row should equal(expectedRow)
  }

  it should "throw an exception when passed maps without string keys" in {
    val schema = StructType(Seq(StructField("test", MapType(IntegerType, StringType), nullable = true)))

    val row = new GenericRowWithSchema(Array(Map(1 -> "one")), schema)
    an[MongoTypeConversionException] should be thrownBy rowToDocument(row)
  }

  "rowToDocument" should "convert a Row into a Document" in {
    val schema: StructType = schemaFor[Person]
    val original: BsonDocument = BsonDocument.parse("{name: 'John', age: 18}")

    val row: Row = documentToRow(original, schema)
    val converted: BsonDocument = rowToDocument(row)

    converted should equal(original)
  }

  it should "handle nested schemas" in {
    val schema: StructType = schemaFor[Family]
    val original: BsonDocument = BsonDocument.parse(
      """{familyName: "Smith", members:[
        |{name: "James", age: 48},
        |{name: "Jane", age: 42},
        |{name: 'Jeremy', age: 18},
        |{name: 'John', age: 18}]}""".stripMargin
    )

    val row: Row = documentToRow(original, schema)
    val converted: BsonDocument = rowToDocument(row)

    converted should equal(original)
  }

  it should "handle nested schemas within nested arrays" in {
    val schema: StructType = schemaFor[NestedFamily]
    val original: BsonDocument = BsonDocument.parse(
      """{familyName: "Smith", members:[
        |[{name: "James", age: 48}, {name: "Jane", age: 42}],
        |[{name: 'Jeremy', age: 18}, {name: 'John', age: 18}]]}""".stripMargin
    )

    val row: Row = documentToRow(original, schema)
    val converted: BsonDocument = rowToDocument(row)

    converted should equal(original)
  }

  it should "handle mixed numerics based on the schema" in {
    val convertedInt = rowToDocument(documentToRow(BsonDocument.parse("{num: 1.0}"), schemaFor[MixedNumericsInt]))
    convertedInt should equal(BsonDocument.parse("{num: 1 }"))

    val convertedLong = rowToDocument(documentToRow(BsonDocument.parse("{num: 1.0}"), schemaFor[MixedNumericsLong]))
    convertedLong should equal(BsonDocument.parse("""{num: {$numberLong: "1"}}"""))

    val convertedDouble = rowToDocument(documentToRow(BsonDocument.parse("{num: 1}"), schemaFor[MixedNumericsDouble]))
    convertedDouble should equal(BsonDocument.parse("{num: 1.0 }"))
  }

  it should "throw a MongoTypeConversionException when casting to an invalid DataType" in {
    an[MongoTypeConversionException] should be thrownBy documentToRow(BsonDocument.parse("{num: [1]}"), schemaFor[MixedNumericsDouble])
    an[MongoTypeConversionException] should be thrownBy rowToDocument(new GenericRowWithSchema(Array(Array(1)), schemaFor[MixedNumericsDouble]))
  }
  // scalastyle:on magic.number null
}

