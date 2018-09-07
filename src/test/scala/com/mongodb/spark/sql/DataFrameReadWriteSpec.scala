/*
 * Copyright 2018 MongoDB, Inc.
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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{MapType, StructField, StructType}
import org.bson._
import org.bson.types.{Decimal128, ObjectId}

import scala.collection.JavaConverters._

class DataFrameReadWriteSpec extends DataSourceSpecBase {
  // scalastyle:off magic.number

  "Read and Write support" should "round trip all bson types" in withSparkSession() { spark =>
    if (!serverAtLeast(3, 4)) cancel("MongoDB < 3.4")
    database.getCollection(collectionName, classOf[BsonDocument]).insertOne(allBsonTypesDocument)

    val newCollectionName = s"${collectionName}_new"
    spark.load().createWriter().option("collection", newCollectionName).save()

    val original = database.getCollection(collectionName).find().iterator().asScala.toList
    val copied = database.getCollection(newCollectionName).find().iterator().asScala.toList
    copied should equal(original)
  }

  it should "be able to cast all types to a string value" in withSparkSession() { spark =>
    if (!serverAtLeast(3, 4)) cancel("MongoDB < 3.4")
    database.getCollection(collectionName, classOf[BsonDocument]).insertOne(allBsonTypesDocument)

    val bsonValuesAsStrings = spark.loadDS[BsonValuesAsStringClass]().first()

    val expected = BsonValuesAsStringClass(
      nullValue = null, // scalastyle:ignore
      int32 = "42",
      int64 = "52",
      bool = "true",
      date = """{ "$date" : 1463497097 }""",
      dbl = "62.0",
      decimal = """{ "$numberDecimal" : "72.01" }""",
      string = "spark connector",
      minKey = """{ "$minKey" : 1 }""",
      maxKey = """{ "$maxKey" : 1 }""",
      objectId = "000000000000000000000000",
      code = """{ "$code" : "int i = 0;" }""",
      codeWithScope = """{ "$code" : "int x = y", "$scope" : { "y" : 1 } }""",
      regex = """{ "$regex" : "^test.*regex.*xyz$", "$options" : "i" }""",
      symbol = """{ "$symbol" : "ruby stuff" }""",
      timestamp = """{ "$timestamp" : { "t" : 305419896, "i" : 5 } }""",
      undefined = """{ "$undefined" : true }""",
      binary = """{ "$binary" : "BQQDAgE=", "$type" : "00" }""",
      oldBinary = """{ "$binary" : "AQEBAQE=", "$type" : "02" }""",
      arrayInt = "[1, 2, 3]",
      document = """{ "a" : 1 }""",
      dbPointer = """{ "$dbPointer" : { "$ref" : "db.coll", "$id" : { "$oid" : "000000000000000000000000" } } }"""
    )
    bsonValuesAsStrings should equal(expected)
  }

  it should "be able to round trip schemas containing MapTypes" in withSparkSession() { spark =>
    val characterMap = characters.map(doc => Row(doc.getString("name"), Map("book" -> "The Hobbit", "author" -> "J. R. R. Tolkien")))
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = false),
      StructField("attributes", MapType(StringType, StringType), nullable = true)
    ))
    val df = spark.createDataFrame(spark.parallelize(characterMap), schema)
    df.save()

    val savedDF = spark.read().schema(schema).load()
    savedDF.collectAsList() should equal(df.collectAsList())
  }

  private val objectId = new ObjectId("000000000000000000000000")
  private val allBsonTypesDocument: BsonDocument = {
    val document = new BsonDocument()
    document.put("nullValue", new BsonNull())
    document.put("int32", new BsonInt32(42))
    document.put("int64", new BsonInt64(52L))
    document.put("bool", new BsonBoolean(true))
    document.put("date", new BsonDateTime(1463497097))
    document.put("dbl", new BsonDouble(62.0))
    document.put("decimal", new BsonDecimal128(new Decimal128(BigDecimal(72.01).bigDecimal)))
    document.put("string", new BsonString("spark connector"))
    document.put("minKey", new BsonMinKey())
    document.put("maxKey", new BsonMaxKey())
    document.put("objectId", new BsonObjectId(objectId))
    document.put("code", new BsonJavaScript("int i = 0;"))
    document.put("codeWithScope", new BsonJavaScriptWithScope("int x = y", new BsonDocument("y", new BsonInt32(1))))
    document.put("regex", new BsonRegularExpression("^test.*regex.*xyz$", "i"))
    document.put("symbol", new BsonSymbol("ruby stuff"))
    document.put("timestamp", new BsonTimestamp(0x12345678, 5))
    document.put("undefined", new BsonUndefined())
    document.put("binary", new BsonBinary(Array[Byte](5, 4, 3, 2, 1)))
    document.put("oldBinary", new BsonBinary(BsonBinarySubType.OLD_BINARY, Array[Byte](1, 1, 1, 1, 1)))
    document.put("arrayInt", new BsonArray(List(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3)).asJava))
    document.put("document", new BsonDocument("a", new BsonInt32(1)))
    document.put("dbPointer", new BsonDbPointer("db.coll", objectId))
    document
  }
  // scalastyle:on magic.number
}
