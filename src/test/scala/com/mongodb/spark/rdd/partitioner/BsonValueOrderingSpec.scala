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
import scala.util.Random

import org.bson.types.{Decimal128, ObjectId}
import org.bson.{BsonDbPointer, _}

import org.scalatest.{FlatSpec, Matchers}

class BsonValueOrderingSpec extends FlatSpec with Matchers {
  // scalastyle:off magic.number

  private val objectId = new ObjectId("000000000000000000000000")
  private val allBsonTypesDocument: BsonDocument = {
    val document = new BsonDocument()
    document.put("nullValue", new BsonNull())
    document.put("int32", new BsonInt32(42))
    document.put("int64", new BsonInt64(52L))
    document.put("decimal", new BsonDecimal128(new Decimal128(BigDecimal(10).bigDecimal)))
    document.put("boolean", new BsonBoolean(true))
    document.put("date", new BsonDateTime(1463497097))
    document.put("double", new BsonDouble(62.0))
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
  val allBsonTypes: Seq[(String, BsonValue)] = allBsonTypesDocument.entrySet().asScala.map(e => (e.getKey, e.getValue)).toSeq

  val orderedKeys = Seq("minKey", "nullValue", "decimal", "int32", "int64", "double", "symbol", "string", "document", "arrayInt", "binary",
    "oldBinary", "objectId", "boolean", "date", "timestamp", "regex", "maxKey")

  val undefinedOrderingKeys = Seq("dbPointer", "undefined", "code", "codeWithScope")

  implicit object BsonValueOrdering extends BsonValueOrdering

  "The BsonValueOrdering trait" should "order all bson types correctly" in {
    val data = allBsonTypes.filter({ case kv => orderedKeys.contains(kv._1) }).map(_._2)
    val ordered = Random.shuffle(data).sorted
    ordered should equal(orderedKeys.map(allBsonTypesDocument.get(_)))
  }

  it should "compare numbers types correctly" in {
    val data: Seq[BsonValue] = Seq(new BsonInt32(1), new BsonInt64(2), new BsonDecimal128(Decimal128.parse("2.5")),
      new BsonDouble(3.0), new BsonDouble(4.0), new BsonInt64(5), new BsonInt32(6))
    val ordered = Random.shuffle(data).sorted
    ordered should equal(data)
  }

  it should "compare numbers and longs correctly" in {
    val longAndDoubleData: Seq[BsonValue] = Seq(
      new BsonDouble(Double.NegativeInfinity),
      new BsonDouble(Long.MinValue), new BsonInt64(Long.MinValue + 1),
      new BsonDouble(Double.MinPositiveValue), new BsonInt64(1L),
      new BsonInt64(Long.MaxValue), new BsonDouble(Long.MaxValue),
      new BsonDouble(Double.PositiveInfinity)
    )

    val orderedLongsAndDouble = Random.shuffle(longAndDoubleData).sorted
    orderedLongsAndDouble should equal(longAndDoubleData)
  }

  it should "compare string types correctly" in {
    val data: Seq[BsonValue] = Seq(new BsonString("12345"), new BsonSymbol("a"), new BsonSymbol("b"), new BsonSymbol("c 1"),
      new BsonString("c 2"), new BsonSymbol("d"), new BsonString("e"))
    val ordered = Random.shuffle(data).sorted
    ordered should equal(data)
  }

  it should "compare timestamp types correctly" in {
    val data: Seq[BsonValue] = Seq(new BsonTimestamp(1, 5), new BsonTimestamp(1, 6), new BsonTimestamp(2, 1),
      new BsonTimestamp(2, 10), new BsonTimestamp(3, 0), new BsonTimestamp(3, 1), new BsonTimestamp(10010101, 0))
    val ordered = Random.shuffle(data).sorted
    ordered should equal(data)
  }

  it should "compare binary types correctly" in {
    val data: Seq[BsonValue] = Seq(
      new BsonBinary(BsonBinarySubType.BINARY, Array[Byte](1, 1, 1, 1, 1)),
      new BsonBinary(BsonBinarySubType.OLD_BINARY, Array[Byte](1, 1, 1, 1, 1)),
      new BsonBinary(BsonBinarySubType.OLD_BINARY, Array[Byte](2, 2, 2, 2, 2, 2)),
      new BsonBinary(BsonBinarySubType.OLD_BINARY, Array[Byte](2, 2, 2, 2, 2, 2, 2)),
      new BsonBinary(BsonBinarySubType.OLD_BINARY, Array[Byte](2, 2, 2, 2, 2, 2, 3))
    )
    val ordered = Random.shuffle(data).sorted
    ordered should equal(data)
  }

  it should "compare array types correctly" in {
    val data: Seq[BsonValue] = Seq(
      new BsonArray(List(new BsonInt32(1)).asJava),
      new BsonArray(List(new BsonInt32(1), new BsonInt32(2)).asJava),
      new BsonArray(List(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3)).asJava),
      new BsonArray(List(new BsonString("1")).asJava)
    )
    val ordered = Random.shuffle(data).sorted
    ordered should equal(data)
  }

  it should "compare regex types correctly" in {
    val data: Seq[BsonValue] = Seq(
      new BsonRegularExpression(".*"),
      new BsonRegularExpression(".*", "i"),
      new BsonRegularExpression("[0-9a-z]+"),
      new BsonRegularExpression("[a-z]+")
    )
    val ordered = Random.shuffle(data).sorted
    ordered should equal(data)
  }

  it should "compare document types correctly" in {
    val data: Seq[BsonValue] = Seq(
      BsonDocument.parse("""{}"""),
      BsonDocument.parse("""{a: -1}"""),
      BsonDocument.parse("""{a: 0}"""),
      BsonDocument.parse("""{a: 1}"""),
      BsonDocument.parse("""{a: 1, b: 1}"""),
      BsonDocument.parse("""{a: 2}"""),
      BsonDocument.parse("""{a: 9, b: 1}"""),
      BsonDocument.parse("""{a: 10, b: 0}"""),
      BsonDocument.parse("""{a: 100, b: 10}"""),
      BsonDocument.parse("""{a: 1000, b: 10}"""),
      BsonDocument.parse("""{a: 1000, b: 10, c: -1}"""),
      BsonDocument.parse("""{a: "1", b: 0}""")
    )
    val ordered = Random.shuffle(data).sorted
    ordered should equal(data)
  }

  it should "have no defined order for undefined types" in {
    val data: Seq[BsonValue] = allBsonTypes.filter({ case kv => undefinedOrderingKeys.contains(kv._1) }).map(_._2)
    val randomised = Random.shuffle(data)
    val ordered = randomised.sorted
    ordered should equal(randomised)
  }
  // scalastyle:on magic.number
}
