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

package com.mongodb.spark.sql.fieldTypes

import java.util.regex.Pattern
import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession

import org.bson.{BsonBinary, _}
import com.mongodb.spark._
import com.mongodb.spark.sql._

class FieldTypesSpec extends RequiresMongoDB {

  val bsonObjectId = new BsonObjectId(new org.bson.types.ObjectId())
  val bsonBinary = new BsonBinary(BsonBinarySubType.OLD_BINARY, Array[Byte](1, 1, 1, 1, 1))
  val bsonDbPointer = new BsonDbPointer("db.coll", bsonObjectId.getValue)
  val bsonJavaScript = new BsonJavaScript("int i = 0;")
  val bsonJavaScriptWithScope = new BsonJavaScriptWithScope("int x = y", new BsonDocument("y", new BsonInt32(1)))
  val bsonMaxKey = new BsonMaxKey()
  val bsonMinKey = new BsonMinKey()
  val bsonRegularExpression = new BsonRegularExpression("^test.*regex.*xyz$", "i")
  val bsonSymbol = new BsonSymbol("ruby stuff")
  val bsonTimestamp = new BsonTimestamp(0x12345678, 5) // scalastyle:ignore
  val bsonUndefined = new BsonUndefined()
  val bsonDocument: BsonDocument = {
    val document = new BsonDocument()
    document.put("_id", bsonObjectId)
    document.put("binary", bsonBinary)
    document.put("dbPointer", bsonDbPointer)
    document.put("javaScript", bsonJavaScript)
    document.put("javaScriptWithScope", bsonJavaScriptWithScope)
    document.put("maxKey", bsonMaxKey)
    document.put("minKey", bsonMinKey)
    document.put("regularExpression", bsonRegularExpression)
    document.put("symbol", bsonSymbol)
    document.put("timestamp", bsonTimestamp)
    document.put("undefined", bsonUndefined)
    document
  }

  "Fields" should "allow the round tripping of a case class representing complex bson types" in withSparkContext() { sc =>
    database.getCollection(collectionName, classOf[BsonDocument]).insertOne(bsonDocument)
    val newCollectionName = s"${collectionName}_new"

    SparkSession.builder().getOrCreate().read.mongo[BsonTypesCaseClass]().write.option("collection", newCollectionName).mongo()
    val original = database.getCollection(collectionName).find().iterator().asScala.toList.head
    val copied = database.getCollection(newCollectionName).find().iterator().asScala.toList.head
    copied should equal(original)
  }

  it should "be able to create a dataset based on a case class representing complex bson types" in withSparkSession() { spark =>
    database.getCollection(collectionName, classOf[BsonDocument]).insertOne(bsonDocument)
    val expected = BsonTypesCaseClass(ObjectId(bsonObjectId.getValue), Binary(bsonBinary.getType, bsonBinary.getData),
      DbPointer(bsonDbPointer.getNamespace, bsonDbPointer.getId), JavaScript(bsonJavaScript.getCode),
      JavaScriptWithScope(bsonJavaScriptWithScope.getCode, bsonJavaScriptWithScope.getScope), MinKey(), MaxKey(),
      RegularExpression(bsonRegularExpression.getPattern, bsonRegularExpression.getOptions), Symbol(bsonSymbol.getSymbol),
      Timestamp(bsonTimestamp.getTime, bsonTimestamp.getInc), Undefined())

    spark.sparkContext.loadFromMongoDB().toDS[BsonTypesCaseClass]().first() should equal(expected)
  }

  it should "be able to create a Regular Expression from a Pattern" in {
    val regex = RegularExpression(Pattern.compile("^test.*regex.*xyz$", Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | 256))
    regex.regex should equal("^test.*regex.*xyz$")
    regex.options should equal("giu")
  }

}
