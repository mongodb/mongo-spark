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

package UDF

import javax.xml.bind.DatatypeConverter

import scala.collection.JavaConverters._

import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{DataFrame, SQLContext}

import org.bson._
import org.bson.types.ObjectId
import com.mongodb.spark.sql.helpers.{StructFields, UDF}
import com.mongodb.spark.{MongoSpark, RequiresMongoDB}

class HelpersSpec extends RequiresMongoDB {

  override def beforeEach() {
    super.beforeEach()
    database.getCollection(collectionName, classOf[BsonDocument]).insertOne(allBsonTypesDocument)
    database.getCollection(collectionName).insertOne(Document.parse("{_id: 1}"))
  }

  // scalastyle:off magic.number
  "the user defined function helpers" should "handle Binary values" in withSQLContext() { sqlContext =>
    val binary = allBsonTypesDocument.get("binary").asBinary()
    val base64 = DatatypeConverter.printBase64Binary(binary.getData)
    val df = createDF(sqlContext, StructFields.binary("binary", nullable = false))

    sqlContext.udf.register("Binary", UDF.binary _)

    df.filter(s"binary = Binary('$base64')").count() should equal(1)
  }

  it should "handle Binary values with a subtype" in withSQLContext() { sqlContext =>
    val binary = allBsonTypesDocument.get("oldBinary").asBinary()
    val base64 = DatatypeConverter.printBase64Binary(binary.getData)
    val df = createDF(sqlContext, StructFields.binary("oldBinary", nullable = false))

    sqlContext.udf.register("BinaryWithSubType", UDF.binaryWithSubType _)
    df.filter(s"oldBinary = BinaryWithSubType(${binary.getType}, '$base64')").count() should equal(1)
  }

  it should "handle DbPointers" in withSQLContext() { sqlContext =>
    val dbPointer = allBsonTypesDocument.get("dbPointer").asDBPointer()
    val df = createDF(sqlContext, StructFields.dbPointer("dbPointer", nullable = false))

    sqlContext.udf.register("DbPointer", UDF.dbPointer _)
    df.filter(s"dbPointer = DbPointer('${dbPointer.getNamespace}', '${dbPointer.getId.toHexString}')").count() should equal(1)
  }

  it should "handle JavaScript" in withSQLContext() { sqlContext =>
    val code = allBsonTypesDocument.get("code").asJavaScript()
    val df = createDF(sqlContext, StructFields.javaScript("code", nullable = false))

    sqlContext.udf.register("JavaScript", UDF.javaScript _)
    df.filter(s"code = JavaScript('${code.getCode}')").count() should equal(1)
  }

  it should "handle JavaScript with scope" in withSQLContext() { sqlContext =>
    val code = allBsonTypesDocument.get("codeWithScope").asJavaScriptWithScope()
    val df = createDF(sqlContext, StructFields.javaScriptWithScope("codeWithScope", nullable = false))

    sqlContext.udf.register("JavaScript", UDF.javaScriptWithScope _)
    df.filter(s"codeWithScope = JavaScript('${code.getCode}', '${code.getScope.toJson}')").count() should equal(1)
  }

  it should "handle maxKeys" in withSQLContext() { sqlContext =>
    val df = createDF(sqlContext, StructFields.maxKey("maxKey", nullable = false))

    sqlContext.udf.register("maxKey", UDF.maxKey _)
    df.filter(s"maxKey = maxKey()").count() should equal(1)
  }

  it should "handle minKeys" in withSQLContext() { sqlContext =>
    val df = createDF(sqlContext, StructFields.minKey("minKey", nullable = false))

    sqlContext.udf.register("minKey", UDF.minKey _)
    df.filter(s"minKey = minKey()").count() should equal(1)
  }

  it should "handle ObjectIds" in withSQLContext() { sqlContext =>
    val df = createDF(sqlContext, StructFields.objectId("objectId", nullable = false))
    sqlContext.udf.register("ObjectId", UDF.objectId _)

    df.filter(s"objectId = ObjectId('${objectId.toHexString}')").count() should equal(1)
  }

  it should "handle Regular Expressions" in withSQLContext() { sqlContext =>
    val regex = allBsonTypesDocument.get("regex").asRegularExpression()
    val df = createDF(sqlContext, StructFields.regularExpression("regex", nullable = false))
    sqlContext.udf.register("Regex", UDF.regularExpression _)

    df.filter(s"regex = Regex('${regex.getPattern}')").count() should equal(1)
  }

  it should "handle Regular Expressions with options" in withSQLContext() { sqlContext =>
    val regex = allBsonTypesDocument.get("regexWithOptions").asRegularExpression()
    val df = createDF(sqlContext, StructFields.regularExpression("regexWithOptions", nullable = false))
    sqlContext.udf.register("Regex", UDF.regularExpressionWithOptions _)

    df.filter(s"regexWithOptions = Regex('${regex.getPattern}', '${regex.getOptions}')").count() should equal(1)
  }

  it should "handle Symbols" in withSQLContext() { sqlContext =>
    val symbol = allBsonTypesDocument.get("symbol").asSymbol()
    val df = createDF(sqlContext, StructFields.symbol("symbol", nullable = false))
    sqlContext.udf.register("Symbol", UDF.symbol _)

    df.filter(s"symbol = Symbol('${symbol.getSymbol}')").count() should equal(1)
  }

  it should "handle Timestamps" in withSQLContext() { sqlContext =>
    val timestamp = allBsonTypesDocument.get("timestamp").asTimestamp()
    val df = createDF(sqlContext, StructFields.timestamp("timestamp", nullable = false))
    sqlContext.udf.register("Timestamp", UDF.timestamp _)

    df.filter(s"timestamp = Timestamp(${timestamp.getTime}, ${timestamp.getInc})").count() should equal(1)
  }

  it should "handle Undefined values" in withSQLContext() { sqlContext =>
    val df = createDF(sqlContext, StructFields.undefined("undefined", nullable = false))
    sqlContext.udf.register("Undefined", UDF.undefined _)

    df.filter(s"undefined = Undefined()").count() should equal(1)
  }

  private def createDF(sqlContext: SQLContext, structField: StructField): DataFrame = {
    MongoSpark
      .read(sqlContext)
      .schema(DataTypes.createStructType(Array(structField)))
      .load()
  }

  private val objectId = new ObjectId()
  private val allBsonTypesDocument: BsonDocument = {
    val document = new BsonDocument()
    document.put("nullValue", new BsonNull())
    document.put("int32", new BsonInt32(42))
    document.put("int64", new BsonInt64(52L))
    document.put("boolean", new BsonBoolean(true))
    document.put("date", new BsonDateTime(1463497097))
    document.put("double", new BsonDouble(62.0))
    document.put("string", new BsonString("spark connector"))
    document.put("minKey", new BsonMinKey())
    document.put("maxKey", new BsonMaxKey())
    document.put("objectId", new BsonObjectId(objectId))
    document.put("code", new BsonJavaScript("int i = 0;"))
    document.put("codeWithScope", new BsonJavaScriptWithScope("int x = y", new BsonDocument("y", new BsonInt32(1))))
    document.put("regex", new BsonRegularExpression("^test.*regex.*xyz$", ""))
    document.put("regexWithOptions", new BsonRegularExpression("^test.*regex.*xyz$", "i"))
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
