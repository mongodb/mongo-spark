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

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{SQLContext, SaveMode}

import org.bson._
import org.bson.types.ObjectId
import com.mongodb.spark._
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.sql.types.BsonCompatibility

import org.scalatest.FlatSpec

class MongoDataFrameSpec extends FlatSpec with RequiresMongoDB {
  // scalastyle:off magic.number

  val characters =
    """
     | {"name": "Bilbo Baggins", "age": 50}
     | {"name": "Gandalf", "age": 1000}
     | {"name": "Thorin", "age": 195}
     | {"name": "Balin", "age": 178}
     | {"name": "Kíli", "age": 77}
     | {"name": "Dwalin", "age": 169}
     | {"name": "Óin", "age": 167}
     | {"name": "Glóin", "age": 158}
     | {"name": "Fíli", "age": 82}
     | {"name": "Bombur"}""".trim.stripMargin.split("[\\r\\n]+").toSeq.map(Document.parse)

  "DataFrameReader" should "should be easily created from the SQLContext and load from Mongo" in withSparkContext() { sc =>
    sc.parallelize(characters).saveToMongoDB()
    val df = new SQLContext(sc).read.mongo()

    df.schema should equal(expectedSchema)
    df.count() should equal(10)
    df.filter("age > 100").count() should equal(6)
  }

  it should "handle selecting out of order columns" in withSparkContext() { sc =>
    sc.parallelize(characters).saveToMongoDB()
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.mongo()

    df.select("name", "age").orderBy("age").rdd.map(r => (r.get(0), r.get(1))).collect() should
      equal(characters.sortBy(_.getInteger("age", 0)).map(doc => (doc.getString("name"), doc.getInteger("age"))))
  }

  it should "handle mixed numerics with long precedence" in withSparkContext() { sc =>
    sc.parallelize(mixedLong).saveToMongoDB()
    val expectedData = List(1L, 1L, 1L)
    val df = new SQLContext(sc).read.mongo().select("a")

    df.count() should equal(3)
    df.collect().map(r => r.get(0)) should equal(expectedData)

    val cached = df.cache()
    cached.count() should equal(3)
    cached.collect().map(r => r.get(0)) should equal(expectedData)
  }

  it should "handle mixed numerics with double precedence" in withSparkContext() { sc =>
    sc.parallelize(mixedDouble).saveToMongoDB()
    val expectedData = List(1.0, 1.0, 1.0)
    val df = new SQLContext(sc).read.mongo().select("a")

    df.count() should equal(3)
    df.collect().map(r => r.get(0)) should equal(expectedData)

    val cached = df.cache()
    cached.count() should equal(3)
    cached.collect().map(r => r.get(0)) should equal(expectedData)
  }

  it should "handle array fields with null values" in withSparkContext() { sc =>
    sc.parallelize(arrayFieldWithNulls).saveToMongoDB()

    val saveToCollectionName = s"${collectionName}_new"
    val readConf = readConfig.withOptions(Map("collection" -> saveToCollectionName))

    new SQLContext(sc).read.mongo().write.mode(SaveMode.Overwrite).option("collection", saveToCollectionName).mongo()
    sc.loadFromMongoDB(readConfig = readConfig).collect().toList should equal(arrayFieldWithNulls)
  }

  it should "handle document fields with null values" in withSparkContext() { sc =>
    sc.parallelize(documentFieldWithNulls).saveToMongoDB()

    val saveToCollectionName = s"${collectionName}_new"
    val readConf = readConfig.withOptions(Map("collection" -> saveToCollectionName))

    new SQLContext(sc).read.mongo().write.mode(SaveMode.Overwrite).option("collection", saveToCollectionName).mongo()
    sc.loadFromMongoDB(readConfig = readConfig).collect().toList should equal(documentFieldWithNulls)
  }

  it should "be easily created with a provided case class" in withSparkContext() { sc =>
    sc.parallelize(characters).saveToMongoDB()

    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.mongo[Character]()
    val reflectedSchema: StructType = ScalaReflection.schemaFor[Character].dataType.asInstanceOf[StructType]

    df.schema should equal(reflectedSchema)
    df.count() should equal(10)
    df.filter("age > 100").count() should equal(6)
  }

  it should "include any pipelines when inferring the schema" in withSparkContext() { sc =>
    sc.parallelize(characters).saveToMongoDB()
    sc.parallelize(List("{counter: 1}", "{counter: 2}", "{counter: 3}").map(Document.parse)).saveToMongoDB()
    val sqlContext = new SQLContext(sc)

    var df = sqlContext.read.option("pipeline", "[{ $match: { name: { $exists: true } } }]").mongo()
    df.schema should equal(expectedSchema)
    df.count() should equal(10)
    df.filter("age > 100").count() should equal(6)

    df = sqlContext.read.option("pipeline", "[{ $project: { _id: 1, age: 1 } }]").mongo()
    df.schema should equal(createStructType(expectedSchema.fields.filter(p => p.name != "name")))
  }

  it should "throw an exception if pipeline is invalid" in withSparkContext() { sc =>
    sc.parallelize(characters).saveToMongoDB()
    sc.parallelize(List("{counter: 1}", "{counter: 2}", "{counter: 3}").map(Document.parse)).saveToMongoDB()

    an[IllegalArgumentException] should be thrownBy new SQLContext(sc).read.option("pipeline", "[1, 2, 3]").mongo()
  }

  "DataFrameWriter" should "be easily created from a DataFrame and save to Mongo" in withSparkContext() { sc =>
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sc.parallelize(characters)
      .filter(_.containsKey("age")).map(doc => Character(doc.getString("name"), doc.getInteger("age")))
      .toDF().write.mongo()

    sqlContext.read.mongo[Character]().count() should equal(9)
  }

  it should "take custom writeConfig" in withSparkContext() { sc =>
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val saveToCollectionName = s"${collectionName}_new"
    val writeConfig = WriteConfig(sc.getConf).withOptions(Map("collection" -> saveToCollectionName))

    sc.parallelize(characters)
      .filter(_.containsKey("age")).map(doc => Character(doc.getString("name"), doc.getInteger("age")))
      .toDF().write.mongo(writeConfig)

    sqlContext.read.option("collection", saveToCollectionName).mongo[Character]().count() should equal(9)
  }

  "DataFrames" should "round trip all bson types" in withSparkContext() { sc =>
    database.getCollection(collectionName, classOf[BsonDocument]).insertOne(allBsonTypesDocument)

    val newCollectionName = s"${collectionName}_new"
    new SQLContext(sc).read.mongo().write.option("collection", newCollectionName).mongo()

    val original = database.getCollection(collectionName).find().iterator().asScala.toList
    val copied = database.getCollection(newCollectionName).find().iterator().asScala.toList
    copied should equal(original)
  }

  it should "be able to cast all types to a string value" in withSparkContext() { sc =>
    database.getCollection(collectionName, classOf[BsonDocument]).insertOne(allBsonTypesDocument)
    val bsonValuesAsStrings = sc.loadFromMongoDB().toDS[BsonValuesAsStringClass]().first()

    val expected = BsonValuesAsStringClass(
      nullValue = "null",
      int32 = "42",
      int64 = "52",
      boolean = "true",
      date = """{ "$date" : 1463497097 }""",
      double = "62.0",
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
      binary = """{ "$binary" : "BQQDAgE=", "$type" : "0" }""",
      oldBinary = """{ "$binary" : "AQEBAQE=", "$type" : "2" }""",
      arrayInt = "[1, 2, 3]",
      document = """{ "a" : 1 }""",
      dbPointer = """{ "$ref" : "db.coll", "$id" : { "$oid" : "000000000000000000000000" } }"""
    )
    bsonValuesAsStrings should equal(expected)
  }

  private val expectedSchema: StructType = {
    val _idField: StructField = createStructField("_id", BsonCompatibility.ObjectId.structType, true)
    val nameField: StructField = createStructField("name", DataTypes.StringType, true)
    val ageField: StructField = createStructField("age", DataTypes.IntegerType, true)
    createStructType(Array(_idField, ageField, nameField))
  }

  private val arrayFieldWithNulls: Seq[Document] = Seq("{_id: 1, a: [1,2,3]}", "{_id: 2, a: []}", "{_id: 3, a: null}").map(Document.parse)
  private val documentFieldWithNulls: Seq[Document] = Seq("{_id: 1, a: {a: 1}}", "{_id: 2, a: {}}", "{_id: 3, a: null}").map(Document.parse)
  private val mixedLong: Seq[Document] = Seq(new Document("a", 1), new Document("a", 1), new Document("a", 1L))
  private val mixedDouble: Seq[Document] = Seq(new Document("a", 1), new Document("a", 1L), new Document("a", 1.0))
  private val objectId = new ObjectId("000000000000000000000000")
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
