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

import org.apache.spark.sql.types.DataTypes.{createArrayType, createStructField, createStructType}
import org.apache.spark.sql.types.{DataTypes, StructField}

import org.bson.conversions.Bson
import org.bson.{BsonDocument, Document}
import com.mongodb.MongoClient
import com.mongodb.spark._
import com.mongodb.spark.sql.types.{BsonCompatibility, ConflictType}

import org.scalatest.prop.TableDrivenPropertyChecks

class MongoInferSchemaSpec extends RequiresMongoDB with MongoDataGenerator with TableDrivenPropertyChecks {

  "MongoSchemaHelper" should "be able to infer the schema from simple types" in withSparkContext() { sc =>
    forAll(genSimpleDataTypes) { (datum: Seq[MongoDataType]) =>
      datum.foreach { data =>
        sc.parallelize(data.getDocuments.toBson).saveToMongoDB()
        data.schema should equal(MongoInferSchema(sc))
        database.drop()
      }
    }
  }

  it should "be able to infer the schema from a flat array" in withSparkContext() { sc =>
    forAll(genArrayDataType(0)) { (data: MongoDataType) =>
      sc.parallelize(data.getDocuments.toBson).saveToMongoDB()
      data.schema should equal(MongoInferSchema(sc))
      database.drop()
    }
  }

  it should "be able to infer the schema from a flat document" in withSparkContext() { sc =>
    forAll(genDocumentDataType(0)) { (data: MongoDataType) =>
      sc.parallelize(data.getDocuments.toBson).saveToMongoDB()
      data.schema should equal(MongoInferSchema(sc))
      database.drop()
    }
  }

  it should "be able to infer the schema from a nested array" in withSparkContext() { sc =>
    forAll(genArrayDataType()) { (data: MongoDataType) =>
      sc.parallelize(data.getDocuments.toBson).saveToMongoDB()
      data.schema should equal(MongoInferSchema(sc))
      database.drop()
    }
  }

  it should "be able to infer the schema from a multi level document" in withSparkContext() { sc =>
    forAll(genDocumentDataType()) { (data: MongoDataType) =>
      sc.parallelize(data.getDocuments.toBson).saveToMongoDB()
      data.schema should equal(MongoInferSchema(sc))
      database.drop()
    }
  }

  it should "be able to infer the schema with custom sampleSize" in withSparkContext() { sc =>
    forAll(genDocumentDataType()) { (data: MongoDataType) =>
      sc.parallelize(data.getDocuments.toBson).saveToMongoDB()
      val rdd = MongoSpark.builder().sparkContext(sc).readConfig(readConfig.copy(sampleSize = 200)).build().toRDD[BsonDocument]() //scalastyle:ignore
      data.schema should equal(MongoInferSchema(rdd))
      database.drop()
    }
  }

  it should "ignore empty arrays and null values in arrays" in withSparkContext() { sc =>
    forAll(genArrayDataType(0)) { (data: MongoDataType) =>

      val documents: Seq[BsonDocument] = data.getDocuments.toBson
      val fieldName = documents.head.keySet().asScala.head
      val allDocs = documents ++ Seq(
        new Document(fieldName, List().asJava),
        new Document(fieldName, List(null, null).asJava) // scalastyle:ignore
      ).toBson

      sc.parallelize(allDocs).saveToMongoDB()
      data.schema should equal(MongoInferSchema(sc))
      database.drop()
    }
  }

  it should "use any set pipelines on the RDD" in withSparkContext() { sc =>
    forAll(genDocumentDataType(0)) { (data: MongoDataType) =>
      val allDocs = data.getDocuments ++ Seq("{badData: [1 ,2, 3]}", "{badData: 55}", "{badData: 'bad'}").map(Document.parse)
      sc.parallelize(allDocs.toBson).saveToMongoDB()

      val rdd = MongoSpark.load[BsonDocument](sc).withPipeline(Seq(Document.parse("{ $match: { badData : { $exists : false } } }")))
      data.schema should equal(MongoInferSchema(rdd))
      database.drop()
    }
  }

  it should "upscale number types based on numeric precedence" in withSparkContext() { sc =>
    val _idField: StructField = createStructField("_id", BsonCompatibility.ObjectId.structType, true)
    val longField: StructField = createStructField("a", DataTypes.LongType, true)
    val doubleField: StructField = createStructField("a", DataTypes.DoubleType, true)

    // Contains a long
    sc.parallelize(Seq(new Document("a", -1), new Document("a", 1), new Document("a", Long.MaxValue))).saveToMongoDB()
    MongoInferSchema(sc) should equal(createStructType(Array(_idField, longField)))
    database.drop()

    // Contains a double
    sc.parallelize(Seq(new Document("a", -1), new Document("a", 1.0), new Document("a", Long.MaxValue))).saveToMongoDB()
    MongoInferSchema(sc) should equal(createStructType(Array(_idField, doubleField)))
  }

  it should "be able to infer the schema from arrays with mixed keys" in withSparkContext() { sc =>
    val _idField: StructField = createStructField("_id", BsonCompatibility.ObjectId.structType, true)
    val elements = createStructType(Seq("b", "c", "d", "e").map(createStructField(_, DataTypes.IntegerType, true)).toArray)
    val arrayField = createStructField("a", createArrayType(elements, true), true)
    val expectedSchema = createStructType(Array(_idField, arrayField))
    val validSchemas = Table(
      "documents",
      Seq("{a:[{b:1, c:2}]}", "{a: [{d:3, e:4}]}"),
      Seq("{a:[{b:1, c:2}, {d:3, e:4}]}"),
      Seq("{a:[{b:1, c:2}, {d:3}, {e:4}]}"),
      Seq("{a:[{b:1, c:2}, {}, {e:3}, {d:4, e:5}]}")
    )

    forAll(validSchemas) { documents =>
      sc.parallelize(documents.map(BsonDocument.parse)).saveToMongoDB()
      MongoInferSchema(sc) should equal(expectedSchema)
      database.drop()
    }
  }

  it should "be able to infer the schema from arrays with mixed numerics" in withSparkContext() { sc =>
    val _idField: StructField = createStructField("_id", BsonCompatibility.ObjectId.structType, true)
    val arrayField = createStructField("a", createArrayType(DataTypes.DoubleType, true), true)
    val expectedSchema = createStructType(Array(_idField, arrayField))
    val documents = Seq("{a: [1, 2, 3.0]}")
    val validSchemas = Table(
      "documents",
      Seq("{a: [1, 2, 3.0]}"),
      Seq("{a:[1, 2, 3]}", "{a:[1, 2.0, 3]}")
    )

    forAll(validSchemas) { documents =>
      sc.parallelize(documents.map(BsonDocument.parse)).saveToMongoDB()
      MongoInferSchema(sc) should equal(expectedSchema)
      database.drop()
    }
  }

  it should "be able to infer the schema from nested arrays with mixed keys" in withSparkContext() { sc =>
    val _idField: StructField = createStructField("_id", BsonCompatibility.ObjectId.structType, true)
    val elements = createStructType(Seq("b", "c", "d", "e").map(createStructField(_, DataTypes.IntegerType, true)).toArray)
    val arrayField = createStructField("a", createArrayType(createArrayType(elements, true), true), true)
    val expectedSchema = createStructType(Array(_idField, arrayField))
    val validSchemas = Table(
      "documents",
      Seq("{a:[[{b:1, c:2}]]}", "{a: [[{d:3, e:4}]]}"),
      Seq("{a:[[{b:1, c:2}, {d:3, e:4}]]}"),
      Seq("{a:[[], [{b:1}], [{c:2}], [{}], [{e:3}], [{d:4, e:5}]]}")
    )

    forAll(validSchemas) { documents =>
      sc.parallelize(documents.map(BsonDocument.parse)).saveToMongoDB()
      MongoInferSchema(sc) should equal(expectedSchema)
      database.drop()
    }
  }

  it should "still mark incompatible schemas with a ConflictType" in withSparkContext() { sc =>
    val _idField: StructField = createStructField("_id", BsonCompatibility.ObjectId.structType, true)
    val conflictSchema = createStructType(Array(_idField, createStructField("a", ConflictType, true)))
    val conflictingSchemas = Table("documents", Seq("{a:[{b:1, c:2}]}", "{a: {b: 1}}"), Seq("{a:[{b:1, c:2}, {d:3, e:4}, [{b: 1}]]}"))

    forAll(conflictingSchemas) { documents =>
      sc.parallelize(documents.map(BsonDocument.parse)).saveToMongoDB()
      MongoInferSchema(sc) should equal(conflictSchema)
      database.drop()
    }
  }

  implicit class DocHelpers(val pipeline: Seq[Bson]) {
    def toBson: Seq[BsonDocument] =
      pipeline.map(_.toBsonDocument(classOf[BsonDocument], MongoClient.getDefaultCodecRegistry))
  }

}
