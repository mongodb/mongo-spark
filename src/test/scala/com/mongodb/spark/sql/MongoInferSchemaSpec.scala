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

import org.scalatest.FlatSpec

import org.bson.conversions.Bson
import org.bson.{BsonDocument, Document}
import com.mongodb.MongoClient
import com.mongodb.spark._
import com.mongodb.spark.rdd.MongoRDD

class MongoInferSchemaSpec extends FlatSpec with MongoDataGenerator with RequiresMongoDB {

  "MongoSchemaHelper" should "be able to infer the schema from simple types" in withSparkContext() { sc =>
    forAll(genSimpleDataTypes) { (datum: Seq[MongoDataType]) =>
      datum.foreach { data =>
        sc.parallelize(data.getDocuments.toBson).saveToMongoDB()
        data.schema should equal(MongoInferSchema(sc))
        sc.dropDatabase()
      }
    }
  }

  it should "be able to infer the schema from a flat array" in withSparkContext() { sc =>
    forAll(genArrayDataType(0)) { (data: MongoDataType) =>
      sc.parallelize(data.getDocuments.toBson).saveToMongoDB()
      data.schema should equal(MongoInferSchema(sc))
      sc.dropDatabase()
    }
  }

  it should "be able to infer the schema from a flat document" in withSparkContext() { sc =>
    forAll(genDocumentDataType(0)) { (data: MongoDataType) =>
      sc.parallelize(data.getDocuments.toBson).saveToMongoDB()
      data.schema should equal(MongoInferSchema(sc))
      sc.dropDatabase()
    }
  }

  it should "be able to infer the schema from a nested array" in withSparkContext() { sc =>
    forAll(genArrayDataType()) { (data: MongoDataType) =>
      sc.parallelize(data.getDocuments.toBson).saveToMongoDB()
      data.schema should equal(MongoInferSchema(sc))
      sc.dropDatabase()
    }
  }

  it should "be able to infer the schema from a multi level document" in withSparkContext() { sc =>
    forAll(genDocumentDataType()) { (data: MongoDataType) =>
      sc.parallelize(data.getDocuments.toBson).saveToMongoDB()
      data.schema should equal(MongoInferSchema(sc))
      sc.dropDatabase()
    }
  }

  it should "be able to infer the schema with custom sampleSize" in withSparkContext() { sc =>
    forAll(genDocumentDataType()) { (data: MongoDataType) =>
      sc.parallelize(data.getDocuments.toBson).saveToMongoDB()
      data.schema should equal(MongoInferSchema(MongoRDD[BsonDocument](sc, readConfig.copy(sampleSize = 200)))) // scalastyle:ignore
      sc.dropDatabase()
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
      sc.dropDatabase()
    }
  }

  it should "use any set pipelines on the RDD" in withSparkContext() { sc =>
    forAll(genDocumentDataType(0)) { (data: MongoDataType) =>
      val allDocs = data.getDocuments ++ Seq("{badData: [1 ,2, 3]}", "{badData: 55}", "{badData: 'bad'}").map(Document.parse)
      sc.parallelize(allDocs.toBson).saveToMongoDB()

      val rdd = MongoRDD[BsonDocument](sc).withPipeline(Seq(Document.parse("{ $match: { badData : { $exists : false } } }")))
      data.schema should equal(MongoInferSchema(rdd))
      sc.dropDatabase()
    }
  }

  implicit class DocHelpers(val pipeline: Seq[Bson]) {
    def toBson: Seq[BsonDocument] =
      pipeline.map(_.toBsonDocument(classOf[BsonDocument], MongoClient.getDefaultCodecRegistry))
  }

}
