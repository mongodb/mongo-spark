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

import org.scalatest.FlatSpec

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import org.bson.Document
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.{RequiresMongoDB, _}

class MongoDataFrameSpec extends FlatSpec with RequiresMongoDB {
  // scalastyle:off magic.number

  val characters = """
     | {"name": "Bilbo Baggins", "age": 50}
     | {"name": "Gandalf", "age": 1000}
     | {"name": "Thorin", "age": 195}
     | {"name": "Balin", "age": 178}
     | {"name": "Kíli", "age": 77}
     | {"name": "Dwalin", "age": 169}
     | {"name": "Óin", "age": 167}
     | {"name": "Glóin", "age": 158}
     | {"name": "Fíli", "age": 82}
     | {"name": "Bombur"}""".trim.stripMargin.split("[\\r\\n]+").toSeq

  "DataFrameReader" should "should be easily created from the SQLContext and load from Mongo" in withSparkContext() { sc =>
    sc.parallelize(characters.map(Document.parse)).saveToMongoDB()
    val df = new SQLContext(sc).read.mongo()

    df.schema should equal(expectedSchema)
    df.count() should equal(10)
    df.filter("age > 100").count() should equal(6)
  }

  it should "be easily created with a provided case class" in withSparkContext() { sc =>
    sc.parallelize(characters.map(Document.parse)).saveToMongoDB()

    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.mongo[Character]()
    val reflectedSchema: StructType = ScalaReflection.schemaFor[Character].dataType.asInstanceOf[StructType]

    df.schema should equal(reflectedSchema)
    df.count() should equal(10)
    df.filter("age > 100").count() should equal(6)
  }

  it should "include any pipelines when inferring the schema" in withSparkContext() { sc =>
    sc.parallelize(characters.map(json => Document.parse(json))).saveToMongoDB()
    sc.parallelize(List("{counter: 1}", "{counter: 2}", "{counter: 3}").map(Document.parse)).saveToMongoDB()
    val sqlContext = new SQLContext(sc)

    var df = sqlContext.read.option("pipeline", "[{ $match: { name: { $exists: true } } }]").mongo()
    df.schema should equal(expectedSchema)
    df.count() should equal(10)
    df.filter("age > 100").count() should equal(6)

    df = sqlContext.read.option("pipeline", "{ $match: { name: { $exists: true } } }").mongo()
    df.schema should equal(expectedSchema)
    df.count() should equal(10)
    df.filter("age > 100").count() should equal(6)
  }

  it should "throw an exception if pipeline is invalid" in withSparkContext() { sc =>
    sc.parallelize(characters.map(json => Document.parse(json))).saveToMongoDB()
    sc.parallelize(List("{counter: 1}", "{counter: 2}", "{counter: 3}").map(Document.parse)).saveToMongoDB()

    an[IllegalArgumentException] should be thrownBy new SQLContext(sc).read.option("pipeline", "[1, 2, 3]").mongo()
  }

  "DataFrameWriter" should "be easily created from a DataFrame and save to Mongo" in withSparkContext() { sc =>
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sc.parallelize(characters.map(Document.parse))
      .filter(_.containsKey("age")).map(doc => Character(doc.getString("name"), doc.getInteger("age")))
      .toDF().write.mongo()

    sqlContext.read.mongo[Character]().count() should equal(9)
  }

  it should "take custom writeConfig" in withSparkContext() { sc =>
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val saveToCollectionName = s"${collectionName}_new"
    val writeConfig = WriteConfig(sc.getConf).withOptions(Map("collectionName" -> saveToCollectionName))

    sc.parallelize(characters.map(Document.parse))
      .filter(_.containsKey("age")).map(doc => Character(doc.getString("name"), doc.getInteger("age")))
      .toDF().write.mongo(writeConfig)

    sqlContext.read.option("collectionName", saveToCollectionName).mongo[Character]().count() should equal(9)
  }

  private val expectedSchema: StructType = {
    val _idField: StructField = createStructField("_id", DataTypes.StringType, true)
    val nameField: StructField = createStructField("name", DataTypes.StringType, true)
    val ageField: StructField = createStructField("age", DataTypes.IntegerType, true)
    createStructType(Array(_idField, ageField, nameField))
  }

  // scalastyle:on magic.number
}
