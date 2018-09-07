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

import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.StructType
import org.bson.Document

class DataFrameReaderSpec extends DataSourceSpecBase {
  // scalastyle:off magic.number

  "Read support" should "be easily created from the Spark session and load from Mongo" in withSparkSession() { spark =>
    spark.save(characters)
    val df = spark.load()

    df.schema should equal(expectedSchema)
    df.count() should equal(10)
    df.filter("age > 100").count() should equal(6)
  }

  it should "handle decimals with scales greater than the precision" in withSparkSession() { spark =>
    if (!serverAtLeast(3, 4)) cancel("MongoDB < 3.4")
    val data =
      """
        |{"_id":"1", "a": {"$numberDecimal":"0.00"}},
        |{"_id":"2", "a": {"$numberDecimal":"0E-14"}}
      """.trim.stripMargin.split("[\\r\\n]+").toSeq.map(Document.parse)

    spark.save(data)

    val df = spark.load()
    df.count() should equal(2)
  }

  it should "handle selecting out of order columns" in withSparkSession() { spark =>
    spark.save(characters)

    forAll(spark.readConfigs) { readConfig: ReadConfig =>
      val df = spark.load(readConfig)
      df.select("name", "age").orderBy("age").rdd.map(r => (r.get(0), r.get(1))).collect() should
        equal(characters.sortBy(_.getInteger("age", 0)).map(doc => (doc.getString("name"), doc.getInteger("age"))))
    }
  }

  it should "handle mixed numerics with long precedence" in withSparkSession() { spark =>
    spark.save(mixedLong)
    val expectedData = List(1L, 1L, 1L)
    val df = spark.load().select("a")

    df.count() should equal(3)
    df.collect().map(r => r.get(0)) should equal(expectedData)

    val cached = df.cache()
    cached.count() should equal(3)
    cached.collect().map(r => r.get(0)) should equal(expectedData)
  }

  it should "handle mixed numerics with double precedence" in withSparkSession() { spark =>
    spark.save(mixedDouble)
    val expectedData = List(1.0, 1.0, 1.0)
    val df = spark.load().select("a")

    df.count() should equal(3)
    df.collect().map(r => r.get(0)) should equal(expectedData)

    val cached = df.cache()
    cached.count() should equal(3)
    cached.collect().map(r => r.get(0)) should equal(expectedData)
  }

  it should "handle array fields with null values" in withSparkSession() { spark =>
    import spark.implicits._
    val arrayFieldsWithNulls = Seq(ContainsList(1, List(1, 2, 3)), ContainsList(2, List()), ContainsList(3, null)) // scalastyle:ignore
    arrayFieldsWithNulls.toDS().save()

    spark.loadDS[ContainsList]().collect().toList should equal(arrayFieldsWithNulls)
  }

  it should "handle document fields with null values" in withSparkSession() { spark =>
    import spark.implicits._
    val structFieldWithNulls = Seq(
      ContainsMap(1, Map[String, Int]("a" -> 1)),
      ContainsMap(2, Map[String, Int]()), ContainsMap(3, null)
    ) // scalastyle:ignore
    structFieldWithNulls.toDS().save()

    spark.loadDS[ContainsMap]().collect().toList should equal(structFieldWithNulls)
  }

  it should "be easily created with a provided case class" in withSparkSession() { spark =>
    spark.save(charactersWithAge)

    forAll(spark.readConfigs) { readConfig: ReadConfig =>
      val df = spark.loadDS[Character](readConfig)
      val reflectedSchema: StructType = ScalaReflection.schemaFor[Character].dataType.asInstanceOf[StructType]
      df.count() should equal(9)
      df.filter("age > 100").count() should equal(6)
      df.schema should equal(reflectedSchema)
    }
  }

  it should "include user pipelines when inferring the schema" in withSparkSession() { spark =>
    spark.save(characters)
    spark.save(List("{counter: 1}", "{counter: 2}", "{counter: 3}").map(Document.parse))

    forAll(spark.readConfigs) { readConfig: ReadConfig =>
      val df = spark.load(readConfig.withOption("pipeline", "[{ $match: { age: { $exists: true } } }]"))
      df.schema should equal(expectedSchema)
      df.count() should equal(9)
      df.filter("age > 100").count() should equal(6)

      val df2 = spark.load(readConfig.withOption("pipeline", "[{ $project: { _id: 1, age: 1 } }]"))
      df2.schema should equal(createStructType(expectedSchema.fields.filter(p => p.name != "name")))
    }
  }

  it should "throw an exception if pipeline is invalid" in withSparkSession() { spark =>
    spark.save(characters)
    spark.save(List("{counter: 1}", "{counter: 2}", "{counter: 3}").map(Document.parse))

    an[IllegalArgumentException] should be thrownBy spark.load(readConfig.withOption("pipeline", "[1, 2, 3]"))
  }

  private val mixedLong: Seq[Document] = Seq(new Document("a", 1), new Document("a", 1), new Document("a", 1L))
  private val mixedDouble: Seq[Document] = Seq(new Document("a", 1), new Document("a", 1L), new Document("a", 1.0))

  // scalastyle:on magic.number
}

case class ContainsList(_id: Int, list: List[Int])
case class ContainsMap(_id: Int, map: Map[String, Int])
