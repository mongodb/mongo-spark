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

package com.mongodb.spark

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.bson.{BsonDocument, Document}
import com.mongodb.client.model.{Aggregates, Collation, CollationStrength, Filters}
import com.mongodb.spark.config.{AggregationConfig, ReadConfig}
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.sql.types.BsonCompatibility

class MongoRDDSpec extends RequiresMongoDB {
  val counters =
    """
      | {counter: 0}
      | {counter: 1}
      | {counter: 2}
    """.stripMargin.trim.stripMargin.split("[\\r\\n]+").toSeq

  "MongoRDD" should "be easily created from the SparkContext" in withSparkContext() { sc =>
    sc.parallelize(counters.map(Document.parse)).saveToMongoDB()
    val mongoRDD: MongoRDD[Document] = sc.loadFromMongoDB()

    mongoRDD.count() shouldBe 3
    mongoRDD.map(x => x.getInteger("counter")).collect() should contain theSameElementsInOrderAs Seq(0, 1, 2)
  }

  it should "be able to handle non existent collections" in withSparkContext() { sc =>
    sc.loadFromMongoDB().count() shouldBe 0
  }

  it should "be able to query via a pipeline" in withSparkContext() { sc =>
    sc.parallelize(counters.map(Document.parse)).saveToMongoDB()

    sc.loadFromMongoDB().withPipeline(List(Document.parse("{$match: { counter: {$gt: 0}}}"))).count() shouldBe 2
    sc.loadFromMongoDB().withPipeline(List(BsonDocument.parse("{$match: { counter: {$gt: 0}}}"))).count() shouldBe 2
    sc.loadFromMongoDB().withPipeline(List(Aggregates.`match`(Filters.gt("counter", 0)))).count() shouldBe 2
  }

  it should "be able to handle different collection types" in withSparkContext() { sc =>
    sc.parallelize(counters.map(Document.parse)).saveToMongoDB()

    val mongoRDD: MongoRDD[BsonDocument] = sc.loadFromMongoDB[BsonDocument]()
    mongoRDD.count() shouldBe 3
  }

  it should "be able to create a DataFrame by inferring the schema" in withSparkContext() { sc =>
    sc.parallelize(counters.map(Document.parse)).saveToMongoDB()

    val _idField: StructField = createStructField("_id", BsonCompatibility.ObjectId.structType, true)
    val countField: StructField = createStructField("counter", DataTypes.IntegerType, true)
    val expectedSchema: StructType = createStructType(Array(_idField, countField))

    val dataFrame: DataFrame = sc.loadFromMongoDB().toDF()
    dataFrame.schema should equal(expectedSchema)
    dataFrame.count() should equal(3)
  }

  it should "be able to create a DataFrame when provided a case class" in withSparkContext() { sc =>
    sc.parallelize(counters.map(Document.parse)).saveToMongoDB()

    val expectedSchema: StructType = ScalaReflection.schemaFor[Counter].dataType.asInstanceOf[StructType]
    val dataFrame: DataFrame = sc.loadFromMongoDB().toDF[Counter]()
    dataFrame.schema should equal(expectedSchema)
    dataFrame.count() should equal(3)
  }

  it should "be able to create a DataFrame with a set schema" in withSparkContext() { sc =>
    sc.parallelize(counters.map(Document.parse)).saveToMongoDB()

    val _idField: StructField = createStructField("_id", BsonCompatibility.ObjectId.structType, true)
    val countField: StructField = createStructField("counter", DataTypes.IntegerType, true)
    val schema: StructType = createStructType(Array(_idField, countField))

    val dataFrame: DataFrame = sc.loadFromMongoDB().toDF(schema)
    dataFrame.schema should equal(sc.loadFromMongoDB().toDF().schema)
  }

  it should "be able to create a Dataset when provided a case class" in withSparkContext() { sc =>
    sc.parallelize(counters.map(Document.parse)).saveToMongoDB()

    val expectedSchema: StructType = ScalaReflection.schemaFor[Counter].dataType.asInstanceOf[StructType]
    val dataset: Dataset[Counter] = sc.loadFromMongoDB().toDS[Counter]()
    dataset.schema should equal(expectedSchema)
    dataset.count() should equal(3)
  }

  it should "not allow Nothing when trying to create a Dataset" in withSparkContext() { sc =>
    sc.parallelize(counters.map(Document.parse)).saveToMongoDB()

    "sc.loadFromMongoDB().toDS()" shouldNot compile
    "sc.loadFromMongoDB().toDS[Nothing]()" shouldNot compile
  }

  it should "throw when creating a Dataset with invalid data" in withSparkContext() { sc =>
    sc.parallelize(List(Document.parse("{counter: 'a'}"), Document.parse("{counter: 'b'}"))).saveToMongoDB()
    val dataset: Dataset[Counter] = sc.loadFromMongoDB().toDS[Counter]()

    import dataset.sqlContext.implicits._
    an[SparkException] should be thrownBy dataset.map(counter => counter.counter).collectAsList()
  }

  it should "use default values when creating a Dataset with missing data" in withSparkContext() { sc =>
    sc.parallelize(List(Document.parse("{name: 'a'}"), Document.parse("{name: 'b'}"))).saveToMongoDB()
    val dataset: Dataset[Counter] = sc.loadFromMongoDB().toDS[Counter]()
    import dataset.sqlContext.implicits._
    dataset.map(counter => counter.counter).collectAsList() should contain theSameElementsAs List(None, None)
  }

  it should "be easy to use a custom partitioner" in withSparkContext() { sc =>
    sc.parallelize((1 to 100).map(i => Document.parse(s"{number: $i}"))).saveToMongoDB()

    val mongoRDD: MongoRDD[Document] = MongoSpark.builder()
      .sparkContext(sc)
      .option("partitioner", "com.mongodb.spark.HalfwayPartitioner")
      .build()
      .toRDD()
    mongoRDD.getNumPartitions should equal(2)
    mongoRDD.mapPartitions(iter => Array(iter.size).iterator).collect() should equal(Array(50, 50)) // scalastyle:ignore
  }

  it should "be easy to use a custom partitioner that is an object " in withSparkContext() { sc =>
    sc.parallelize((1 to 100).map(i => Document.parse(s"{number: $i}"))).saveToMongoDB()

    val mongoRDD: MongoRDD[Document] = MongoSpark.builder()
      .sparkContext(sc)
      .option("partitioner", "com.mongodb.spark.HalfwayPartitioner$")
      .build()
      .toRDD()
    mongoRDD.getNumPartitions should equal(2)
    mongoRDD.mapPartitions(iter => Array(iter.size).iterator).collect() should equal(Array(50, 50)) // scalastyle:ignore
  }

  it should "support collation" in withSparkContext() { sc =>
    if (!serverAtLeast(3, 4)) cancel("MongoDB < 3.4") // scalastyle:ignore
    sc.parallelize(Seq("{_id: 1, str: 'foo'}", "{_id: 2, str: 'FOO'}").map(Document.parse)).saveToMongoDB()

    val caseInsensitiveCollation = Collation.builder().locale("en").collationStrength(CollationStrength.SECONDARY).build()

    sc.loadFromMongoDB().withPipeline(Seq(Document.parse("{$match: {str: 'FOO'}}"))).count() should equal(1)
    sc.loadFromMongoDB(ReadConfig(sc).copy(aggregationConfig = AggregationConfig(caseInsensitiveCollation)))
      .withPipeline(Seq(Document.parse("{$match: {str: 'FOO'}}"))).count() should equal(2)
  }

}
