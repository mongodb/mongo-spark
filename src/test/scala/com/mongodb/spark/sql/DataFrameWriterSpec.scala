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

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.IndexOptions
import com.mongodb.spark.{MongoConnector, MongoSpark}
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.SparkException
import org.bson._

class DataFrameWriterSpec extends DataSourceSpecBase {
  // scalastyle:off magic.number

  "Write Support" should "be easily created from a DataFrame and save to Mongo" in withSparkSession() { spark =>
    import spark.implicits._

    spark.parallelize(characters)
      .filter(_.containsKey("age")).map(doc => Character(doc.getString("name"), doc.getInteger("age")))
      .toDF().save()

    spark.loadDS[Character]().count() should equal(9)
  }

  it should "take custom writeConfig" in withSparkSession() { spark =>
    import spark.implicits._
    val saveToCollectionName = s"${collectionName}_new"
    val writeConfig = WriteConfig(spark.sparkContext.getConf).withOptions(Map("collection" -> saveToCollectionName))

    spark.parallelize(characters)
      .filter(_.containsKey("age")).map(doc => Character(doc.getString("name"), doc.getInteger("age")))
      .toDF().save(writeConfig)

    spark.read().option("collection", saveToCollectionName).load().as[Character].count() should equal(9)
  }

  it should "support INSERT INTO SELECT statements" in withSparkSession() { spark =>
    val df = spark.loadDS[Character]()
    df.createOrReplaceTempView("people")
    spark.sql("INSERT INTO table people SELECT 'Mort', 1000")

    spark.load().count() should equal(1)
  }

  it should "support INSERT OVERWRITE SELECT statements" in withSparkSession() { spark =>
    import spark.implicits._
    spark.parallelize(characters)
      .filter(_.containsKey("age")).map(doc => Character(doc.getString("name"), doc.getInteger("age")))
      .toDF().save()

    val ds = spark.loadDS[Character]()

    ds.createOrReplaceTempView("people")
    spark.sql("INSERT OVERWRITE table people SELECT 'Mort', 1000")

    spark.load().count() should equal(1)
  }

  it should "be able to upsert and replace data in an existing collection" in withSparkSession() { spark =>
    import spark.implicits._
    val originalData = Seq(SomeData(1, 100), SomeData(2, 200), SomeData(3, 300)).toDS()
    originalData.save()

    spark.loadDS[SomeData]().collect() should contain theSameElementsAs originalData.collect()

    val replacementData = Seq(SomeData(1, 1000), SomeData(2, 2000), SomeData(3, 3000)).toDS()
    replacementData.toDF().createWriter().mode("append").save()

    spark.loadDS[SomeData]().collect() should contain theSameElementsAs replacementData.collect()
  }

  it should "fail when force insert is set to true and data already exists" in withSparkSession() { spark =>
    import spark.implicits._
    val originalData = Seq(SomeData(1, 100), SomeData(2, 200), SomeData(3, 300)).toDS()
    originalData.save()

    an[SparkException] should be thrownBy {
      originalData.toDF().createWriter().mode("append").option(WriteConfig.forceInsertProperty, "true").save()
    }
  }

  it should "be able to handle optional _id fields when upserting / replacing data in a collection" in withSparkSession() { spark =>
    import spark.implicits._
    val originalData = characters.map(doc => CharacterWithOid(None, doc.getString("name"),
      if (doc.containsKey("age")) Some(doc.getInteger("age")) else None))

    originalData.toDS().save()
    val savedData = spark.loadDS[CharacterWithOid]().map(c => (c.name, c.age)).collect()
    originalData.map(c => (c.name, c.age)) should contain theSameElementsAs savedData
  }

  it should "be able honour replaceDocument" in withSparkSession() { spark =>
    import spark.implicits._
    val configPrefix = Table("prefix", WriteConfig.configPrefix, "")

    forAll(configPrefix) { prefix: String =>
      val prefix = ""

      val originalData = Seq(AllData(1, 100, "1"), AllData(2, 200, "2"), AllData(3, 300, "3"))
      val expectedData = originalData.map(d => AllData(d._id, d.count * 10, d.extra))
      val replacementData = originalData.map(d => SomeData(d._id, d.count * 10))

      originalData.toDS().createWriter().mode("overwrite").save()

      replacementData.toDF().createWriter().option(s"$prefix${WriteConfig.replaceDocumentProperty}", "false").save()
      spark.loadDS[AllData]().collect() should contain theSameElementsAs expectedData

      replacementData.toDF().createWriter().option(s"$prefix${WriteConfig.replaceDocumentProperty}", "true").save()
      spark.loadDS[AllData]().collect() should contain theSameElementsAs expectedData.map(d => d.copy(extra = null)) // scalastyle:ignore
    }
  }

  it should "be able to set only the data in the Dataset to the collection" in withSparkSession() { spark =>
    import spark.implicits._
    val writeConfig = WriteConfig(spark.getConf).withOptions(Map("replaceDocument" -> "false"))

    val originalData = characters.map(doc => CharacterWithOid(None, doc.getString("name"),
      if (doc.containsKey("age")) Some(doc.getInteger("age")) else None))

    originalData.toDS().save()

    spark.loadDS[CharacterUpperCaseNames]()
      .map(c => CharacterUpperCaseNames(c._id, c.name.toUpperCase())).save(writeConfig)

    val savedData = spark.loadDS[CharacterWithOid]().map(c => (c.name, c.age)).collect()
    originalData.map(c => (c.name.toUpperCase(), c.age)) should contain theSameElementsAs savedData
  }

  it should "be able to replace data in sharded collections" in withSparkSession() { spark =>
    if (!isSharded) cancel("Not a Sharded MongoDB")
    val shardKey = """{shardKey1: 1, shardKey2: 1}"""
    shardCollection(collectionName, Document.parse(shardKey))

    import spark.implicits._
    val readConfig = ReadConfig(spark.getConf).withOptions(Map("partitionerOptions.shardKey" -> shardKey))
    val writeConfig = WriteConfig(spark.getConf).withOptions(Map("replaceDocument" -> "true", "shardKey" -> shardKey))

    val originalData = characters.zipWithIndex.map {
      case (doc: Document, i: Int) =>
        ShardedCharacter(i, i, i, doc.getString("name"), if (doc.containsKey("age")) Some(doc.getInteger("age")) else None)
    }
    originalData.toDS().saveToMongoDB()

    spark.loadDS[ShardedCharacter](readConfig).map(c => c.copy(age = c.age.map(_ + 10))).save(writeConfig)

    val savedData = spark.spark.loadDS[ShardedCharacter](readConfig).collect()
    originalData.map(c => c.copy(age = c.age.map(_ + 10))) should contain theSameElementsAs savedData
  }

  it should "be able to update data in sharded collections" in withSparkSession() { spark =>
    if (!isSharded) cancel("Not a Sharded MongoDB")
    val shardKey = """{shardKey1: 1, shardKey2: 1}"""
    shardCollection(collectionName, Document.parse(shardKey))

    import spark.implicits._
    val readConfig = ReadConfig(spark.getConf).withOptions(Map("partitionerOptions.shardKey" -> shardKey))
    val writeConfig = WriteConfig(spark.getConf).withOptions(Map("replaceDocument" -> "false", "shardKey" -> shardKey))

    val originalData = characters.zipWithIndex.map {
      case (doc: Document, i: Int) =>
        ShardedCharacter(i, i, i, doc.getString("name"), if (doc.containsKey("age")) Some(doc.getInteger("age")) else None)
    }
    originalData.toDS().saveToMongoDB()

    spark.spark.loadDS[ShardedCharacter](readConfig).map(c => c.copy(age = c.age.map(_ + 10))).save(writeConfig)

    val savedData = spark.spark.loadDS[ShardedCharacter](readConfig).collect()
    originalData.map(c => c.copy(age = c.age.map(_ + 10))) should contain theSameElementsAs savedData
  }

  it should "throw an exception and stop the job when failing a bulk operation" in withSparkSession() { spark =>
    val writeConfig = WriteConfig(spark.getConf)
    MongoConnector(readConfig).withCollectionDo(writeConfig, { coll: MongoCollection[Document] =>
      coll.createIndex(new Document("count", 1), new IndexOptions().unique(true))
    })

    import spark.implicits._
    val ds = spark.createDataset(Seq(SomeData(1, 1), SomeData(2, 2), SomeData(3, 1)))

    an[SparkException] should be thrownBy MongoSpark.save(ds.write.mode("append"), writeConfig)
  }

  // scalastyle:on magic.number
}

case class SomeData(_id: Int, count: Int)

case class AllData(_id: Int, count: Int, extra: String)

case class CharacterWithOid(_id: Option[fieldTypes.ObjectId], name: String, age: Option[Int])

case class CharacterUpperCaseNames(_id: Option[fieldTypes.ObjectId], name: String)

case class ShardedCharacter(_id: Int, shardKey1: Int, shardKey2: Int, name: String, age: Option[Int])

