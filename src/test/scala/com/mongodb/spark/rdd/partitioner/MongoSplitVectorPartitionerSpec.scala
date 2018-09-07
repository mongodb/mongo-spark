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

import com.mongodb.spark.{MongoConnector, MongoSpark, RequiresMongoDB}
import org.apache.spark.sql.SparkSession
import org.bson._

class MongoSplitVectorPartitionerSpec extends RequiresMongoDB {

  private val partitionKey = "_id"
  private val pipeline = Array.empty[BsonDocument]

  // scalastyle:off magic.number
  "MongoSplitVectorPartitioner" should "partition the database as expected" in {
    if (isSharded) cancel("Sharded MongoDB")
    loadSampleData(5)

    val locations = PartitionerHelper.locations(MongoConnector(sparkConf))
    val partitions = MongoSplitVectorPartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("partitionSizeMB" -> "1")), pipeline
    )
    val zipped: Array[(MongoPartition, MongoPartition)] = partitions zip partitions.tail
    zipped.foreach({
      case (lt, gte) => lt.queryBounds.get("lt") should equal(gte.queryBounds.get("gte"))
    })
    partitions.length should be >= 5
    partitions.head.locations should equal(locations)

    MongoSplitVectorPartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("partitionSizeMB" -> "10")), pipeline
    ).length shouldBe 1
  }

  it should "use the provided pipeline for min and max keys" in {
    loadSampleData(10)

    val readConf = readConfig.copy(partitionerOptions = Map("partitionSizeMB" -> "1"))
    val rangePipeline = Array(BsonDocument.parse(s"""{$$match: { $partitionKey: {$$gte: "00001", $$lt: "00022"}}}"""))

    val rangePartitions = MongoSplitVectorPartitioner.partitions(mongoConnector, readConf, rangePipeline)
    getQueryBoundForKey(rangePartitions.head, "$gte") should equal("00001")
    getQueryBoundForKey(rangePartitions.reverse.head, "$lt") should equal("00022")
    rangePartitions.length should (be >= 4 and be <= 5)
  }

  it should "use the users pipeline when set in a rdd / dataframe" in withSparkSession() { spark =>
    if (!serverAtLeast(3, 2)) cancel("Testing on MongoDB 3.2+, so to have predictable partition sizes.")
    val numberOfDocuments = 100
    loadSampleData(10, numberOfDocuments)

    val readConf = readConfig.copy(partitioner = MongoSplitVectorPartitioner, partitionerOptions = Map("partitionSizeMB" -> "1"))
    val rangePipeline = BsonDocument.parse(s"""{$$match: { $partitionKey: {$$gte: "00001", $$lt: "00031"}}}""")
    val rdd = MongoSpark.load(spark.sparkContext, readConf).withPipeline(Seq(rangePipeline))
    rdd.count() should equal(30)
    rdd.partitions.length should equal(6)

    val df = MongoSpark.load(spark, readConf).filter(s"""$partitionKey >= "00001" AND  $partitionKey < "00031"""")
    df.count() should equal(30)
    df.rdd.partitions.length should equal(6)
    val dfPartitions = df.rdd.partitions.toList
    getQueryBoundForKey(dfPartitions.head.asInstanceOf[MongoPartition], "$gte") should equal("00001")
    getQueryBoundForKey(dfPartitions.reverse.head.asInstanceOf[MongoPartition], "$lt") should equal("00031")

    val df2 = MongoSpark.load(spark, readConf).filter(s"""$partitionKey < "00049"""")
    df2.count() should equal(48)
    df2.rdd.partitions.length should equal(9)
    val df2Partitions = df2.rdd.partitions.toList
    Option(getBsonValueQueryBoundForKey(df2Partitions.head.asInstanceOf[MongoPartition], "$gte")) should equal(None)
    getQueryBoundForKey(df2Partitions.reverse.head.asInstanceOf[MongoPartition], "$lt") should equal("00049")

    val df3 = MongoSpark.load(spark, readConf).filter(s"""$partitionKey >= "00051"""")
    df3.count() should equal(50)
    df3.rdd.partitions.length should equal(9)
    val df3Partitions = df3.rdd.partitions.toList
    getQueryBoundForKey(df3Partitions.head.asInstanceOf[MongoPartition], "$gte") should equal("00051")
    Option(getBsonValueQueryBoundForKey(df3Partitions.reverse.head.asInstanceOf[MongoPartition], "$lt")) should equal(None)
  }
  // scalastyle:on magic.number

  it should "have a default bounds of min to max key" in {
    collection.insertOne(new Document())
    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoSplitVectorPartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }

  it should "handle no collection" in {
    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoSplitVectorPartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }

  it should "handle an empty collection" in {
    collection.insertOne(new Document())
    collection.deleteMany(new Document())
    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoSplitVectorPartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }

  private def getQueryBoundForKey(partition: MongoPartition, key: String): String =
    getBsonValueQueryBoundForKey(partition, key).asString().getValue

  private def getBsonValueQueryBoundForKey(partition: MongoPartition, key: String): BsonValue =
    partition.queryBounds.getDocument(partitionKey).get(key)

}
