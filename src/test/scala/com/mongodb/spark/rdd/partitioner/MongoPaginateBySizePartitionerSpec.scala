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

import org.apache.spark.sql.SparkSession

import org.bson._
import com.mongodb.spark.{MongoConnector, MongoSpark, RequiresMongoDB}

class MongoPaginateBySizePartitionerSpec extends RequiresMongoDB {

  private val partitionKey = "_id"
  private val pipeline = Array.empty[BsonDocument]

  // scalastyle:off magic.number
  "MongoPaginateBySizePartitioner" should "partition the database as expected" in {
    if (!serverAtLeast(3, 2)) cancel("Testing on MongoDB 3.2+, so to have predictable partition sizes.")
    loadSampleData(10)

    val rightHandBoundaries = (1 to 100 by 10).map(x => new BsonString(f"$x%05d")) :+ new BsonString("00100")
    val locations = PartitionerHelper.locations(MongoConnector(sparkConf))
    val expectedPartitions = PartitionerHelper.createPartitions(partitionKey, rightHandBoundaries, locations)
    val partitions = MongoPaginateBySizePartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("partitionSizeMB" -> "1")), pipeline
    )

    partitions should equal(expectedPartitions)
    val singlePartition = PartitionerHelper.createPartitions(partitionKey, Seq.empty[BsonValue], locations)
    val largerSizedPartitions = MongoPaginateBySizePartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("partitionSizeMB" -> "10")), pipeline
    )
    largerSizedPartitions should equal(singlePartition)
  }

  it should "use the provided pipeline for min and max keys" in withSparkContext() { sc =>
    if (!serverAtLeast(3, 2)) cancel("Testing on MongoDB 3.2+, so to have predictable partition sizes.")
    loadSampleData(10)

    val readConf = readConfig.copy(partitioner = MongoPaginateBySizePartitioner, partitionerOptions = Map("partitionSizeMB" -> "1"))
    val rangePipeline = Array(BsonDocument.parse(s"""{$$match: { $partitionKey: {$$gte: "00001", $$lt: "00031"}}}"""))

    val rangePartitions = MongoPaginateBySizePartitioner.partitions(mongoConnector, readConf, rangePipeline)
    getQueryBoundForKey(rangePartitions.head, "$gte") should equal("00001")
    getQueryBoundForKey(rangePartitions.reverse.head, "$lte") should equal("00030")
    rangePartitions.length should equal(3)
  }

  it should "use the users pipeline when set in a rdd / dataframe" in withSparkContext() { sc =>
    if (!serverAtLeast(3, 2)) cancel("Testing on MongoDB 3.2+, so to have predictable partition sizes.")
    loadSampleData(10)

    val readConf = readConfig.copy(partitioner = MongoPaginateBySizePartitioner, partitionerOptions = Map("partitionSizeMB" -> "1"))
    val rangePipeline = BsonDocument.parse(s"""{$$match: { $partitionKey: {$$gte: "00001", $$lt: "00031"}}}""")
    val sparkSession = SparkSession.builder().getOrCreate()
    val rdd = MongoSpark.load(sparkSession.sparkContext, readConf).withPipeline(Seq(rangePipeline))
    rdd.count() should equal(30)
    rdd.partitions.length should equal(3)

    val df = MongoSpark.load(sparkSession, readConf).filter(s"""$partitionKey >= "00001" AND  $partitionKey < "00031"""")
    df.count() should equal(30)
    df.rdd.partitions.length should equal(3)
  }
  // scalastyle:on magic.number

  it should "have a default bounds of min to max key" in {
    collection.insertOne(new Document())
    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoPaginateBySizePartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }

  it should "handle no collection" in {
    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoPaginateBySizePartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }

  it should "handle an empty collection" in {
    collection.insertOne(new Document())
    collection.deleteMany(new Document())
    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoPaginateBySizePartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }

  private def getQueryBoundForKey(partition: MongoPartition, key: String): String =
    partition.queryBounds.getDocument(partitionKey).getString(key).getValue

}

