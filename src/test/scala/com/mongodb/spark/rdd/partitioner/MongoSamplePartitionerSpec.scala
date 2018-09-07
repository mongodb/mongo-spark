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

class MongoSamplePartitionerSpec extends RequiresMongoDB {

  private val partitionKey = "_id"
  private val pipeline = Array.empty[BsonDocument]

  // scalastyle:off magic.number
  "MongoSamplePartitioner" should "partition the database as expected" in {
    if (!serverAtLeast(3, 2)) cancel("MongoDB < 3.2")
    loadSampleData(10)

    val rightHandBoundaries = (1 to 100 by 10).map(x => new BsonString(f"$x%05d"))
    val locations = PartitionerHelper.locations(MongoConnector(sparkConf))
    val expectedPartitions = PartitionerHelper.createPartitions(partitionKey, rightHandBoundaries, locations)
    val partitions = MongoSamplePartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("partitionSizeMB" -> "1")), pipeline
    )

    partitions should equal(expectedPartitions)

    val singlePartition = PartitionerHelper.createPartitions(partitionKey, Seq.empty[BsonValue], locations)
    val largerSizedPartitions = MongoSamplePartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("partitionSizeMB" -> "10")), pipeline
    )
    largerSizedPartitions should equal(singlePartition)
  }

  it should "partition on an alternative shardkey as expected" in {
    if (!serverAtLeast(3, 2)) cancel("MongoDB < 3.2")
    loadSampleData(10)

    val rightHandBoundaries = (1 to 100 by 10).map(x => new BsonString(f"$x%05d"))
    val locations = PartitionerHelper.locations(MongoConnector(sparkConf))
    val expectedPartitions = PartitionerHelper.createPartitions("pk", rightHandBoundaries, locations)
    val partitions = MongoSamplePartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("partitionSizeMB" -> "1", "partitionKey" -> "pk")), pipeline
    )

    partitions should equal(expectedPartitions)
  }

  it should "partition with a composite key" in {
    if (!serverAtLeast(3, 2)) cancel("MongoDB < 3.2")
    loadSampleDataCompositeKey(10)

    val rightHandBoundaries = (1 to 100 by 10).map(x =>
      new BsonDocument("a", new BsonString(f"$x%05d")).append("b", new BsonString(f"${x % 2}%05d")))
    val locations = PartitionerHelper.locations(MongoConnector(sparkConf))
    val expectedPartitions = PartitionerHelper.createPartitions("_id", rightHandBoundaries, locations)
    val partitions = MongoSamplePartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("partitionSizeMB" -> "1", "partitionKey" -> "_id")), pipeline
    )

    partitions should equal(expectedPartitions)
  }

  it should "use the users pipeline when set in a rdd / dataframe" in withSparkSession() { spark =>
    if (!serverAtLeast(3, 2)) cancel("MongoDB < 3.2")
    val numberOfDocuments = 10000
    loadSampleData(10, numberOfDocuments)

    val readConf = readConfig.copy(partitioner = MongoSamplePartitioner, partitionerOptions = Map("partitionSizeMB" -> "1"))
    val rangePipeline = BsonDocument.parse(s"""{$$match: { $partitionKey: {$$gte: "00001", $$lt: "00031"}}}""")

    val rdd = MongoSpark.load(spark.sparkContext, readConf).withPipeline(Seq(rangePipeline))
    rdd.count() should equal(30)
    rdd.partitions.length should equal(1)

    val df = MongoSpark.load(spark, readConf).filter(s"""$partitionKey < "02041"""")
    df.count() should equal(2040)
    df.rdd.partitions.length should equal(3)

    val df2 = MongoSpark.load(spark, readConf).filter(s"""$partitionKey >= "00000"""")
    df2.count() should equal(numberOfDocuments)
  }
  // scalastyle:on magic.number

  it should "have a default bounds of min to max key" in {
    if (!serverAtLeast(3, 2)) cancel("MongoDB < 3.2")
    collection.insertOne(new Document())
    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoSamplePartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }

  it should "handle no collection" in {
    if (!serverAtLeast(3, 2)) cancel("MongoDB < 3.2")
    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoSamplePartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }

  it should "handle an empty collection" in {
    if (!serverAtLeast(3, 2)) cancel("MongoDB < 3.2")
    collection.insertOne(new Document())
    collection.deleteMany(new Document())
    val expectedPartitions = MongoSinglePartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoSamplePartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }

  private def getQueryBoundForKey(partition: MongoPartition, key: String): String =
    partition.queryBounds.getDocument(partitionKey).getString(key).getValue

}
