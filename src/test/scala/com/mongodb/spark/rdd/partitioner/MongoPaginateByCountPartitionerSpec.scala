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
import com.mongodb.spark._

class MongoPaginateByCountPartitionerSpec extends RequiresMongoDB {

  private val partitionKey = "_id"
  private val pipeline = Array.empty[BsonDocument]

  "MongoPaginateByCountPartitioner" should "partition the database as expected" in withSparkContext() { sc =>
    sc.parallelize((0 to 1000).map(i => BsonDocument.parse(s"{ _id: $i }"))).saveToMongoDB()

    val rightHandBoundaries = (0 to 1000 by 100).map(x => new BsonInt32(x))
    val locations = PartitionerHelper.locations(MongoConnector(sparkConf))
    val expectedPartitions = PartitionerHelper.createPartitions(partitionKey, rightHandBoundaries, locations)
    val partitions = MongoPaginateByCountPartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("numberOfPartitions" -> "10")), pipeline
    )

    partitions should equal(expectedPartitions)
  }

  it should "partition on an alternative shardkey as expected" in withSparkContext() { sc =>
    sc.parallelize((0 to 1000).map(i => BsonDocument.parse(s"{ pk: $i }"))).saveToMongoDB()

    val rightHandBoundaries = (0 to 1000 by 100).map(x => new BsonInt32(x))
    val locations = PartitionerHelper.locations(MongoConnector(sparkConf))
    val expectedPartitions = PartitionerHelper.createPartitions("pk", rightHandBoundaries, locations)
    val partitions = MongoPaginateByCountPartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("numberOfPartitions" -> "10", "partitionKey" -> "pk")), pipeline
    )

    partitions should equal(expectedPartitions)
  }

  // scalastyle:off magic.number
  it should "use the users pipeline when set in a rdd / dataframe" in withSparkContext() { sc =>
    val numberOfDocuments = 10000
    loadSampleData(10, numberOfDocuments)

    val readConf = readConfig.copy(partitioner = MongoPaginateByCountPartitioner, partitionerOptions = Map("numberOfPartitions" -> "10"))
    val rangePipeline = BsonDocument.parse(s"""{$$match: { $partitionKey: {$$gte: "00001", $$lt: "00031"}}}""")
    val sparkSession = SparkSession.builder().getOrCreate()
    val rdd = MongoSpark.load(sparkSession.sparkContext, readConf).withPipeline(Seq(rangePipeline))
    rdd.count() should equal(30)
    rdd.partitions.length should equal(10)

    val df = MongoSpark.load(sparkSession, readConf).filter(s"""$partitionKey >= "00001" AND  $partitionKey < "00031"""")
    df.count() should equal(30)
    df.rdd.partitions.length should equal(10)
    val dfPartitions = df.rdd.partitions.toList
    getQueryBoundForKey(dfPartitions.head.asInstanceOf[MongoPartition], "$gte") should equal("00001")
    getQueryBoundForKey(dfPartitions.reverse.head.asInstanceOf[MongoPartition], "$lte") should equal("00030")

    val df2 = MongoSpark.load(sparkSession, readConf).filter(s"""$partitionKey < "00099"""")
    df2.count() should equal(98)
    df2.rdd.partitions.length should equal(10)
    val df2Partitions = df2.rdd.partitions.toList
    getQueryBoundForKey(df2Partitions.head.asInstanceOf[MongoPartition], "$gte") should equal("00001")
    getQueryBoundForKey(df2Partitions.reverse.head.asInstanceOf[MongoPartition], "$lte") should equal("00098")

    val df3 = MongoSpark.load(sparkSession, readConf).filter(s"""$partitionKey > "09000"""")
    df3.count() should equal(1000)
    df3.rdd.partitions.length should equal(10)
    val df3Partitions = df3.rdd.partitions.toList
    getQueryBoundForKey(df3Partitions.head.asInstanceOf[MongoPartition], "$gte") should equal("09001")
    getQueryBoundForKey(df3Partitions.reverse.head.asInstanceOf[MongoPartition], "$lte") should equal("10000")
  }
  // scalastyle:on magic.number

  it should "handle fewer documents than partitions" in withSparkContext() { sc =>
    sc.parallelize((0 to 10).map(i => BsonDocument.parse(s"{ _id: $i }"))).saveToMongoDB()

    val rightHandBoundaries = (0 to 10).map(x => new BsonInt32(x))
    val locations = PartitionerHelper.locations(MongoConnector(sparkConf))
    val partitions = MongoPaginateByCountPartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("numberOfPartitions" -> "100")), pipeline
    )
    val expectedPartitions = PartitionerHelper.createPartitions(partitionKey, rightHandBoundaries, locations)
    partitions should equal(expectedPartitions)
  }

  it should "handle single item partition" in withSparkContext() { sc =>
    sc.parallelize(Seq(BsonDocument.parse("{ _id: 1 }"))).saveToMongoDB()

    val rightHandBoundaries = Seq(new BsonInt32(1))
    val locations = PartitionerHelper.locations(MongoConnector(sparkConf))
    val pipe = List(BsonDocument.parse("{$match: {_id: {$gte: 0}}}")).toArray
    val partitions = MongoPaginateByCountPartitioner.partitions(
      mongoConnector,
      readConfig.copy(partitionerOptions = Map("numberOfPartitions" -> "1")), pipe
    )
    val expectedPartitions = PartitionerHelper.createPartitions(partitionKey, rightHandBoundaries, locations, addMinMax = false)
    PartitionerHelper.setLastBoundaryToLessThanOrEqualTo(partitionKey, expectedPartitions)
    partitions should equal(expectedPartitions)
  }

  it should "handle no collection" in {
    val expectedPartitions = MongoPaginateByCountPartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoPaginateByCountPartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }

  private def getQueryBoundForKey(partition: MongoPartition, key: String): String =
    partition.queryBounds.getDocument(partitionKey).getString(key).getValue
}
