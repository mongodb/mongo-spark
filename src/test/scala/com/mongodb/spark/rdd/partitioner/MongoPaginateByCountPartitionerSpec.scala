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

  // scalastyle:off magic.number
  it should "use the provided pipeline for min and max keys" in withSparkContext() { sc =>
    sc.parallelize((0 to 1000).map(i => BsonDocument.parse(s"{ _id: $i }"))).saveToMongoDB()

    val readConf = readConfig.copy(partitionerOptions = Map("numberOfPartitions" -> "10"))
    val rangePipeline = Array(BsonDocument.parse(s"""{$$match: { $partitionKey: {$$gte: 250, $$lt: 500}}}"""))

    val rangePartitions = MongoPaginateByCountPartitioner.partitions(mongoConnector, readConf, rangePipeline)
    getQueryBoundForKey(rangePartitions.head, "$gte") should equal(250)
    getQueryBoundForKey(rangePartitions.reverse.head, "$lte") should equal(499)
    rangePartitions.length should equal(10)
  }

  it should "use the users pipeline when set in a rdd / dataframe" in withSparkContext() { sc =>
    sc.parallelize((0 to 1000).map(i => BsonDocument.parse(s"{ _id: $i }"))).saveToMongoDB()

    val readConf = readConfig.copy(partitioner = MongoPaginateByCountPartitioner, partitionerOptions = Map("numberOfPartitions" -> "10"))
    val rangePipeline = BsonDocument.parse(s"""{$$match: { $partitionKey: {$$gte: 250, $$lt: 500}}}""")

    val sparkSession = SparkSession.builder().getOrCreate()
    val rdd = MongoSpark.load(sparkSession.sparkContext, readConf).withPipeline(Seq(rangePipeline))
    rdd.count() should equal(250)
    val rddPartitions = rdd.partitions.toList
    getQueryBoundForKey(rddPartitions.head.asInstanceOf[MongoPartition], "$gte") should equal(250)
    getQueryBoundForKey(rddPartitions.reverse.head.asInstanceOf[MongoPartition], "$lte") should equal(499)

    val df = MongoSpark.load(sparkSession, readConf).filter(s"""$partitionKey >= 250 AND  $partitionKey < 500""")
    df.count() should equal(250)
    val dfPartitions = df.rdd.partitions.toList
    getQueryBoundForKey(dfPartitions.head.asInstanceOf[MongoPartition], "$gte") should equal(250)
    getQueryBoundForKey(dfPartitions.reverse.head.asInstanceOf[MongoPartition], "$lte") should equal(499)
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

  it should "handle no collection" in {
    val expectedPartitions = MongoPaginateByCountPartitioner.partitions(mongoConnector, readConfig, pipeline)
    MongoPaginateByCountPartitioner.partitions(mongoConnector, readConfig, pipeline) should equal(expectedPartitions)
  }

  private def getQueryBoundForKey(partition: MongoPartition, key: String): Int =
    partition.queryBounds.getDocument(partitionKey).getInt32(key).getValue
}
