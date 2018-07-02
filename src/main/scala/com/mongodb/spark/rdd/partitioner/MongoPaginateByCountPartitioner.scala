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

import scala.util.{Failure, Success, Try}

import org.bson.BsonDocument
import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.ReadConfig

/**
 * The pagination by count partitioner.
 *
 * Paginates the collection into a maximum number of partitions.
 *
 * $configurationProperties
 *
 *  - [[partitionKeyProperty partitionKey]], the field to partition the collection by. The field should be indexed and contain unique values.
 *     Defaults to `_id`.
 *  - [[numberOfPartitionsProperty numberOfPartitions]], the maximum number of partitions to create. Defaults to `64`.
 *
 *
 * '''Note:''' This can be a expensive operation as it creates 1 cursor for every partition.
 *
 * @since 1.0
 */
class MongoPaginateByCountPartitioner extends MongoPartitioner with MongoPaginationPartitioner {

  private implicit object BsonValueOrdering extends BsonValueOrdering
  private val DefaultPartitionKey = "_id"
  private val DefaultNumberOfPartitions = "64"

  /**
   * The partition key property
   */
  val partitionKeyProperty = "partitionKey".toLowerCase()

  /**
   * The number of partitions property
   */
  val numberOfPartitionsProperty = "numberOfPartitions".toLowerCase()

  /**
   * Calculate the Partitions
   *
   * @param connector  the MongoConnector
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @return the partitions
   */
  override def partitions(connector: MongoConnector, readConfig: ReadConfig, pipeline: Array[BsonDocument]): Array[MongoPartition] = {
    val matchQuery = PartitionerHelper.matchQuery(pipeline)
    Try(connector.withCollectionDo(readConfig, { coll: MongoCollection[BsonDocument] => coll.countDocuments(matchQuery) })) match {
      case Success(count) =>
        val partitionerOptions = readConfig.partitionerOptions.map(kv => (kv._1.toLowerCase, kv._2))
        val partitionKey = partitionerOptions.getOrElse(partitionKeyProperty, DefaultPartitionKey)
        val maxNumberOfPartitions = partitionerOptions.getOrElse(numberOfPartitionsProperty, DefaultNumberOfPartitions).toInt
        require(maxNumberOfPartitions > 0, "maxNumberOfPartitions must be greater than zero")
        val numberOfPartitions = if (count < maxNumberOfPartitions) count else maxNumberOfPartitions

        if (count == 0) {
          logInfo(s"Empty collection (${readConfig.collectionName}), using a single partition")
          MongoSinglePartitioner.partitions(connector, readConfig, pipeline)
        } else {
          if (count == numberOfPartitions) {
            logWarning("Inefficient partitioning, creating a partition per document. Decrease the `numberOfPartitions` property.")
          }
          val numDocumentsPerPartition = math.floor(count / numberOfPartitions).toInt
          val rightHandBoundaries = calculatePartitions(connector, readConfig, partitionKey, count, numDocumentsPerPartition, matchQuery)
          val addMinMax = matchQuery.isEmpty
          val partitions = PartitionerHelper.createPartitions(partitionKey, rightHandBoundaries, PartitionerHelper.locations(connector),
            addMinMax)
          if (!addMinMax) PartitionerHelper.setLastBoundaryToLessThanOrEqualTo(partitionKey, partitions)
          partitions
        }
      case Failure(e) =>
        logWarning(s"Could not get collection statistics. Server errmsg: ${e.getMessage}")
        throw e
    }
  }
}

case object MongoPaginateByCountPartitioner extends MongoPaginateByCountPartitioner
