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

import org.bson.{BsonDocument, BsonInt64}
import com.mongodb.MongoCommandException
import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.ReadConfig

/**
 * The pagination by size partitioner.
 *
 * Paginates the collection into partitions based on their size.  Uses the `collStats` command and the average document size to
 * estimate the partition boundaries.
 *
 * $configurationProperties
 *
 *  - [[partitionKeyProperty partitionKey]], the field to partition the collection by. The field should be indexed and contain unique values.
 *     Defaults to `_id`.
 *  - [[partitionSizeMBProperty partitionSizeMB]], the size (in MB) for each partition. Defaults to `64`.
 *
 *
 * *Note:* This can be a expensive operation as it creates 1 cursor for every estimated `partitionSizeMB`s worth of documents.
 * *Note:* Does not support views. Use `MongoPaginateByCountPartitioner` or create a custom partitioner.
 *
 * @since 1.0
 */
class MongoPaginateBySizePartitioner extends MongoPartitioner with MongoPaginationPartitioner {

  private val DefaultPartitionKey = "_id"
  private val DefaultPartitionSizeMB = "64"

  /**
   * The partition key property
   */
  val partitionKeyProperty = "partitionKey".toLowerCase()

  /**
   * The partition size MB property
   */
  val partitionSizeMBProperty = "partitionSizeMB".toLowerCase()

  /**
   * Calculate the Partitions
   *
   * @param connector  the MongoConnector
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @return the partitions
   */
  override def partitions(connector: MongoConnector, readConfig: ReadConfig, pipeline: Array[BsonDocument]): Array[MongoPartition] = {
    Try(PartitionerHelper.collStats(connector, readConfig)) match {
      case Success(results) =>
        val partitionerOptions = readConfig.partitionerOptions.map(kv => (kv._1.toLowerCase, kv._2))
        val partitionKey = partitionerOptions.getOrElse(partitionKeyProperty, DefaultPartitionKey)
        val partitionSizeInBytes = partitionerOptions.getOrElse(partitionSizeMBProperty, DefaultPartitionSizeMB).toInt * 1024 * 1024

        val avgObjSizeInBytes = results.get("avgObjSize", new BsonInt64(0)).asNumber().longValue()
        val numDocumentsPerPartition: Int = math.floor(partitionSizeInBytes.toFloat / avgObjSizeInBytes).toInt

        val matchQuery = PartitionerHelper.matchQuery(pipeline)
        val count = if (matchQuery.isEmpty) {
          results.getNumber("count").longValue()
        } else {
          connector.withCollectionDo(readConfig, { coll: MongoCollection[BsonDocument] => coll.countDocuments(matchQuery) })
        }
        if (numDocumentsPerPartition >= count) {
          if (count == 0) {
            logInfo(s"Empty collection (${readConfig.collectionName}), using a single partition")
          } else {
            logInfo(s"Inefficient partitioning, creating a single partition. Decrease the `$partitionSizeMBProperty` property.")
          }
          MongoSinglePartitioner.partitions(connector, readConfig, pipeline)
        } else {
          val rightHandBoundaries = calculatePartitions(connector, readConfig, partitionKey, count, numDocumentsPerPartition, matchQuery)
          val addMinMax = matchQuery.isEmpty
          val partitions = PartitionerHelper.createPartitions(partitionKey, rightHandBoundaries, PartitionerHelper.locations(connector), addMinMax)
          if (!addMinMax) PartitionerHelper.setLastBoundaryToLessThanOrEqualTo(partitionKey, partitions)
          partitions
        }
      case Failure(ex: MongoCommandException) if ex.getErrorMessage.endsWith("not found.") || ex.getErrorCode == 26 =>
        logInfo(s"Could not find collection (${readConfig.collectionName}), using a single partition")
        MongoSinglePartitioner.partitions(connector, readConfig, pipeline)
      case Failure(e) =>
        logWarning(s"Could not get collection statistics. Server errmsg: ${e.getMessage}")
        throw e
    }
  }

}

case object MongoPaginateBySizePartitioner extends MongoPaginateBySizePartitioner
