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

import scala.annotation.tailrec

import org.bson.{BsonDocument, BsonMinKey, BsonValue}
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.{Filters, Projections, Sorts}
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.exceptions.MongoPartitionerException

private[partitioner] trait MongoPaginationPartitioner {

  private implicit object BsonValueOrdering extends BsonValueOrdering

  protected def calculatePartitions(connector: MongoConnector, readConfig: ReadConfig, partitionKey: String, count: Long,
                                    numDocumentsPerPartition: Int): Seq[BsonValue] = {

    @tailrec
    def accumulatePartitions(results: List[BsonValue], position: Int): List[BsonValue] = {
      position >= count match {
        case true => results
        case false =>
          val (skipValue, preBsonValue) = results.isEmpty match {
            case true  => (0, new BsonMinKey())
            case false => (numDocumentsPerPartition, results.head)
          }
          val newHead: Option[BsonValue] = connector.withCollectionDo(readConfig, { coll: MongoCollection[BsonDocument] =>
            Option(coll.find()
              .filter(Filters.gte(partitionKey, preBsonValue))
              .skip(skipValue)
              .projection(Projections.include(partitionKey))
              .sort(Sorts.ascending(partitionKey))
              .first()).map(doc => doc.get(partitionKey))
          })
          newHead.isDefined match {
            case true  => accumulatePartitions(newHead.get :: results, position + numDocumentsPerPartition)
            case false => results
          }
      }
    }

    val partitions = accumulatePartitions(List.empty[BsonValue], 0).sorted
    if (partitions.distinct.length != partitions.length) {
      throw new MongoPartitionerException(
        """Partitioner contained duplicated partitions!
          |Pagination partitioners require partition keys that are indexed and contain unique values""".stripMargin
      )
    }
    partitions
  }

}
