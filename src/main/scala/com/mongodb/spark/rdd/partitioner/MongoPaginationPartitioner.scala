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

import org.bson.conversions.Bson
import org.bson.{BsonDocument, BsonMinKey, BsonValue}
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.{Filters, Sorts}
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.exceptions.MongoPartitionerException

private[partitioner] trait MongoPaginationPartitioner {

  private implicit object BsonValueOrdering extends BsonValueOrdering

  protected def calculatePartitions(connector: MongoConnector, readConfig: ReadConfig, partitionKey: String, count: Long,
                                    numDocumentsPerPartition: Int, matchQuery: Bson): Seq[BsonValue] = {

    @tailrec
    def accumulatePartitions(results: List[BsonValue], position: Int): List[BsonValue] = {
      if (position > count) {
        results
      } else {
        val (skipValue: Int, filter: Bson, sort: Bson) = if (results.isEmpty) {
          (0, Filters.and(matchQuery, Filters.gte(partitionKey, new BsonMinKey())), Sorts.ascending(partitionKey))
        } else {
          val query = Filters.and(matchQuery, Filters.gte(partitionKey, results.head))
          val (skip, sort) = if (position + numDocumentsPerPartition > count) {
            (0, Sorts.descending(partitionKey))
          } else {
            (numDocumentsPerPartition, Sorts.ascending(partitionKey))
          }
          (skip, query, sort)
        }
        val projection = if (partitionKey == "_id") {
          BsonDocument.parse(s"""{"$partitionKey": 1}""")
        } else {
          BsonDocument.parse(s"""{"$partitionKey": 1, "_id": 0}""")
        }
        val newHead: Option[BsonValue] = connector.withCollectionDo(readConfig, { coll: MongoCollection[BsonDocument] =>
          Option(coll.find()
            .filter(filter)
            .skip(skipValue)
            .sort(sort)
            .projection(projection)
            .first()).map(doc => doc.get(partitionKey))
        })
        if (newHead.isEmpty || (results.nonEmpty && newHead.get == results.head)) {
          results
        } else {
          accumulatePartitions(newHead.get :: results, position + numDocumentsPerPartition)
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
