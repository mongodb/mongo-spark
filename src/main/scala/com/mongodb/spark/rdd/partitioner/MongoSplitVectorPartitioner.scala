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

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.bson._
import com.mongodb.MongoNotPrimaryException
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.PartitionConfig
import com.mongodb.spark.exceptions.MongoSplitException

private[partitioner] case object MongoSplitVectorPartitioner extends MongoPartitioner {

  override def partitions(connector: MongoConnector, partitionConfig: PartitionConfig): Array[MongoPartition] = {
    val ns: String = s"${partitionConfig.databaseName}.${partitionConfig.collectionName}"
    logDebug(s"Getting split bounds for a non-sharded collection: $ns")

    val keyPattern: BsonDocument = new BsonDocument(partitionConfig.splitKey, new BsonInt32(1))
    val splitVectorCommand: BsonDocument = new BsonDocument("splitVector", new BsonString(ns))
      .append("keyPattern", keyPattern)
      .append("maxChunkSize", new BsonInt32(partitionConfig.maxChunkSize))

    connector.withDatabaseDo(partitionConfig, { db =>
      Try(db.runCommand(splitVectorCommand, classOf[BsonDocument])) match {
        case Success(result: BsonDocument) => createPartitions(partitionConfig.splitKey, result)
        case Failure(e: MongoNotPrimaryException) =>
          logInfo(s"Splitting failed: '${e.getMessage}'. Continuing with a single partition.")
          MongoSinglePartitioner.partitions(connector, partitionConfig)
        case Failure(t: Throwable) => throw t
      }
    })
  }

  private def createPartitions(splitKey: String, result: BsonDocument): Array[MongoPartition] = {
    result.getDouble("ok").getValue match {
      case 1.0 =>
        val minBounds = new BsonDocument(splitKey, new BsonMinKey())
        val splitKeys: Seq[BsonDocument] = result.get("splitKeys").asInstanceOf[java.util.List[BsonDocument]].asScala
        val maxBounds = new BsonDocument(splitKey, new BsonMaxKey())
        val minToMaxSplitKeys: Seq[BsonDocument] = minBounds +: splitKeys :+ maxBounds
        if (splitKeys.isEmpty) {
          logInfo(
            """No splitKeys were calculated by the splitVector command, proceeding with a single partition.
              |If this is undesirable try lowering 'maxChunkSize' to produce more partitions.""".stripMargin.replaceAll("\n", " ")
          )
        }
        val splitKeyPairs: Seq[(BsonDocument, BsonDocument)] = minToMaxSplitKeys zip minToMaxSplitKeys.tail
        splitKeyPairs.zipWithIndex.map({
          case ((minKey: BsonDocument, maxKey: BsonDocument), i: Int) =>
            MongoPartition(
              i,
              PartitionerHelper.createBoundaryQuery(
                splitKey,
                minKey.get(splitKey),
                maxKey.get(splitKey)
              )
            )
        }).toArray
      case _ => throw new MongoSplitException(s"""Could not calculate standalone splits. Server errmsg: ${result.get("errmsg")}""")
    }
  }
}
