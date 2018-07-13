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

import java.util

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.bson._
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.exceptions.MongoPartitionerException
import com.mongodb.{MongoCommandException, MongoNotPrimaryException}

/**
 * The SplitVector Partitioner.
 *
 * Uses the `SplitVector` command on the primary node to generate partitions for a collection.
 * Requires `ClusterManager` privilege.
 *
 *  $configurationProperties
 *
 *  - [[partitionKeyProperty partitionKey]], the field to partition the collection by. The field should be indexed and contain unique values.
 *     Defaults to `_id`.
 *  - [[partitionSizeMBProperty partitionSizeMB]], the size (in MB) for each partition. Defaults to `64`.
 *
 * @since 1.0
 */
class MongoSplitVectorPartitioner extends MongoPartitioner {
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

  override def partitions(connector: MongoConnector, readConfig: ReadConfig, pipeline: Array[BsonDocument]): Array[MongoPartition] = {
    val ns: String = s"${readConfig.databaseName}.${readConfig.collectionName}"
    logDebug(s"Getting split bounds for a non-sharded collection: $ns")

    val partitionerOptions = readConfig.partitionerOptions.map(kv => (kv._1.toLowerCase, kv._2))
    val partitionKey = partitionerOptions.getOrElse(partitionKeyProperty, DefaultPartitionKey)
    val partitionSize = partitionerOptions.getOrElse(partitionSizeMBProperty, DefaultPartitionSizeMB).toInt

    val keyPattern: BsonDocument = new BsonDocument(partitionKey, new BsonInt32(1))
    val minKeyMaxKey = PartitionerHelper.getSplitVectorRangeQuery(partitionKey, pipeline)

    val splitVectorCommand: BsonDocument = new BsonDocument("splitVector", new BsonString(ns))
      .append("keyPattern", keyPattern)
      .append("maxChunkSize", new BsonInt32(partitionSize))
      .append("min", new BsonDocument(partitionKey, minKeyMaxKey._1))
      .append("max", new BsonDocument(partitionKey, minKeyMaxKey._2))

    connector.withDatabaseDo(readConfig, { db =>
      Try(db.runCommand(splitVectorCommand, classOf[BsonDocument])) match {
        case Success(result: BsonDocument) =>
          val locations: Seq[String] = connector.withMongoClientDo(mongoClient => mongoClient.getAllAddress.asScala.map(_.getHost).distinct)
          createPartitions(partitionKey, result, locations, minKeyMaxKey)
        case Failure(e: MongoNotPrimaryException) =>
          logWarning("The `SplitVector` command must be run on the primary node")
          throw e
        case Failure(ex: MongoCommandException) if ex.getErrorMessage.contains("ns not found") =>
          logInfo(s"Could not find collection (${readConfig.collectionName}), using a single partition")
          MongoSinglePartitioner.partitions(connector, readConfig, pipeline)
        case Failure(t: Throwable) => throw t
      }
    })
  }

  private def createPartitions(partitionKey: String, result: BsonDocument, locations: Seq[String], minMaxKey: (BsonValue, BsonValue)): Array[MongoPartition] = {
    result.getDouble("ok").getValue match {
      case 1.0 =>
        val splitKeys = result.get("splitKeys").asInstanceOf[util.List[BsonDocument]].asScala.map(_.get(partitionKey))
        val rightHandBoundaries = minMaxKey._1 +: splitKeys :+ minMaxKey._2
        val partitions = PartitionerHelper.createPartitions(partitionKey, rightHandBoundaries, locations, addMinMax = false)
        if (partitions.length == 1) {
          logInfo(
            """No splitKeys were calculated by the splitVector command, proceeding with a single partition.
              |If this is undesirable try lowering 'partitionSizeMB' property to produce more partitions.""".stripMargin.replaceAll("\n", " ")
          )
        }
        partitions
      case _ => throw new MongoPartitionerException(s"""Could not calculate standalone splits. Server errmsg: ${result.get("errmsg")}""")
    }
  }
}

case object MongoSplitVectorPartitioner extends MongoSplitVectorPartitioner
