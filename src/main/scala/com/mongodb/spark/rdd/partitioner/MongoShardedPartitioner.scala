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

import org.bson.BsonDocument
import com.mongodb.ServerAddress
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.{Filters, Projections}
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.ReadConfig

/**
 * The Sharded Partitioner
 *
 * Partitions collections by shard and chunk.
 *
 * @since 1.0
 */
case object MongoShardedPartitioner extends MongoPartitioner {

  override def partitions(connector: MongoConnector, readConfig: ReadConfig): Array[MongoPartition] = {
    val ns: String = s"${readConfig.databaseName}.${readConfig.collectionName}"
    logDebug(s"Getting split bounds for a sharded collection: $ns")

    val chunks: Seq[BsonDocument] = connector.withCollectionDo(
      ReadConfig("config", "chunks"), { collection: MongoCollection[BsonDocument] =>
        collection.find(Filters.eq("ns", ns)).projection(Projections.include("min", "max", "shard"))
          .into(new util.ArrayList[BsonDocument]).asScala
      }
    )

    chunks.isEmpty match {
      case true =>
        logWarning(
          s"""Collection '$ns' does not appear to be sharded, continuing with a single partition.
             |To split the collections into multiple partitions connect to the MongoDB node directly""".stripMargin.replaceAll("\n", " ")
        )
        MongoSinglePartitioner.partitions(connector, readConfig)
      case false =>
        generatePartitions(chunks, readConfig.splitKey, mapShards(connector))
    }
  }

  private[partitioner] def generatePartitions(chunks: Seq[BsonDocument], splitKey: String, shardsMap: Map[String, Seq[String]]): Array[MongoPartition] = {
    chunks.zipWithIndex.map({
      case (chunk: BsonDocument, i: Int) =>
        MongoPartition(
          i,
          PartitionerHelper.createBoundaryQuery(
            splitKey,
            chunk.getDocument("min").get(splitKey),
            chunk.getDocument("max").get(splitKey)
          ),
          shardsMap.getOrElse(chunk.getString("shard").getValue, Nil)
        )
    }).toArray
  }

  private[partitioner] def mapShards(connector: MongoConnector): Map[String, Seq[String]] = {
    connector.withCollectionDo(
      ReadConfig("config", "shards"), { collection: MongoCollection[BsonDocument] =>
        Map(collection.find().projection(Projections.include("_id", "host")).into(new util.ArrayList[BsonDocument]).asScala
          .map(shard => (shard.getString("_id").getValue, getHosts(shard.getString("host").getValue))): _*)
      }
    )
  }

  private[partitioner] def getHosts(hosts: String): Seq[String] = hosts.split(",").toSeq.map(getHost).distinct
  private[partitioner] def getHost(hostAndPort: String): String = new ServerAddress(hostAndPort.split("/").reverse.head).getHost
}
