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

private[partitioner] case object MongoShardedPartitioner extends MongoPartitioner {

  override def partitions(connector: MongoConnector, readConfig: ReadConfig): Array[MongoPartition] = {
    val ns: String = s"${readConfig.databaseName}.${readConfig.collectionName}"
    logDebug(s"Getting split bounds for a sharded collection: $ns")

    val chunks: Seq[BsonDocument] = connector.withCollectionDo(
      ReadConfig("config", "chunks"),
      { collection: MongoCollection[BsonDocument] =>
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
        val shardsMap: Map[String, String] = mapShards(connector)
        val mongosMap: Map[String, String] = mapMongos(connector)

        chunks.zipWithIndex.map({
          case (chunk: BsonDocument, i: Int) =>
            MongoPartition(
              i,
              PartitionerHelper.createBoundaryQuery(
                readConfig.splitKey,
                chunk.getDocument("min").get(readConfig.splitKey),
                chunk.getDocument("max").get(readConfig.splitKey)
              ),
              Nil
            )
        }).toArray
    }
  }

  // Todo review in relation to sharded replicasets
  private[partitioner] def getPreferredLocations(shardedConnectDirectly: Boolean, shardedConnectToMongos: Boolean, shardsMap: Map[String, String],
                                                 mongosMap: Map[String, String], shard: String): Seq[ServerAddress] = {
    val default = if (shardedConnectToMongos) mongosMap.values.map(new ServerAddress(_)).toSeq else Nil
    shardedConnectDirectly && shardsMap.contains(shard) match {
      case true =>
        val chunkHostAndPort = shardsMap.get(shard).get
        val chunkHost = splitHost(chunkHostAndPort)
        shardedConnectToMongos match {
          case true =>
            mongosMap.contains(chunkHost) match {
              case true  => Seq(new ServerAddress(mongosMap.get(chunkHost).get))
              case false => default
            }
          case false => Seq(new ServerAddress(chunkHostAndPort))
        }
      case false => default
    }
  }

  private def mapShards(connector: MongoConnector): Map[String, String] = {
    connector.withCollectionDo(
      ReadConfig("config", "shards"), { collection: MongoCollection[BsonDocument] =>
        Map(collection.find().projection(Projections.include("_id", "host")).into(new util.ArrayList[BsonDocument]).asScala
          .map(shard => (shard.getString("_id").getValue, shard.getString("host").getValue)): _*)
      }
    )
  }

  private def mapMongos(connector: MongoConnector): Map[String, String] = {
    connector.withCollectionDo(
      ReadConfig("config", "mongos"), { collection: MongoCollection[BsonDocument] =>
        Map(collection.find().projection(Projections.include("_id")).into(new util.ArrayList[BsonDocument]).asScala
          .map(mongos => {
            val hostAndPort = mongos.getString("_id").getValue
            (splitHost(hostAndPort), hostAndPort)
          }): _*)
      }
    )
  }

  private def splitHost(hostAndPort: String): String = hostAndPort.splitAt(hostAndPort lastIndexOf ":")._1
}
