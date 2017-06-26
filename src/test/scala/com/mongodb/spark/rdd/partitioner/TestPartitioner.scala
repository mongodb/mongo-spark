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

import org.bson.{BsonDocument, Document}
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.ReadConfig
import com.mongodb.{MongoClient, MongoCommandException}

/**
 * The test collection partitioner implementation
 *
 * Checks if the collection is sharded then:
 *  - If sharded uses the [[MongoShardedPartitioner]] to partition the collection by shard chunks
 *  - If non sharded uses the [[MongoSplitVectorPartitioner]] to partition the collection by using the `splitVector` command.
 *
 * @since 1.0
 */
case object TestPartitioner extends MongoPartitioner {

  var _partitioner: Option[MongoPartitioner] = None
  def getPartitioner(connector: MongoConnector): MongoPartitioner = {
    if (_partitioner.isEmpty) {
      val version: (Int, Int) = Try(connector.withMongoClientDo({ client: MongoClient =>
        val buildinfo = client.getDatabase("admin").runCommand(Document.parse("{buildinfo: 1}"))
        val version = buildinfo.getString("version").split("\\.").take(2).map(_.toInt)
        (version.head, version(1))
      })).getOrElse((1, 0))
      val partitioner = if (version._1 > 3 || version._1 == 3 && version._2 >= 4) MongoSamplePartitioner else MongoSplitVectorPartitioner
      _partitioner = Some(partitioner)
    }
    _partitioner.get
  }

  override def partitions(connector: MongoConnector, readConfig: ReadConfig, pipeline: Array[BsonDocument]): Array[MongoPartition] = {
    val collStatsCommand: Document = new Document("collStats", readConfig.collectionName)
    val partitioner: MongoPartitioner = Try(connector.withDatabaseDo(readConfig, { db => db.runCommand(collStatsCommand) })) match {
      case Success(result) => result.getBoolean("sharded").asInstanceOf[Boolean] match {
        case true  => MongoShardedPartitioner
        case false => getPartitioner(connector)
      }
      case Failure(ex: MongoCommandException) if ex.getErrorMessage.endsWith("not found.") || ex.getErrorCode == 26 =>
        logWarning(s"Could not find collection (${readConfig.collectionName}), using single partition")
        MongoSinglePartitioner
      case Failure(e) => throw e
    }
    partitioner.partitions(connector, readConfig, pipeline)
  }

}
