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

import org.apache.spark.Partition

import org.bson.Document
import com.mongodb.MongoCommandException
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.conf.ReadConfig

private[rdd] case class MongoSplitKeyPartitioner(readConfig: ReadConfig) extends MongoPartitioner {

  override def partitions(@transient connector: MongoConnector): Array[Partition] = {
    splitter(connector).bounds().zipWithIndex.map(x => MongoPartition(x._2, x._1)).toArray
  }

  def splitter(@transient connector: MongoConnector): MongoSplitter = {

    val collStatsCommand: Document = new Document("collStats", readConfig.collectionName)
    Try(connector.getDatabase(readConfig.databaseName).runCommand(collStatsCommand)) match {
      case Success(result) => result.getBoolean("sharded").asInstanceOf[Boolean] match {
        case true  => MongoShardedSplitter(connector, readConfig)
        case false => MongoStandaloneSplitter(connector, readConfig)
      }
      case Failure(ex: MongoCommandException) if ex.getErrorMessage.endsWith("not found.") =>
        logWarning(s"Could not find collection (${readConfig.collectionName}), using empty splitter")
        MongoEmptySplitter()
      case Failure(e) =>
        logWarning(s"Could not get collection statistics, using max bounds splitter. Server errmsg: ${e.getMessage}")
        MongoMinToMaxSplitter(readConfig.splitKey)
    }
  }

}
