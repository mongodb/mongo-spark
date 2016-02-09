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
import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.conf.ReadConfig
import com.mongodb.spark.exceptions.MongoSplitException

private[partitioner] case class MongoStandaloneSplitter(connector: MongoConnector, readConfig: ReadConfig) extends MongoSplitter {

  override def bounds(): Seq[BsonDocument] = {
    val collection: MongoCollection[BsonDocument] = connector.collection(readConfig.databaseName, readConfig.collectionName)
    val ns: String = collection.getNamespace.getFullName
    logDebug(s"Getting split bounds for a non-sharded collection: $ns")

    val keyPattern: BsonDocument = new BsonDocument(readConfig.splitKey, new BsonInt32(1))
    val splitVectorCommand: BsonDocument = new BsonDocument("splitVector", new BsonString(ns))
      .append("keyPattern", keyPattern)
      .append("maxChunkSize", new BsonInt32(readConfig.maxChunkSize))

    Try(connector.getDatabase(readConfig.databaseName).runCommand(splitVectorCommand, classOf[BsonDocument])) match {
      case Success(result: BsonDocument) => createSplits(result)
      case Failure(e: MongoNotPrimaryException) =>
        logInfo(s"Splitting failed: '${e.getMessage}'. Continuing with a single partition.")
        createSplits(new BsonDocument("ok", new BsonDouble(1.0)))
      case Failure(t: Throwable) => throw t
    }
  }

  private def createSplits(result: BsonDocument): Seq[BsonDocument] = {
    result.getDouble("ok").getValue match {
      case 1.0 =>
        val splitKeys: Seq[BsonDocument] = result.get("splitKeys").asInstanceOf[java.util.List[BsonDocument]].asScala
        val minMaxBounds = MongoMinToMaxSplitter(readConfig.splitKey).bounds()
        val minToMaxSplitKeys: Seq[BsonDocument] = minMaxBounds.head +: splitKeys :+ minMaxBounds.tail.head
        if (splitKeys.isEmpty) {
          logInfo(
            """No splitKeys were calculated by the splitVector command, proceeding with a single partition.
              |If this is undesirable try lowering 'maxChunkSize' to produce more partitions.""".stripMargin.replaceAll("\n", " ")
          )
        }
        val splitKeyPairs: Seq[(BsonDocument, BsonDocument)] = minToMaxSplitKeys zip minToMaxSplitKeys.tail
        splitKeyPairs.map(x => createBoundaryQuery(readConfig.splitKey, x._1.get(readConfig.splitKey), x._2.get(readConfig.splitKey)))
      case _ => throw new MongoSplitException(s"""Could not calculate standalone splits. Server errmsg: ${result.get("errmsg")}""")
    }
  }
}
