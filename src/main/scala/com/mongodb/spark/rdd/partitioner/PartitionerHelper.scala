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

import org.bson._
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.ReadConfig

object PartitionerHelper {

  /**
   * Creates the upper and lower boundary query for the given key
   *
   * @param key   the key that represents the values that can be partitioned
   * @param lower the value of the lower bound
   * @param upper the value of the upper bound
   * @return the document containing the partition bounds
   */
  def createBoundaryQuery(key: String, lower: BsonValue, upper: BsonValue): BsonDocument =
    new BsonDocument(key, new BsonDocument("$gte", lower).append("$lt", upper))

  /**
   * Creates partitions using a single Seq of documents representing the right handside of partitions
   *
   * @param partitionKey the key representing the partition most likely the `_id`.
   * @param partitions the documents representing a split
   * @param locations the optional server hostnames for the data
   * @return
   */
  def createPartitions(partitionKey: String, partitions: Seq[BsonValue], locations: Seq[String] = Nil): Array[MongoPartition] = {
    val minToMaxSplitKeys: Seq[BsonValue] = new BsonMinKey() +: partitions :+ new BsonMaxKey()
    val partitionPairs: Seq[(BsonValue, BsonValue)] = minToMaxSplitKeys zip minToMaxSplitKeys.tail
    partitionPairs.zipWithIndex.map({
      case ((min: BsonValue, max: BsonValue), i: Int) => MongoPartition(i, createBoundaryQuery(partitionKey, min, max), locations)
    }).toArray
  }

  /**
   * Get the locations of the Mongo hosts
   *
   * @param connector the MongoConnector
   * @return the locations
   */
  def locations(connector: MongoConnector): Seq[String] =
    connector.withMongoClientDo(mongoClient => mongoClient.getAllAddress.asScala.map(_.getHost).distinct)

  /**
   * Runs the `collStats` command and returns the results
   *
   * @param connector the MongoConnector
   * @param readConfig the readConfig
   * @return the collStats result
   */
  def collStats(connector: MongoConnector, readConfig: ReadConfig): BsonDocument = {
    val collStatsCommand: BsonDocument = new BsonDocument("collStats", new BsonString(readConfig.collectionName))
    connector.withDatabaseDo(readConfig, { db => db.runCommand(collStatsCommand, readConfig.readPreference, classOf[BsonDocument]) })
  }

}
