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
import scala.collection.JavaConverters._

import org.bson._
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.annotation.DeveloperApi
import com.mongodb.spark.config.ReadConfig

/**
 * :: DeveloperApi ::
 *
 * Helper methods for partitioner implementations
 *
 * @since 1.0
 */
@DeveloperApi
object PartitionerHelper {

  /**
   * Creates the upper and lower boundary query for the given key
   *
   * @param key   the key that represents the values that can be partitioned
   * @param lower the value of the lower bound
   * @param upper the value of the upper bound
   * @return the document containing the partition bounds
   */
  def createBoundaryQuery(key: String, lower: BsonValue, upper: BsonValue): BsonDocument = {
    require(Option(lower).isDefined, "lower range partition key missing")
    require(Option(upper).isDefined, "upper range partition key missing")
    val queryBoundry = new BsonDocument()
    if (!lower.isInstanceOf[BsonMinKey]) {
      queryBoundry.append("$gte", lower)
    }
    if (!upper.isInstanceOf[BsonMaxKey]) {
      queryBoundry.append("$lt", upper)
    }
    if (queryBoundry.isEmpty) {
      queryBoundry
    } else {
      new BsonDocument(key, queryBoundry)
    }
  }

  /**
   * Creates partitions using a single Seq of documents representing the right handside of partitions
   *
   * @param partitionKey the key representing the partition most likely the `_id`.
   * @param splitKeys the documents representing a split
   * @param locations the optional server hostnames for the data
   * @param addMinMax add min and maxkey query bounds.
   * @return
   */
  def createPartitions(partitionKey: String, splitKeys: Seq[BsonValue], locations: Seq[String] = Nil, addMinMax: Boolean = true): Array[MongoPartition] = {
    val minKeyMaxKeys = (new BsonMinKey(), new BsonMaxKey())
    val minToMaxSplitKeys: Seq[BsonValue] = if (addMinMax) minKeyMaxKeys._1 +: splitKeys :+ minKeyMaxKeys._2 else splitKeys
    val minToMaxKeysToPartition = if (minToMaxSplitKeys.length <= 1) minToMaxSplitKeys else minToMaxSplitKeys.tail
    val partitionPairs: Seq[(BsonValue, BsonValue)] = minToMaxSplitKeys zip minToMaxKeysToPartition
    if (partitionPairs.isEmpty) {
      Array(MongoPartition(0, new BsonDocument(), locations))
    } else {
      partitionPairs.zipWithIndex.map({
        case ((min: BsonValue, max: BsonValue), i: Int) => MongoPartition(i, createBoundaryQuery(partitionKey, min, max), locations)
      }).toArray
    }
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

  /**
   * Returns the head `\$match` from a pipeline
   *
   * Removes any `\$exists` and `\$ne` queries as they cannot use an index.
   *
   * @param pipeline the users pipeline
   * @return the head `\$match` or an empty `BsonDocument`.
   */
  def matchQuery(pipeline: Array[BsonDocument]): BsonDocument = {
    val defaultQuery = new BsonDocument()
    pipeline.headOption match {
      case Some(document) => removeExistsAndNeChecks(document.getDocument("$match", defaultQuery)).asDocument()
      case None           => defaultQuery
    }
  }

  private def removeExistsAndNeChecks(value: BsonValue): BsonValue = {
    if (value.isDocument) {
      val excludeKeys = List("$exists", "$ne")
      val cleanedDocument = new BsonDocument()
      value.asDocument().asScala.foreach {
        case (k: String, v: BsonValue) =>
          if (v.isDocument) {
            val cleanedSubDocument = new BsonDocument()
            v.asDocument().asScala.foreach({
              case (sk: String, sv: BsonValue) =>
                if (!excludeKeys.contains(sk)) {
                  cleanedSubDocument.put(sk, removeExistsAndNeChecks(sv))
                }
            })
            if (!cleanedSubDocument.isEmpty) {
              cleanedDocument.put(k, cleanedSubDocument)
            }
          } else {
            cleanedDocument.put(k, v)
          }
      }
      cleanedDocument
    } else {
      value
    }
  }

  /**
   * Checks an aggregation pipeline to see if it starts with a range based query that is suitable for the `SplitVector` command.
   *
   * If it does it returns the `\$match` filter otherwise None
   *
   * @param pipeline the aggregation pipeline
   * @return the min and max keys for the pipeline
   * @since 2.1
   */
  def getSplitVectorRangeQuery(partitionKey: String, pipeline: Array[BsonDocument]): (BsonValue, BsonValue) = {
    val filter = pipeline.headOption match {
      case Some(document: BsonDocument) => getNestedDocument(Seq("$match", partitionKey), document)
      case None                         => new BsonDocument()
    }
    (filter.get("$gte", new BsonMinKey()), filter.get("$lt", new BsonMaxKey()))
  }

  /**
   * Sets the final boundary to use `\$lte` rather than `\$lt` so that boundaries with users provided queries have the correct upper bound
   *
   * @param partitionKey the partition key
   * @param partitions the partitions
   * @return the updated partitions
   * @since 2.1
   */
  def setLastBoundaryToLessThanOrEqualTo(partitionKey: String, partitions: Array[MongoPartition]): Array[MongoPartition] = {
    if (partitions.length > 0) {
      val lastPartition = partitions.reverse.head
      val partitionQuery = lastPartition.queryBounds.getDocument(partitionKey)
      partitionQuery.append("$lte", partitionQuery.remove("$lt"))
    }
    partitions
  }

  /**
   * Gets a nested document from a document
   *
   * @param keys the keys for the document fields
   * @param document the document to do a nested document lookup
   * @return the subDocument
   */
  @tailrec
  private def getNestedDocument(keys: Seq[String], document: BsonDocument): BsonDocument = {
    if (keys.nonEmpty && document.containsKey(keys.head)) {
      val bsonValue = document.get(keys.head)
      if (bsonValue.isDocument) {
        val subDoc = bsonValue.asDocument()
        if (keys.tail.isEmpty) subDoc else getNestedDocument(keys.tail, subDoc)
      } else {
        new BsonDocument()
      }
    } else {
      new BsonDocument()
    }
  }

}
