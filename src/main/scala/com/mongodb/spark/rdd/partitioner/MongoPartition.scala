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

import org.apache.spark.Partition

import org.bson.BsonDocument

/**
 * The MongoPartition companion object
 *
 * @since 1.0
 */
object MongoPartition {

  /**
   * Create a MongoPartition with no preferred locations
   *
   * @param index The partition's index within its parent RDD
   * @param queryBounds The query bounds for the data within this partition
   * @return the MongoPartition
   */
  def apply(index: Int, queryBounds: BsonDocument): MongoPartition = new MongoPartition(index, queryBounds, Nil)

  /**
   * Create a MongoPartition with no preferred locations from the Java API
   *
   * @param index The partition's index within its parent RDD
   * @param queryBounds The query bounds for the data within this partition
   * @return the MongoPartition
   */
  def create(index: Int, queryBounds: BsonDocument): MongoPartition = apply(index, queryBounds)

  /**
   * Create a MongoPartition from the Java API
   *
   * @param index The partition's index within its parent RDD
   * @param queryBounds The query bounds for the data within this partition
   * @param locations The preferred locations (hostnames) for the data
   * @return the MongoPartition
   */
  def create(index: Int, queryBounds: BsonDocument, locations: java.util.List[String]): MongoPartition =
    new MongoPartition(index, queryBounds, locations.asScala)
}

/**
 * An identifier for a partition in a MongoRDD.
 *
 * @param index The partition's index within its parent RDD
 * @param queryBounds The query bounds for the data within this partition
 * @param locations The preferred locations (hostnames) for the data
 *
 * @since 1.0
 */
case class MongoPartition(index: Int, queryBounds: BsonDocument, locations: Seq[String]) extends Partition {
  override def hashCode(): Int = super.hashCode()

  override def equals(other: Any): Boolean = other match {
    case p: MongoPartition if index.equals(p.index) && queryBounds.equals(p.queryBounds) && locations.equals(p.locations) => true
    case _ => false
  }

  override def toString: String = s"MongoPartition(index=$index, queryBounds=${queryBounds.toJson}, locations=${locations.toList})"
}
