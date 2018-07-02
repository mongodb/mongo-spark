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

package com.mongodb.spark

import org.bson.BsonDocument
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Projections
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.rdd.partitioner.{MongoPartition, MongoPartitioner}

class HalfwayPartitioner extends MongoPartitioner {
  override def partitions(connector: MongoConnector, readConfig: ReadConfig, pipeline: Array[BsonDocument]): Array[MongoPartition] = {
    val midId = connector.withCollectionDo(readConfig, { collection: MongoCollection[BsonDocument] =>
      @transient val midPoint = collection.countDocuments() / 2
      collection.find().skip(midPoint.toInt).limit(1).projection(Projections.include("_id")).first().get("_id")
    })
    Array(
      MongoPartition(0, new BsonDocument("_id", new BsonDocument("$lt", midId))),
      MongoPartition(1, new BsonDocument("_id", new BsonDocument("$gte", midId)))
    )
  }
}

case object HalfwayPartitioner extends HalfwayPartitioner
