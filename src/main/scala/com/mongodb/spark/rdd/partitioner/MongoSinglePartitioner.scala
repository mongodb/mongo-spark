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

import org.bson.{BsonDocument, BsonValue}
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.ReadConfig

/**
 * The Single Partitioner.
 *
 * Creates a single partition for the whole collection.
 *
 * '''Note:''' Using this partitioner loses any parallelism and therefore is not generally recommended.
 *
 * @since 1.0
 */
class MongoSinglePartitioner extends MongoPartitioner {

  override def partitions(connector: MongoConnector, readConfig: ReadConfig,
                          pipeline: Array[BsonDocument] = Array.empty[BsonDocument]): Array[MongoPartition] = {
    PartitionerHelper.createPartitions("_id", Seq.empty[BsonValue], PartitionerHelper.locations(connector), addMinMax = false)
  }
}

case object MongoSinglePartitioner extends MongoSinglePartitioner

