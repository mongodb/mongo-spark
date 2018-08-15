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

import org.bson.BsonDocument
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.{Logging, MongoConnector}

/**
 * The MongoPartitioner provides the partitions of a collection
 *
 * @define configurationProperties
 *
 * == Configuration Properties ==
 *
 * The prefix when using `sparkConf` is: `spark.mongodb.input.partitionerOptions` followed by the property name:
 *
 * @since 1.0
 */
trait MongoPartitioner extends Logging with Serializable {

  /**
   * Calculate the Partitions
   *
   * @param connector the MongoConnector
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @param pipeline the pipeline to apply if any. Note this pipeline may have been appended to during optimization.
   * @return the partitions
   */
  def partitions(connector: MongoConnector, readConfig: ReadConfig, pipeline: Array[BsonDocument]): Array[MongoPartition]

}
