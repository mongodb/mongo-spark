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
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.ReadConfig

/**
 * The default collection partitioner implementation
 *
 * Wraps the [[MongoSamplePartitioner]] and provides in-depth information for users of older MongoDBs.
 *
 * @since 1.0
 */
class DefaultMongoPartitioner extends MongoPartitioner {

  override def partitions(connector: MongoConnector, readConfig: ReadConfig, pipeline: Array[BsonDocument]): Array[MongoPartition] = {
    connector.hasSampleAggregateOperator(readConfig) match {
      case true => MongoSamplePartitioner.partitions(connector, readConfig, pipeline)
      case false =>
        logError(
          """
            |----------------------------------------
            |WARNING: MongoDB version < 3.2 detected.
            |----------------------------------------
            |
            |With legacy MongoDB installations you will need to explicitly configure the Spark Connector with a partitioner.
            |
            |This can be done by:
            | * Setting a "spark.mongodb.input.partitioner" in SparkConf.
            | * Setting in the "partitioner" parameter in ReadConfig.
            | * Passing the "partitioner" option to the DataFrameReader.
            |
            |The following Partitioners are available:
            |
            | * MongoShardedPartitioner - for sharded clusters, requires read access to the config database.
            | * MongoSplitVectorPartitioner - for single nodes or replicaSets. Utilises the SplitVector command on the primary.
            | * MongoPaginateByCountPartitioner - creates a specific number of partitions. Slow as requires a query for every partition.
            | * MongoPaginateBySizePartitioner - creates partitions based on data size. Slow as requires a query for every partition.
            |
          """.stripMargin
        )
        throw new UnsupportedOperationException("The DefaultMongoPartitioner requires MongoDB >= 3.2")
    }
  }

}

case object DefaultMongoPartitioner extends DefaultMongoPartitioner
