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

private[partitioner] object PartitionerHelper {

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

}
