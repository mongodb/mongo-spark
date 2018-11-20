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

package com.mongodb.spark.sql

import org.apache.spark.sql.Dataset

import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.{LoggingTrait, MongoSpark}

private[spark] case class MongoDataFrameFunctions[T](@transient df: Dataset[T]) extends LoggingTrait {

  /**
   * Saves the contents of the `DataFrame` to MongoDB.
   */
  def saveToMongoDB(): Unit = MongoSpark.save(df)

  /**
   * Saves the contents of the `DataFrame` to MongoDB.
   *
   * @param writeConfig the [[com.mongodb.spark.config.WriteConfig]] to use
   */
  def saveToMongoDB(writeConfig: WriteConfig): Unit = MongoSpark.save(df, writeConfig)

}
