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

package com.mongodb.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import org.bson.Document
import com.mongodb.spark.DefaultHelper.DefaultsTo
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.annotation.DeveloperApi
import com.mongodb.spark.config.WriteConfig

/**
 * :: DeveloperApi ::
 *
 * Functions for RDD's that allow the data to be saved to MongoDB.
 *
 * @param rdd the rdd
 * @param e the implicit datatype of the rdd
 * @param ct the implicit ClassTag of the datatype of the rdd
 * @tparam D the type of data in the RDD
 *
 * @since 1.0
 */
@DeveloperApi
case class DocumentRDDFunctions[D](rdd: RDD[D])(implicit e: D DefaultsTo Document, ct: ClassTag[D]) {

  @transient val sparkContext = rdd.sparkContext

  /**
   * Saves the RDD data to MongoDB using the given `WriteConfig`
   *
   * @param writeConfig the optional [[com.mongodb.spark.config.WriteConfig]] to use
   * @return the rdd
   */
  def saveToMongoDB(writeConfig: WriteConfig = WriteConfig(sparkContext)): Unit = MongoSpark.save(rdd, writeConfig)

}
