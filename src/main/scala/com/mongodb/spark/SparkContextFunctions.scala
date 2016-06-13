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

import scala.reflect.ClassTag

import org.apache.spark.SparkContext

import org.bson.Document
import com.mongodb.spark.DefaultHelper.DefaultsTo
import com.mongodb.spark.annotation.DeveloperApi
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.rdd.MongoRDD

/**
 * :: DeveloperApi ::
 *
 * Helpers to create [[com.mongodb.spark.rdd.MongoRDD]] in the current `SparkContext`.
 *
 * @param sc the Spark context
 * @since 1.0
 */
@DeveloperApi
case class SparkContextFunctions(@transient sc: SparkContext) extends Serializable {

  /**
   * Creates a MongoRDD
   *
   * Defaults to using the `SparkConf` for configuration, alternatively supply a [[com.mongodb.spark.config.ReadConfig]].
   *
   * @param readConfig the optional readConfig
   * @tparam D the type of Document to return from MongoDB - defaults to Document
   * @return a MongoRDD
   */
  def loadFromMongoDB[D: ClassTag](readConfig: ReadConfig = ReadConfig(sc))(implicit e: D DefaultsTo Document): MongoRDD[D] =
    MongoSpark.builder().sparkContext(sc).readConfig(readConfig).build().toRDD[D]()
}
