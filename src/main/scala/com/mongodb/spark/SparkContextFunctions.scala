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

import org.apache.spark.{SparkConf, SparkContext}

import org.bson.Document
import org.bson.conversions.Bson
import com.mongodb.spark.DefaultHelper.DefaultsTo
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.rdd.MongoRDD

/**
 * Helpers to create [[com.mongodb.spark.rdd.MongoRDD]] in the current `SparkContext`.
 *
 * @param sc the Spark context
 * @since 1.0
 */
case class SparkContextFunctions(@transient sc: SparkContext) extends Serializable {

  @transient private val sparkConf: SparkConf = sc.getConf

  /**
   * Creates a MongoRDD
   *
   * @tparam D the type of Document to return from MongoDB - defaults to Document
   * @return a MongoRDD
   */
  def loadFromMongoDB[D: ClassTag]()(implicit e: D DefaultsTo Document): MongoRDD[D] =
    MongoRDD[D](sc, MongoConnector(sc), ReadConfig(sc), Nil)

  /**
   * Creates a MongoRDD
   *
   * @param readConfig   the [[com.mongodb.spark.config.ReadConfig]]
   * @tparam D the type of Document to return from MongoDB - defaults to Document
   * @return a MongoRDD
   */
  def loadFromMongoDB[D: ClassTag](readConfig: ReadConfig)(implicit e: D DefaultsTo Document): MongoRDD[D] =
    MongoRDD[D](sc, MongoConnector(ReadConfig(sc.getConf, readConfig.asOptions).asOptions), readConfig, Nil)

  /**
   * Creates a MongoRDD
   *
   * @param readConfig   the [[com.mongodb.spark.config.ReadConfig]]
   * @param pipeline the aggregate pipeline
   * @tparam D the type of Document to return from MongoDB - defaults to Document
   * @return a MongoRDD
   */
  def loadFromMongoDB[D: ClassTag](readConfig: ReadConfig, pipeline: Seq[Bson])(implicit e: D DefaultsTo Document): MongoRDD[D] =
    MongoRDD[D](sc, MongoConnector(readConfig.asOptions), readConfig, pipeline)

  /**
   * Creates a MongoRDD
   *
   * @param connector    the [[com.mongodb.spark.MongoConnector]]
   * @tparam D the type of Document to return from MongoDB - defaults to Document
   * @return a MongoRDD
   */
  def loadFromMongoDB[D: ClassTag](connector: MongoConnector)(implicit e: D DefaultsTo Document): MongoRDD[D] =
    MongoRDD[D](sc, connector, ReadConfig(sc), Nil)

  /**
   * Creates a MongoRDD
   *
   * @param connector    the [[com.mongodb.spark.MongoConnector]]
   * @param pipeline the aggregate pipeline
   * @tparam D the type of Document to return from MongoDB - defaults to Document
   * @return a MongoRDD
   */
  def loadFromMongoDB[D: ClassTag](connector: MongoConnector, pipeline: Seq[Bson])(implicit e: D DefaultsTo Document): MongoRDD[D] =
    MongoRDD[D](sc, connector, ReadConfig(sc), pipeline)

  /**
   * Creates a MongoRDD
   *
   * @param connector    the [[com.mongodb.spark.MongoConnector]]
   * @param readConfig   the [[com.mongodb.spark.config.ReadConfig]]
   * @param pipeline the aggregate pipeline
   * @tparam D the type of Document to return from MongoDB - defaults to Document
   * @return a MongoRDD
   */
  def loadFromMongoDB[D: ClassTag](connector: MongoConnector, readConfig: ReadConfig,
                                   pipeline: Seq[Bson])(implicit e: D DefaultsTo Document): MongoRDD[D] =
    MongoRDD[D](sc, connector, readConfig, pipeline)

}
