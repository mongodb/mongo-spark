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

import scala.reflect.runtime.universe._

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.annotation.DeveloperApi
import com.mongodb.spark.config.ReadConfig

/**
 * :: DeveloperApi ::
 *
 * Helpers to create [[com.mongodb.spark.rdd.MongoRDD]] in the current `SparkSession`.
 *
 * @param sparkSession the SparkSession
 * @since 2.0
 */
@DeveloperApi
case class SparkSessionFunctions(@transient sparkSession: SparkSession) extends Serializable {

  /**
   * Creates a MongoRDD
   *
   * Defaults to using the `SparkConf` for configuration, alternatively supply a [[com.mongodb.spark.config.ReadConfig]].
   *
   * @param readConfig the optional readConfig
   *
   *
   * @return a MongoRDD
   */
  def loadFromMongoDB[T <: Product: TypeTag](readConfig: ReadConfig = ReadConfig(sparkSession.sparkContext)): DataFrame =
    MongoSpark.builder().sparkSession(sparkSession).readConfig(readConfig).build().toDF[T]()
}
