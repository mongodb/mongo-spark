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

import com.mongodb.spark.LoggingTrait
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader}

import com.mongodb.spark.config.ReadConfig

private[spark] case class MongoDataFrameReaderFunctions(@transient dfr: DataFrameReader) extends LoggingTrait {

  /**
   * Creates a [[DataFrame]] through schema inference via the `T` type, otherwise will sample the collection to
   * determine the type.
   *
   * @tparam T The optional type of the data from MongoDB
   * @return DataFrame
   */
  def mongo[T <: Product: TypeTag](): DataFrame = createDataFrame(MongoInferSchema.reflectSchema[T](), None)

  /**
   * Creates a [[DataFrame]] through schema inference via the `T` type, otherwise will sample the collection to
   * determine the type.
   *
   * @param readConfig any connection read configuration overrides. Overrides the configuration set in [[org.apache.spark.SparkConf]]
   * @tparam T The optional type of the data from MongoDB
   * @return DataFrame
   */
  def mongo[T <: Product: TypeTag](readConfig: ReadConfig): DataFrame =
    createDataFrame(MongoInferSchema.reflectSchema[T](), Some(readConfig))

  /**
   * Creates a [[DataFrame]] with the set schema
   *
   * @param schema the schema definition
   * @return DataFrame
   */
  def mongo(schema: StructType): DataFrame = createDataFrame(Some(schema), None)

  /**
   * Creates a [[DataFrame]] with the set schema
   *
   * @param schema the schema definition
   * @param readConfig any custom read configuration
   * @return DataFrame
   */
  def mongo(schema: StructType, readConfig: ReadConfig): DataFrame = createDataFrame(Some(schema), Some(readConfig))

  private def createDataFrame(schema: Option[StructType], readConfig: Option[ReadConfig]): DataFrame = {
    val builder = dfr.format("mongo")
    if (schema.isDefined) dfr.schema(schema.get)
    if (readConfig.isDefined) dfr.options(readConfig.get.asOptions)
    builder.load()
  }

}
