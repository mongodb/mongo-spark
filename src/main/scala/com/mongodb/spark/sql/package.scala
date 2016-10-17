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

import scala.language.implicitConversions

import org.apache.spark.sql._

import com.mongodb.spark.annotation.DeveloperApi

/**
 * The Mongo Spark Connector Sql package
 */
package object sql {

  /**
   * :: DeveloperApi ::
   *
   * Helper to implicitly add MongoDB based functions to a SQLContext
   *
   * @param sqlContext the current SQLContext
   * @return the MongoDB based SQLContext
   */
  @DeveloperApi
  @deprecated("As of Spark 2.0 SQLContext was replaced by SparkSession. Use the SparkSession method instead", "2.0.0")
  implicit def toSparkSessionFunctions(sqlContext: SQLContext): SparkSessionFunctions = toSparkSessionFunctions(sqlContext.sparkSession)

  /**
   * :: DeveloperApi ::
   *
   * Helper to implicitly add MongoDB based functions to a SQLContext
   *
   * @param sparkSession the current SparkSession
   * @return the MongoDB based SQLContext
   */
  @DeveloperApi
  implicit def toSparkSessionFunctions(sparkSession: SparkSession): SparkSessionFunctions = SparkSessionFunctions(sparkSession)

  /**
   * :: DeveloperApi ::
   *
   * Helper to implicitly add MongoDB based functions to a DataFrameReader
   *
   * @param dfr the DataFrameReader
   * @return the MongoDB based DataFrameReader
   */
  @DeveloperApi
  implicit def toMongoDataFrameReaderFunctions(dfr: DataFrameReader): MongoDataFrameReaderFunctions = MongoDataFrameReaderFunctions(dfr)

  /**
   * :: DeveloperApi ::
   *
   * Helper to implicitly add MongoDB based functions to a DataFrameWriter
   *
   * @param dfw the DataFrameWriter
   * @return the MongoDB based DataFrameWriter
   */
  @DeveloperApi
  implicit def toMongoDataFrameWriterFunctions(dfw: DataFrameWriter[_]): MongoDataFrameWriterFunctions = MongoDataFrameWriterFunctions(dfw)

  /**
   * :: DeveloperApi ::
   *
   * Helper to implicitly add MongoDB based functions to a Dataset
   *
   * @param ds the Dataset
   * @return the MongoDB based DataFrame
   * @since 1.1.0
   */
  @DeveloperApi
  implicit def toMongoDataFrame[T](ds: Dataset[T]): MongoDataFrameFunctions[Row] = MongoDataFrameFunctions[Row](ds.toDF())
}
