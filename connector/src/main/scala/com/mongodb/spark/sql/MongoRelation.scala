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

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

import org.bson.Document
import com.mongodb.spark.conf.ReadConfig
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.sql.MongoRelationHelper.{createPipeline, documentToRow}

case class MongoRelation(mongoRDD: MongoRDD[Document], _schema: Option[StructType])(@transient val sqlContext: SQLContext)
    extends BaseRelation
    with PrunedFilteredScan
    with Logging {

  val readConfig: ReadConfig = ReadConfig(mongoRDD.context.getConf)

  override lazy val schema: StructType = _schema.getOrElse(MongoInferSchema(sqlContext.sparkContext))

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    if (requiredColumns.nonEmpty || filters.nonEmpty) {
      logInfo(s"requiredColumns: ${requiredColumns.mkString(", ")}, filters: ${filters.mkString(", ")}")
    }
    mongoRDD.appendPipeline(createPipeline(requiredColumns, filters)).map(doc => documentToRow(doc, schema, requiredColumns))
  }

}
