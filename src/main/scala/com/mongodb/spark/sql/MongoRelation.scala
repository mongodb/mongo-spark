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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.bson.BsonDocument
import com.mongodb.spark.LoggingTrait
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.sql.MapFunctions.documentToRow
import com.mongodb.spark.sql.MongoRelationHelper.createPipeline

private[spark] case class MongoRelation(mongoRDD: MongoRDD[BsonDocument], _schema: Option[StructType])(@transient val sqlContext: SQLContext)
    extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation
    with LoggingTrait {

  override lazy val schema: StructType = _schema.getOrElse(MongoInferSchema(sqlContext.sparkContext))

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val readConfig = mongoRDD.readConfig

    val rdd = if (readConfig.pipelineIncludeFiltersAndProjections) {
      // Fields that explicitly aren't nullable must also be added to the filters
      val pipelineFilters = if (readConfig.pipelineIncludeNullFilters) {
        schema.fields.filter(!_.nullable).map(_.name).map(IsNotNull) ++ filters
      } else {
        filters
      }

      if (requiredColumns.nonEmpty || pipelineFilters.nonEmpty) {
        logInfo(s"requiredColumns: ${requiredColumns.mkString(", ")}, filters: ${pipelineFilters.mkString(", ")}")
      }
      mongoRDD.appendPipeline(createPipeline(requiredColumns, pipelineFilters))
    } else {
      logInfo(s"${ReadConfig.pipelineIncludeFiltersAndProjectionsProperty} set to false. Filters and Projections have not been pushed to MongoDB")
      mongoRDD
    }
    rdd.map(doc => documentToRow(doc, schema, requiredColumns))
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val dfw = data.write.format("mongo")
    if (overwrite) {
      dfw.mode(SaveMode.Overwrite).option(WriteConfig.forceInsertProperty, "true").save()
    } else {
      dfw.mode(SaveMode.ErrorIfExists).save()
    }
  }

}
