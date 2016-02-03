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

import scala.collection.JavaConverters._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import org.bson.conversions.Bson
import org.bson.{BsonType, BsonArray, BsonDocument, Document}
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.conf.ReadConfig
import com.mongodb.spark.rdd.MongoRDD

/**
 * A MongoDB based DataSource
 */
class DefaultSource extends DataSourceRegister with RelationProvider with SchemaRelationProvider {

  override def shortName(): String = "mongo"

  /**
   * Create a `MongoRelation`
   *
   * Infers the schema by sampling documents from the database.
   *
   * @param sqlContext the sqlContext
   * @param parameters any user provided parameters
   * @return a MongoRelation
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): MongoRelation =
    createRelation(sqlContext, parameters, None)

  /**
   * Create a `MongoRelation` based on the provided schema
   *
   * @param sqlContext the sqlContext
   * @param parameters any user provided parameters
   * @param schema     the provided schema for the documents
   * @return a MongoRelation
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): MongoRelation =
    createRelation(sqlContext, parameters, Some(schema))

  private def createRelation(sqlContext: SQLContext, parameters: Map[String, String], structType: Option[StructType]): MongoRelation = {
    val sparkConf: SparkConf = sqlContext.sparkContext.getConf
    val uri: String = parameters.getOrElse("uri", sparkConf.get("mongodb.uri"))
    val mongoConnector: MongoConnector = MongoConnector(uri)
    val readConfig = ReadConfig(sparkConf).withParameters(parameters)
    val pipeline = parameters.get("pipeline")
    val schema: StructType = structType match {
      case Some(s) => s
      case None    => MongoInferSchema(pipelinedRdd(MongoRDD[BsonDocument](sqlContext.sparkContext, mongoConnector, readConfig), pipeline))
    }
    MongoRelation(pipelinedRdd(MongoRDD[Document](sqlContext.sparkContext, mongoConnector, readConfig), pipeline), Some(schema))(sqlContext)
  }

  private def pipelinedRdd[T](rdd: MongoRDD[T], pipelineJson: Option[String]): MongoRDD[T] = {
    pipelineJson match {
      case Some(json) =>
        val pipeline: Seq[Bson] = BsonDocument.parse(s"{pipeline: $json}").get("pipeline") match {
          case seq: BsonArray if seq.get(0).getBsonType == BsonType.DOCUMENT => seq.getValues.asScala.asInstanceOf[Seq[Bson]]
          case doc: BsonDocument => Seq(doc)
          case _ => throw new IllegalArgumentException(
            s"""Invalid pipeline option: $pipelineJson.
               | It should be a list of pipeline stages (Documents) or a single pipeline stage (Document)""".stripMargin
          )
        }
        rdd.withPipeline(pipeline)
      case None => rdd
    }
  }
}
