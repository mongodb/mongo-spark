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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import org.bson.conversions.Bson
import org.bson.{BsonArray, BsonDocument, BsonType, Document}
import com.mongodb.client.MongoCollection
import com.mongodb.spark.config.{PartitionConfig, ReadConfig, WriteConfig}
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.sql.MongoRelationHelper._
import com.mongodb.spark.{MongoConnector, toDocumentRDDFunctions}

/**
 * A MongoDB based DataSource
 */
class DefaultSource extends DataSourceRegister with RelationProvider with SchemaRelationProvider with CreatableRelationProvider
    with InsertableRelation {

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
    val (mongoConnector, readConfig, partitionConfig, _) = connectorAndConfigs(sqlContext, parameters)
    val pipeline = parameters.get("pipeline")
    val schema: StructType = structType match {
      case Some(s) => s
      case None    => MongoInferSchema(pipelinedRdd(MongoRDD[BsonDocument](sqlContext.sparkContext, mongoConnector, readConfig, partitionConfig), pipeline))
    }
    MongoRelation(pipelinedRdd(MongoRDD[Document](sqlContext.sparkContext, mongoConnector, readConfig, partitionConfig), pipeline), Some(schema))(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val (mongoConnector, _, _, writeConfig) = connectorAndConfigs(sqlContext, parameters)
    val documentRdd: RDD[Document] = data.map(row => rowToDocument(row))

    lazy val collectionExists: Boolean = mongoConnector.withDatabaseDo(
      writeConfig, { db => db.listCollectionNames().asScala.toList.contains(writeConfig.collectionName) }
    )

    mode match {
      case Append => documentRdd.saveToMongoDB(writeConfig)
      case Overwrite =>
        mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] => collection.drop() })
        documentRdd.saveToMongoDB(writeConfig)
      case ErrorIfExists =>
        if (collectionExists) {
          throw new UnsupportedOperationException("MongoCollection already exists")
        } else {
          documentRdd.saveToMongoDB(writeConfig)
        }
      case Ignore =>
        if (!collectionExists) {
          documentRdd.saveToMongoDB(writeConfig)
        }
    }
    createRelation(sqlContext, writeConfig.asOptions)
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val dfw = data.write
    overwrite match {
      case true  => dfw.mode(SaveMode.Overwrite).save()
      case false => dfw.mode(SaveMode.ErrorIfExists).save()
    }
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

  private def connectorAndConfigs(sqlContext: SQLContext, parameters: Map[String, String]): (MongoConnector, ReadConfig, PartitionConfig, WriteConfig) = {
    val sparkConf: SparkConf = sqlContext.sparkContext.getConf
    val uri: String = parameters.getOrElse("uri", sparkConf.get(MongoConnector.mongoURIProperty))
    val mongoConnector: MongoConnector = MongoConnector(uri)
    val readConfig: ReadConfig = ReadConfig(sparkConf).withOptions(parameters)
    val partitionConfig: PartitionConfig = PartitionConfig(sparkConf).withOptions(parameters)
    val writeConfig: WriteConfig = WriteConfig(sparkConf).withOptions(parameters)
    (mongoConnector, readConfig, partitionConfig, writeConfig)
  }
}
