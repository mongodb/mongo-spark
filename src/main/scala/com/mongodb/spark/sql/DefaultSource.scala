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

import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import org.bson.conversions.Bson
import org.bson.{BsonArray, BsonDocument, BsonType, Document}
import com.mongodb.client.MongoCollection
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.{MongoConnector, MongoSpark}

/**
 * A MongoDB based DataSource
 */
class DefaultSource extends DataSourceRegister with RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

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
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
    constructRelation(sqlContext, parameters, None)

  /**
   * Create a `MongoRelation` based on the provided schema
   *
   * @param sqlContext the sqlContext
   * @param parameters any user provided parameters
   * @param schema     the provided schema for the documents
   * @return a MongoRelation
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation =
    constructRelation(sqlContext, parameters, Some(schema))

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val writeConfig = WriteConfig(sqlContext.sparkContext.getConf, parameters)
    val mongoConnector = MongoConnector(writeConfig.asOptions)
    lazy val collectionExists: Boolean = mongoConnector.withDatabaseDo(
      writeConfig, { db => db.listCollectionNames().asScala.toList.contains(writeConfig.collectionName) }
    )
    mode match {
      case Append => MongoSpark.save(data, writeConfig)
      case Overwrite =>
        mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] => collection.drop() })
        MongoSpark.save(data, writeConfig)
      case ErrorIfExists =>
        if (collectionExists) {
          throw new UnsupportedOperationException("MongoCollection already exists")
        } else {
          MongoSpark.save(data, writeConfig)
        }
      case Ignore =>
        if (!collectionExists) {
          MongoSpark.save(data, writeConfig)
        }
    }
    constructRelation(sqlContext, parameters ++ writeConfig.asOptions, Some(data.schema))
  }

  private def constructRelation(sqlContext: SQLContext, parameters: Map[String, String], structType: Option[StructType]): BaseRelation = {
    val rdd = MongoSpark.builder()
      .sparkSession(sqlContext.sparkSession)
      .readConfig(ReadConfig(sqlContext.sparkContext.getConf, parameters))
      .build()
      .toRDD[BsonDocument]()

    val schema: StructType = structType match {
      case Some(s) => s
      case None    => MongoInferSchema(rdd)
    }
    MongoRelation(rdd, Some(schema))(sqlContext)
  }

}
