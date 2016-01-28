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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import org.bson.{BsonDocument, Document}
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
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): MongoRelation = {
    val sparkConf: SparkConf = sqlContext.sparkContext.getConf
    val uri: String = parameters.getOrElse("uri", sparkConf.get("mongodb.uri"))
    val mongoConnector: MongoConnector = MongoConnector(uri)
    val readConfig = ReadConfig(sparkConf).withParameters(parameters)
    val schema: StructType = MongoInferSchema(MongoRDD[BsonDocument](sqlContext.sparkContext, mongoConnector, readConfig))
    val mongoRDD: MongoRDD[Document] = MongoRDD(sqlContext.sparkContext, mongoConnector, readConfig)
    MongoRelation(mongoRDD, Some(schema))(sqlContext)
  }

  /**
   * Create a `MongoRelation` based on the provided schema
   *
   * @param sqlContext the sqlContext
   * @param parameters any user provided parameters
   * @param schema     the provided schema for the documents
   * @return a MongoRelation
   */
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema:     StructType
  ): MongoRelation = {
    val sparkConf: SparkConf = sqlContext.sparkContext.getConf
    val uri: String = parameters.getOrElse("uri", sparkConf.get("mongodb.uri"))
    val mongoConnector: MongoConnector = MongoConnector(uri)
    val readConfig = ReadConfig(sparkConf).withParameters(parameters)
    val mongoRDD: MongoRDD[Document] = MongoRDD(sqlContext.sparkContext, mongoConnector, readConfig)
    MongoRelation(mongoRDD, Some(schema))(sqlContext)
  }
}
