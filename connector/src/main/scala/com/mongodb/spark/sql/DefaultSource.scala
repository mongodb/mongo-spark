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
    val uri: String = parameters.getOrElse("uri", sparkConf.get("com.mongodb.spark.uri"))
    val databaseName: String = parameters.getOrElse("databaseName", sparkConf.get("com.mongodb.spark.databaseName"))
    val collectionName: String = parameters.getOrElse("collectionName", sparkConf.get("com.mongodb.spark.collectionName"))
    val mongoConnector: MongoConnector = MongoConnector(uri, databaseName, collectionName)
    val mongoRDD: MongoRDD[BsonDocument] = MongoRDD(sqlContext.sparkContext, mongoConnector)

    val sampleSize: Int = parameters.getOrElse("sampleSize", MongoInferSchema.defaultSampleSize.toString).toInt
    val samplingRatio: Double = parameters.getOrElse("samplingRatio", MongoInferSchema.defaultSamplingRatio.toString).toDouble
    val schema: StructType = MongoInferSchema(mongoRDD, sampleSize = sampleSize, samplingRatio = samplingRatio)
    createRelation(sqlContext, parameters, schema)
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
    val uri: String = parameters.getOrElse("uri", sparkConf.get("com.mongodb.spark.uri"))
    val databaseName: String = parameters.getOrElse("databaseName", sparkConf.get("com.mongodb.spark.databaseName"))
    val collectionName: String = parameters.getOrElse("collectionName", sparkConf.get("com.mongodb.spark.collectionName"))
    val mongoConnector: MongoConnector = MongoConnector(uri, databaseName, collectionName)
    val mongoRDD: MongoRDD[Document] = MongoRDD(sqlContext.sparkContext, mongoConnector) // TODO config - so can switch splitters
    MongoRelation(mongoRDD, Some(schema))(sqlContext)
  }
}
