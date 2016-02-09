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

import java.io.{Closeable, Serializable}

import scala.reflect.ClassTag

import org.apache.spark.{Logging, SparkConf}

import org.bson.Document
import com.mongodb.client.{MongoCollection, MongoDatabase}
import com.mongodb.spark.DefaultHelper.DefaultsTo
import com.mongodb.{MongoClient, MongoClientURI}

/**
 * The MongoConnector companion object
 *
 * @since 1.0
 */
object MongoConnector {

  /**
   * Creates a MongoConnector
   *
   * @param sparkConf the Spark configuration containing the `uri`, `databaseName` and `collectionName` parameters
   * @return the MongoConnector
   */
  def apply(sparkConf: SparkConf): MongoConnector = {
    // TODO validate the SparkConf and throw a meaningful error message
    val uri = sparkConf.get("mongodb.uri")
    MongoConnector(uri)
  }

  /**
   * Creates a MongoConnector
   *
   * @param connectionString the connection string (`uri`)
   * @return the MongoConnector
   */
  def apply(connectionString: String): MongoConnector =
    MongoConnector(() => new MongoClient(new MongoClientURI(connectionString)))
}

/**
 * The MongoConnector
 *
 * Connects Spark to MongoDB
 *
 * @param mongoClientFactory the factory that can be used to create a MongoClient
 *
 * @since 1.0
 */
case class MongoConnector(mongoClientFactory: () => MongoClient)
    extends Serializable with Closeable with Logging {
  @transient private[spark] var fetchedClient: Boolean = false
  @transient private[spark] lazy val _mongoClient: MongoClient = createMongoClient()

  def mongoClient(): MongoClient = _mongoClient
  def database(databaseName: String): MongoDatabase = mongoClient().getDatabase(databaseName)
  def collection[D](databaseName: String, collectionName: String)(implicit e: D DefaultsTo Document, ct: ClassTag[D]): MongoCollection[D] =
    getDatabase(databaseName).getCollection[D](collectionName, classTagToClassOf(ct))

  // Java helpers
  def getMongoClient(): MongoClient = mongoClient()
  def getDatabase(databaseName: String): MongoDatabase = mongoClient().getDatabase(databaseName)
  def getCollection(databaseName: String, collectionName: String): MongoCollection[Document] = collection(databaseName, collectionName)
  def getCollection[D](databaseName: String, collectionName: String, clazz: Class[D]): MongoCollection[D] =
    getDatabase(databaseName).getCollection[D](collectionName, clazz)

  override def close(): Unit = {
    logInfo("Closing MongoClient")
    if (fetchedClient) mongoClient().close()
  }

  private[this] def createMongoClient(): MongoClient = {
    logInfo(s"Creating MongoClient")
    fetchedClient = true
    mongoClientFactory()
  }
}
