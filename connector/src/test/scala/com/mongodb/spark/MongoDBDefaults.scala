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

import scala.collection.JavaConverters._
import scala.collection.immutable.IndexedSeq
import scala.io.Source
import scala.util._

import org.apache.spark.{Logging, SparkConf}

import org.bson.Document
import com.mongodb.client.model.Updates
import com.mongodb.client.{MongoCollection, MongoDatabase}
import com.mongodb.{MongoClient, MongoClientURI, ReadPreference}

object MongoDBDefaults {
  def apply(): MongoDBDefaults = new MongoDBDefaults()
}

class MongoDBDefaults extends Logging {
  val DEFAULT_URI: String = "mongodb://localhost:27017/"
  val MONGODB_URI_SYSTEM_PROPERTY_NAME: String = "org.mongodb.test.uri"
  val DATABASE_NAME = "mongo-spark-connector-test"

  val mongoClientURI = Properties.propOrElse(MONGODB_URI_SYSTEM_PROPERTY_NAME, DEFAULT_URI)

  lazy val mongoClient = new MongoClient(new MongoClientURI(mongoClientURI))

  def getMongoClientURI: String = mongoClientURI
  def getMongoClient(): MongoClient = mongoClient

  def getSparkConf(collectionName: String): SparkConf = {
    new SparkConf()
      .setMaster("local")
      .setAppName("MongoSparkConnector")
      .set("spark.app.id", "MongoSparkConnector")
      .set("mongodb.uri", mongoClientURI)
      .set("mongodb.input.databaseName", DATABASE_NAME)
      .set("mongodb.input.collectionName", collectionName)
      .set("mongodb.output.databaseName", DATABASE_NAME)
      .set("mongodb.output.collectionName", collectionName)
  }

  def isMongoDBOnline(): Boolean = Try(mongoClient.listDatabaseNames()).isSuccess

  def dropDB(): Unit = {
    if (isMongoDBOnline()) {
      mongoClient.getDatabase(DATABASE_NAME).drop()
    }
  }

  def shardCollection(collectionName: String, shardKey: Document): Unit = {
    val ns: String = s"$DATABASE_NAME.$collectionName"

    if (Option(configDB.getCollection("databases").find(new Document("_id", DATABASE_NAME)).first()).isEmpty) enableSharding()
    if (Option(configDB.getCollection("collections").find(new Document("_id", ns).append("dropped", false)).first()).isEmpty) {
      logInfo(s"Sharding: $ns")
      adminDB.runCommand(Document.parse(s"{shardCollection: '$ns', key: ${shardKey.toJson}, unique: true}"))
    }
    logInfo("Settings chunkSize to 1MB")
    configDB.getCollection("settings").updateOne(new Document("_id", "chunksize"), Updates.set("value", 1))
  }

  def enableSharding(): Unit = {
    logInfo("Enabling Sharding")
    adminDB.runCommand(new Document("enableSharding", DATABASE_NAME))
  }

  def loadSampleData(collectionName: String, name: String): Unit = {
    // TODO review usage and remove if not required before 1.0
    assert(isMongoDBOnline(), "MongoDB offline")
    logInfo(s"Loading sample Data: '$name' data into '$collectionName'")
    val collection: MongoCollection[Document] = mongoClient.getDatabase(DATABASE_NAME).getCollection(collectionName)
    Try(Source.fromURL(getClass.getResource(name)).getLines().map(x => Document.parse(x))) match {
      case Success(documents) => collection.insertMany(documents.toList.asJava)
      case Failure(e)         => throw e
    }
  }

  def loadSampleData(collectionName: String, sizeInMB: Int): Unit = {
    assert(isMongoDBOnline(), "MongoDB offline")
    assert(sizeInMB > 0, "Size in MB must be more than ")
    logInfo(s"Loading sample Data: ~${sizeInMB}MB data into '$collectionName'")
    val collection: MongoCollection[Document] = mongoClient.getDatabase(DATABASE_NAME)
      .withReadPreference(ReadPreference.primary())
      .getCollection(collectionName)
    val random: Random = new Random()
    val numberOfDocuments: Int = 10
    val fieldLength: Int = (1024 * 1024 / numberOfDocuments) - 30 // One MB / number of Documents - Some padding
    (1 to sizeInMB).foreach({ x =>
      val sampleString: String = Random.alphanumeric.take(fieldLength).mkString
      val documents: IndexedSeq[Document] = (1 to numberOfDocuments).map(y => new Document("s", sampleString))
      collection.insertMany(documents.toList.asJava)
    })
  }

  lazy val adminDB: MongoDatabase = mongoClient.getDatabase("admin")
  lazy val configDB: MongoDatabase = mongoClient.getDatabase("config")

  Runtime.getRuntime.addShutdownHook(new ShutdownHook())
  private[mongodb] class ShutdownHook extends Thread {
    override def run() {
      Try(mongoClient.getDatabase(DATABASE_NAME).drop())
    }
  }
}
