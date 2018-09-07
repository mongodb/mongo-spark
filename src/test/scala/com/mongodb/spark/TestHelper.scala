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

import com.mongodb.client.model.{UpdateOptions, Updates}
import com.mongodb.client.{MongoCollection, MongoDatabase}
import com.mongodb.{MongoClient, MongoClientURI, ReadPreference}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.{BsonDocument, BsonString, Document}

import scala.collection.JavaConverters._
import scala.collection.immutable.IndexedSeq
import scala.io.Source
import scala.util._

object TestHelper {
  def apply(): TestHelper = new TestHelper()

  private var _sparkContext: Option[SparkContext] = None
  private var customConf: Boolean = false

  def getOrCreateSparkContext(sparkConf: SparkConf, requiredCustomConf: Boolean): SparkContext = synchronized {
    if (customConf != requiredCustomConf) resetSparkContext()
    customConf = requiredCustomConf
    _sparkContext.getOrElse({
      val sc = new SparkContext(sparkConf)
      _sparkContext = Some(sc)
      sc
    })
  }

  def resetSparkContext(): Unit = {
    _sparkContext.foreach { sc => sc.stop() }
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    _sparkContext = None
  }

}

class TestHelper extends Logging {
  val DEFAULT_URI: String = "mongodb://localhost:27017/"
  val MONGODB_URI_SYSTEM_PROPERTY_NAME: String = "org.mongodb.test.uri"
  val DATABASE_NAME: String = "mongo-spark-connector-test"

  val mongoClientURI: String = Properties.propOrElse(MONGODB_URI_SYSTEM_PROPERTY_NAME, DEFAULT_URI)

  lazy val mongoClient: MongoClient = new MongoClient(new MongoClientURI(mongoClientURI))

  def getMongoClientURI: String = mongoClientURI
  def getMongoClient(): MongoClient = mongoClient

  def getSparkConf(collectionName: String): SparkConf = {
    new SparkConf()
      .setMaster("local")
      .setAppName("MongoSparkConnector")
      .set("spark.driver.allowMultipleContexts", "false")
      .set("spark.sql.allowMultipleContexts", "false")
      .set("spark.app.id", "MongoSparkConnector")
      .set("spark.mongodb.input.uri", mongoClientURI)
      .set("spark.mongodb.input.database", DATABASE_NAME)
      .set("spark.mongodb.input.collection", collectionName)
      .set("spark.mongodb.input.partitioner", "TestPartitioner$")
      .set("spark.mongodb.output.uri", mongoClientURI)
      .set("spark.mongodb.output.database", DATABASE_NAME)
      .set("spark.mongodb.output.collection", collectionName)
  }

  def isMongoDBOnline(): Boolean = _isMongoDBOnline
  private lazy val _isMongoDBOnline = Try(mongoClient.listDatabaseNames()).isSuccess

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
      Try(adminDB.runCommand(Document.parse(s"{ enableSharding: '$DATABASE_NAME' }")))
      adminDB.runCommand(Document.parse(s"{shardCollection: '$ns', key: ${shardKey.toJson}, unique: true}"))
    }
    logInfo("Settings chunkSize to 1MB")
    configDB.getCollection("settings").updateOne(new Document("_id", "chunksize"), Updates.set("value", 1), new UpdateOptions().upsert(true))
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

  def loadSampleData(collectionName: String, sizeInMB: Int, numberOfDocuments: Int): Unit = {
    assert(isMongoDBOnline(), "MongoDB offline")
    assert(sizeInMB > 0, "Size in MB must be more than ")
    logInfo(s"Loading sample Data: ~${sizeInMB}MB data into '$collectionName'")
    val collection: MongoCollection[BsonDocument] = mongoClient.getDatabase(DATABASE_NAME)
      .withReadPreference(ReadPreference.primary())
      .getCollection(collectionName, classOf[BsonDocument])

    val sizeBytes = (1024 * 1024) * sizeInMB
    val fieldLength = sizeBytes / numberOfDocuments
    var counter = 0
    val sampleString: String = Random.alphanumeric.take(fieldLength - 60).mkString
    val documents: IndexedSeq[BsonDocument] = (1 to numberOfDocuments).map(y => {
      counter += 1
      val idValue = new BsonString(f"$counter%05d")
      new BsonDocument("_id", idValue).append("pk", idValue).append("s", new BsonString(sampleString))
    })
    collection.insertMany(documents.toList.asJava)
  }

  def loadSampleDataCompositeKey(collectionName: String, sizeInMB: Int): Unit = {
    assert(isMongoDBOnline(), "MongoDB offline")
    assert(sizeInMB > 0, "Size in MB must be more than ")
    logInfo(s"Loading sample Data: ~${sizeInMB}MB data into '$collectionName'")
    val collection: MongoCollection[BsonDocument] = mongoClient.getDatabase(DATABASE_NAME)
      .withReadPreference(ReadPreference.primary())
      .getCollection(collectionName, classOf[BsonDocument])
    val numberOfDocuments: Int = 10

    val fieldLength: Int = (1024 * 1024 / numberOfDocuments) - 60 // One MB / number of Documents - Some padding
    var counter = 0
    (1 to sizeInMB).foreach({ x =>
      val sampleString: String = Random.alphanumeric.take(fieldLength).mkString
      val documents: IndexedSeq[BsonDocument] = (1 to numberOfDocuments).map(y => {
        counter += 1
        val idValue = new BsonString(f"$counter%05d")
        val bValue = new BsonString(f"${counter % 2}%05d")
        new BsonDocument("_id", new BsonDocument("a", idValue).append("b", bValue)).append("s", new BsonString(sampleString))
      })
      collection.insertMany(documents.toList.asJava)
    })
  }

  lazy val adminDB: MongoDatabase = mongoClient.getDatabase("admin")
  lazy val configDB: MongoDatabase = mongoClient.getDatabase("config")

}
