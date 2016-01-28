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

import org.scalatest._

import org.apache.spark.{Logging, SparkConf, SparkContext}

import org.bson.Document
import com.mongodb.Implicits._
import com.mongodb.MongoClient
import com.mongodb.client.{MongoCollection, MongoDatabase}
import com.mongodb.connection.ClusterType.{REPLICA_SET, SHARDED, STANDALONE}
import com.mongodb.spark.conf.ReadConfig
import com.mongodb.spark.rdd.MongoRDD

trait RequiresMongoDB extends FlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with Logging {

  private val mongoDBDefaults: MongoDBDefaults = MongoDBDefaults()
  private var _currentTestName: Option[String] = None
  private var mongoDBOnline: Boolean = false

  private var requestedSparkContext: Boolean = false
  private def sparkContext: SparkContext = _sparkContext
  private lazy val _sparkContext: SparkContext = {
    requestedSparkContext = true
    new SparkContext(sparkConf)
  }

  protected override def runTest(testName: String, args: Args): Status = {
    if (!requestedSparkContext) _currentTestName = Some(testName.split("should")(1).trim)
    mongoDBOnline = mongoDBDefaults.isMongoDBOnline()
    super.runTest(testName, args)
  }

  lazy val mongoClientURI: String = mongoDBDefaults.mongoClientURI

  lazy val mongoClient: MongoClient = mongoDBDefaults.mongoClient

  lazy val database: MongoDatabase = mongoClient.getDatabase(databaseName)

  lazy val collection: MongoCollection[Document] = database.getCollection(readConfig.collectionName)

  lazy val isStandalone: Boolean = mongoClient.cluster.getDescription.getType == STANDALONE

  lazy val isReplicaSet: Boolean = mongoClient.cluster.getDescription.getType == REPLICA_SET

  lazy val isSharded: Boolean = mongoClient.cluster.getDescription.getType == SHARDED

  def readConfig: ReadConfig = ReadConfig(sparkContext.getConf)

  def shardCollection(): Unit = mongoDBDefaults.shardCollection(readConfig.collectionName, new Document("_id", 1))

  def loadSampleDataIntoShardedCollection(sizeInMB: Int): Unit = {
    shardCollection()
    loadSampleData(sizeInMB)
  }

  def loadSampleData(filename: String): Unit = mongoDBDefaults.loadSampleData(readConfig.collectionName, filename)

  def loadSampleData(sizeInMB: Int): Unit = mongoDBDefaults.loadSampleData(readConfig.collectionName, sizeInMB)

  def mongoConnector: MongoConnector = MongoConnector(mongoClientURI)

  def sparkConf: SparkConf = sparkConf(collectionName)

  def sparkConf(collectionName: String): SparkConf = mongoDBDefaults.getSparkConf(collectionName)

  /**
   * Test against a set SparkContext
   *
   * @param testCode the test case
   */
  def withSparkContext()(testCode: SparkContext => Any) {
    checkMongoDB()
    try {
      logInfo(s"Running Test: '${_currentTestName.getOrElse(suiteName)}'")
      testCode(sparkContext) // "loan" the fixture to the test
    } finally {
      sparkContext.dropDatabase()
    }
  }

  implicit class MongoSparkContext(sc: SparkContext) {
    def dropDatabase(): Unit = MongoRDD(sc).connector.value.database(databaseName).drop()
  }

  /**
   * The database name to use for this test
   */
  val databaseName: String = mongoDBDefaults.DATABASE_NAME

  /**
   * The collection name to use for this test
   */
  def collectionName: String = _currentTestName.getOrElse(suiteName).filter(_.isLetterOrDigit)

  def checkMongoDB() {
    if (!mongoDBOnline) {
      cancel("No Available MongoDB")
    }
  }

  override def beforeEach() {
    if (mongoDBOnline) collection.drop()
  }

  override def beforeAll() {
    mongoDBDefaults.dropDB()
  }

  override def afterAll() {
    mongoDBDefaults.dropDB()
    if (requestedSparkContext) sparkContext.stop()
  }

}
