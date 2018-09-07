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

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import org.bson.{BsonDocument, Document}
import com.mongodb.Implicits._
import com.mongodb.MongoClient
import com.mongodb.client.{MongoCollection, MongoDatabase}
import com.mongodb.connection.ClusterType.{REPLICA_SET, SHARDED, STANDALONE}
import com.mongodb.connection.ServerVersion
import com.mongodb.spark.config.ReadConfig

import org.scalatest._

trait RequiresMongoDB extends FlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with LoggingTrait {

  private val testHelper: TestHelper = TestHelper()
  private var _currentTestName: Option[String] = None
  private var mongoDBOnline: Boolean = false

  def sparkContext: SparkContext = TestHelper.getOrCreateSparkContext(sparkConf, requiredCustomConf = false)

  def sparkContext(sparkConf: SparkConf): SparkContext = TestHelper.getOrCreateSparkContext(sparkConf, requiredCustomConf = true)

  protected override def runTest(testName: String, args: Args): Status = {
    _currentTestName = Some(testName.trim)
    mongoDBOnline = testHelper.isMongoDBOnline()
    super.runTest(testName, args)
  }

  lazy val mongoClientURI: String = testHelper.mongoClientURI

  lazy val mongoClient: MongoClient = testHelper.mongoClient

  lazy val database: MongoDatabase = mongoClient.getDatabase(databaseName)

  lazy val collection: MongoCollection[Document] = database.getCollection(readConfig.collectionName)

  lazy val isStandalone: Boolean = mongoClient.cluster.getDescription.getType == STANDALONE

  lazy val isReplicaSet: Boolean = mongoClient.cluster.getDescription.getType == REPLICA_SET

  lazy val isSharded: Boolean = mongoClient.cluster.getDescription.getType == SHARDED

  val serverVersionMap: mutable.Map[String, Boolean] = mutable.Map.empty[String, Boolean]
  def serverAtLeast(versions: Int*): Boolean = {
    val fullVersion = versions.padTo(3, 0)
    val versionString = fullVersion.mkString(".")
    serverVersionMap.getOrElseUpdate(versionString, {
      val buildInfo: BsonDocument = database.runCommand(new Document("buildInfo", 1), classOf[BsonDocument])
      val serverVersionArray: util.List[Integer] = buildInfo.getArray("versionArray").asScala.take(3).map(_.asInt32().getValue.asInstanceOf[Integer]).asJava
      val versionArray = fullVersion.toList.asJava.asInstanceOf[util.List[Integer]]
      new ServerVersion(serverVersionArray).compareTo(new ServerVersion(versionArray)) >= 0
    })
  }

  def readConfig: ReadConfig = ReadConfig(sparkConf)

  def shardCollection(): Unit = shardCollection(readConfig.collectionName, new Document("_id", 1))

  def shardCollection(collectionName: String, shardKey: Document): Unit = testHelper.shardCollection(collectionName, shardKey)

  def loadSampleDataIntoShardedCollection(sizeInMB: Int): Unit = {
    shardCollection(readConfig.collectionName, Document.parse("{_id: 1, pk: 1}"))
    loadSampleData(sizeInMB)
  }

  def loadSampleDataIntoShardedCompoundKeyCollection(sizeInMB: Int): Unit = {
    shardCollection()
    loadSampleData(sizeInMB)
  }

  def loadSampleData(filename: String): Unit = testHelper.loadSampleData(readConfig.collectionName, filename)

  def loadSampleData(sizeInMB: Int): Unit = loadSampleData(sizeInMB, sizeInMB * 10)

  def loadSampleData(sizeInMB: Int, numberOfDocuments: Int): Unit =
    testHelper.loadSampleData(readConfig.collectionName, sizeInMB, numberOfDocuments)

  def loadSampleDataCompositeKey(sizeInMB: Int): Unit = testHelper.loadSampleDataCompositeKey(readConfig.collectionName, sizeInMB)

  def mongoConnector: MongoConnector = MongoConnector(sparkConf)

  def sparkConf: SparkConf = sparkConf(collectionName)

  def sparkConf(collectionName: String): SparkConf = testHelper.getSparkConf(collectionName)

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
      database.drop()
    }
  }

  /**
   * Test against a set Spark Session
   *
   * @param testCode the test case
   */
  def withSparkSession()(testCode: SparkSession => Any) {
    withSparkContext()({ sparkContext: SparkContext =>
      {
        testCode(SparkSession.builder().getOrCreate())
      }
    })
  }

  /**
   * The database name to use for this test
   */
  val databaseName: String = testHelper.DATABASE_NAME

  /**
   * The collection name to use for this test
   */
  def collectionName: String = suiteName.filter(_.isLetterOrDigit)

  def checkMongoDB() {
    if (!mongoDBOnline) {
      cancel("No Available MongoDB")
    }
  }

  override def beforeEach() {
    if (mongoDBOnline) collection.drop()
  }

  override def beforeAll() {
    testHelper.dropDB()
  }

  override def afterAll() {
    testHelper.dropDB()
    TestHelper.resetSparkContext()
    logInfo(s"Ended Test: '${_currentTestName.getOrElse(suiteName)}'")
  }

}
