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
import java.util
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.Function
import org.apache.spark.{SparkConf, SparkContext}

import org.bson.codecs.configuration.CodecRegistry
import org.bson.{BsonDocument, Document}
import com.mongodb.MongoClient
import com.mongodb.client.{MongoCollection, MongoDatabase}
import com.mongodb.connection.ServerVersion
import com.mongodb.spark.config.{MongoCollectionConfig, ReadConfig, WriteConfig}
import com.mongodb.spark.connection.{DefaultMongoClientFactory, MongoClientCache}

/**
 * The MongoConnector companion object
 *
 * @since 1.0
 */
object MongoConnector {

  /**
   * Creates a MongoConnector using the [[com.mongodb.spark.config.ReadConfig.mongoURIProperty]] from the `sparkConf`.
   *
   * @param sparkContext the Spark context
   * @return the MongoConnector
   */
  def apply(sparkContext: SparkContext): MongoConnector = apply(sparkContext.getConf)

  /**
   * Creates a MongoConnector using the [[com.mongodb.spark.config.ReadConfig]] from the `sparkConf`.
   *
   * @param sparkConf the Spark configuration.
   * @return the MongoConnector
   */
  def apply(sparkConf: SparkConf): MongoConnector = apply(ReadConfig(sparkConf).asOptions)

  /**
   * Creates a MongoConnector
   *
   * @param readConfig the readConfig
   * @return the MongoConnector
   */
  def apply(readConfig: ReadConfig): MongoConnector = apply(readConfig.asOptions)

  /**
   * Creates a MongoConnector
   *
   * @param options the configuration options
   * @return the MongoConnector
   */
  def apply(options: collection.Map[String, String]): MongoConnector = new MongoConnector(DefaultMongoClientFactory(options))

  /**
   * Creates a MongoConnector
   *
   * @param javaSparkContext the Java Spark context
   * @return the MongoConnector
   */
  def create(javaSparkContext: JavaSparkContext): MongoConnector = {
    notNull("javaSparkContext", javaSparkContext)
    MongoConnector(javaSparkContext.sc)
  }

  /**
   * Creates a MongoConnector
   *
   * @param sparkConf the spark configuration
   * @return the MongoConnector
   */
  def create(sparkConf: SparkConf): MongoConnector = {
    notNull("sparkConf", sparkConf)
    apply(sparkConf)
  }

  /**
   * Creates a MongoConnector
   *
   * @param options the configuration options
   * @return the MongoConnector
   */
  def create(options: java.util.Map[String, String]): MongoConnector = {
    notNull("options", options)
    apply(options.asScala)
  }

  /**
   * Creates a MongoConnector
   *
   * @param mongoClientFactory the factory for creating the MongoClient
   * @return the MongoConnector
   */
  def create(mongoClientFactory: MongoClientFactory): MongoConnector = {
    notNull("mongoClientFactory", mongoClientFactory)
    MongoConnector(mongoClientFactory)
  }

  private[spark] val mongoClientKeepAlive = Duration(System.getProperty(
    "mongodb.keep_alive_ms",
    System.getProperty("spark.mongodb.keep_alive_ms", "5000")
  ).toInt, TimeUnit.MILLISECONDS)
  private val mongoClientCache = new MongoClientCache(mongoClientKeepAlive)
  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    def run() {
      mongoClientCache.shutdown()
    }
  }))
}

/**
 * The MongoConnector
 *
 * Connects Spark to MongoDB
 *
 * @param mongoClientFactory the factory that can be used to create a MongoClient
 * @since 1.0
 */
case class MongoConnector(mongoClientFactory: MongoClientFactory)
    extends Logging with Serializable with Closeable {

  /**
   * Execute some code on a `MongoClient`
   *
   * '''Note:''' The MongoClient is reference counted and loaned to the `code` method and should only be used inside that function.
   *
   * @param code the code block that is passed
   * @tparam T the result of the code function
   * @return the result
   */
  def withMongoClientDo[T](code: MongoClient => T): T = {
    val client = acquireClient()
    try {
      code(client)
    } finally {
      releaseClient(client)
    }
  }

  /**
   * Execute some code on a database
   *
   * '''Note:''' The `MongoDatabase` is reference counted and loaned to the `code` method and should only be used inside that function.
   *
   * @param config the [[com.mongodb.spark.config.MongoCollectionConfig]] determining which database to connect to
   * @param code the code block that is executed
   * @tparam T the result of the code function
   * @return the result
   */
  def withDatabaseDo[T](config: MongoCollectionConfig, code: MongoDatabase => T): T =
    withMongoClientDo({ client => code(client.getDatabase(config.databaseName)) })

  /**
   * Execute some code on a collection
   *
   * '''Note:''' The `MongoCollection` is reference counted and loaned to the `code` method and should only be used inside that function.
   *
   * @param config the [[com.mongodb.spark.config.MongoCollectionConfig]] determining which database and collection to connect to
   * @param code the code block that is executed
   * @tparam T the result of the code function
   * @return the result
   */
  def withCollectionDo[D, T](config: MongoCollectionConfig, code: MongoCollection[D] => T)(implicit ct: ClassTag[D]): T =
    withDatabaseDo(config, { db =>
      val collection = db.getCollection[D](config.collectionName, classTagToClassOf(ct))
      code(config match {
        case writeConfig: WriteConfig => collection.withWriteConcern(writeConfig.writeConcern)
        case readConfig: ReadConfig   => collection.withReadConcern(readConfig.readConcern).withReadPreference(readConfig.readPreference)
        case _                        => collection
      })
    })

  /**
   * A Java friendly way to execute some code on a `MongoClient`
   *
   * '''Note:''' The MongoClient is reference counted and loaned to the `code` method and should only be used inside that function.
   *
   * @param code the code block that is passed
   * @tparam T the result of the code function
   * @return the result
   */
  def withMongoClientDo[T](code: Function[MongoClient, T]): T = withMongoClientDo[T]({ client => code.call(client) })

  /**
   * A Java friendly way to execute some code on a database
   *
   * '''Note:''' The `MongoDatabase` is reference counted and loaned to the `code` method and should only be used inside that function.
   *
   * @param config the [[com.mongodb.spark.config.MongoCollectionConfig]] determining which database to connect to
   * @param code the code block that is executed
   * @tparam T the result of the code function
   * @return the result
   */
  def withDatabaseDo[T](config: MongoCollectionConfig, code: Function[MongoDatabase, T]): T =
    withDatabaseDo[T](config, { mongoDatabase => code.call(mongoDatabase) })

  /**
   * A Java friendly way to execute some code on a collection
   *
   * '''Note:''' The `MongoCollection` is reference counted and loaned to the `code` method and should only be used inside that function.
   *
   * @param config the [[com.mongodb.spark.config.MongoCollectionConfig]] determining which database and collection to connect to
   * @param clazz the class representing documents from the collection
   * @param code the code block that is executed
   * @tparam T the result of the code function
   * @return the result
   */
  def withCollectionDo[D, T](config: MongoCollectionConfig, clazz: Class[D], code: Function[MongoCollection[D], T]): T = {
    implicit def ct: ClassTag[D] = ClassTag(clazz)
    withCollectionDo[D, T](config, { collection => code.call(collection) })
  }

  private[spark] def hasSampleAggregateOperator(readConfig: ReadConfig): Boolean = {
    val buildInfo: BsonDocument = withDatabaseDo(readConfig, { db => db.runCommand(new Document("buildInfo", 1), classOf[BsonDocument]) })
    val versionArray: util.List[Integer] = buildInfo.getArray("versionArray").asScala.take(3).map(_.asInt32().getValue.asInstanceOf[Integer]).asJava
    new ServerVersion(versionArray).compareTo(new ServerVersion(3, 2)) >= 0
  }

  private[spark] def acquireClient(): MongoClient = MongoConnector.mongoClientCache.acquire(mongoClientFactory)
  private[spark] def releaseClient(client: MongoClient): Unit = MongoConnector.mongoClientCache.release(client)
  private[spark] def codecRegistry: CodecRegistry = withMongoClientDo({ client => client.getMongoClientOptions.getCodecRegistry })

  override def close(): Unit = MongoConnector.mongoClientCache.shutdown()

}
