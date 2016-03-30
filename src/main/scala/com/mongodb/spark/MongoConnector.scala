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
import java.util.concurrent.TimeUnit

import com.mongodb.MongoClient
import com.mongodb.client.{MongoCollection, MongoDatabase}
import com.mongodb.spark.config.{MongoCollectionConfig, ReadConfig, WriteConfig}
import com.mongodb.spark.connection.{DefaultMongoClientFactory, MongoClientCache}
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.codecs.configuration.CodecRegistry

import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

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
   * @param options the configuration options
   * @return the MongoConnector
   */
  def apply(options: collection.Map[String, String]): MongoConnector = new MongoConnector(DefaultMongoClientFactory(options))

  private[spark] val mongoClientKeepAlive = Duration(System.getProperty("spark.mongodb.keep_alive_ms", "5000").toInt, TimeUnit.MILLISECONDS)
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
    extends Serializable with Closeable with Logging {

  /**
   * Execute some code on a `MongoClient`
   *
   * *Note:* The MongoClient is reference counted and loaned to the `code` method and should only be used inside that function.
   *
   * @param code the code block that is passed
   * @tparam T the result of the code function
   * @return the result
   */
  def withMongoClientDo[T](code: MongoClient => T): T = {
    val client = MongoConnector.mongoClientCache.acquire(mongoClientFactory)
    try {
      code(client)
    } finally {
      MongoConnector.mongoClientCache.release(client)
    }
  }

  /**
   * Execute some code on a database
   *
   * *Note:* The `MongoDatabase` is reference counted and loaned to the `code` method and should only be used inside that function.
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
   * *Note:* The `MongoCollection` is reference counted and loaned to the `code` method and should only be used inside that function.
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

  private[spark] def codecRegistry: CodecRegistry = withMongoClientDo({ client => client.getMongoClientOptions.getCodecRegistry })

  override def close(): Unit = MongoConnector.mongoClientCache.shutdown()

}
