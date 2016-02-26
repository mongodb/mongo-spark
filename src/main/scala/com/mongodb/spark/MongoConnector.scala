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

import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.{Logging, SparkConf}

import org.bson.codecs.configuration.CodecRegistry
import com.mongodb.client.{MongoCollection, MongoDatabase}
import com.mongodb.spark.config.CollectionConfig
import com.mongodb.spark.connection.{DefaultMongoClientFactory, MongoClientCache}
import com.mongodb.{MongoClient, ServerAddress}

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
    require(sparkConf.contains(mongoURIProperty), s"Missing '$mongoURIProperty' property from sparkConfig")
    MongoConnector(sparkConf.get(mongoURIProperty))
  }

  /**
   * Creates a MongoConnector
   *
   * @param connectionString the connection string (`uri`)
   * @return the MongoConnector
   */
  def apply(connectionString: String): MongoConnector = MongoConnector(DefaultMongoClientFactory(connectionString))

  private[spark] val mongoURIProperty: String = "mongodb.uri"
  private[spark] val mongoClientKeepAlive = Duration(10, TimeUnit.SECONDS) // scalastyle:ignore

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

  def withMongoClientDo[T](code: MongoClient => T): T = withMongoClientDo(code, None)

  def withMongoClientDo[T](code: MongoClient => T, serverAddress: Option[ServerAddress]): T = {
    val client = MongoConnector.mongoClientCache.acquire(serverAddress, mongoClientFactory)
    try {
      code(client)
    } finally {
      MongoConnector.mongoClientCache.release(client)
    }
  }

  def withDatabaseDo[T](config: CollectionConfig, code: MongoDatabase => T, serverAddress: Option[ServerAddress] = None): T =
    withMongoClientDo({ client => code(client.getDatabase(config.databaseName)) }, serverAddress)

  def withCollectionDo[D, T](config: CollectionConfig, code: MongoCollection[D] => T,
                             serverAddress: Option[ServerAddress] = None)(implicit ct: ClassTag[D]): T =
    withDatabaseDo(config, { db => code(db.getCollection[D](config.collectionName, classTagToClassOf(ct))) }, serverAddress)

  private[spark] def codecRegistry: CodecRegistry = withMongoClientDo({ client => client.getMongoClientOptions.getCodecRegistry })

  override def close(): Unit = MongoConnector.mongoClientCache.shutdown()

}
