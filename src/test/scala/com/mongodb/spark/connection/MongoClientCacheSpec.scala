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

package com.mongodb.spark.connection

import java.util
import java.util.concurrent.TimeUnit

import com.mongodb.ClientSessionOptions

import scala.concurrent.duration.Duration
import org.scalamock.scalatest.proxy.MockFactory
import com.mongodb.client.{ChangeStreamIterable, ClientSession, ListDatabasesIterable, MongoClient, MongoDatabase, MongoIterable}
import com.mongodb.connection.{ClusterDescription, ClusterSettings}
import com.mongodb.spark.{JavaRequiresMongoDB, MongoClientFactory, RequiresMongoDB}
import com.mongodb.spark.config.ReadConfig
import org.bson.Document
import org.bson.conversions.Bson

class MongoClientCacheSpec extends RequiresMongoDB with MockFactory {

  import MongoClientCacheSpec._

  val zeroDuration = Duration(0, TimeUnit.MILLISECONDS)
  val longDuration = Duration(1, TimeUnit.MINUTES)
  val keepAlive = Duration(250, TimeUnit.MILLISECONDS) // scalastyle:off
  val clientCache = new MongoClientCache(keepAlive)

  val clientFactory = TestMongoClientFactory(DefaultMongoClientFactory(ReadConfig(sparkConf).asOptions))
  val clientFactory2 = TestMongoClientFactory(DefaultMongoClientFactory(ReadConfig(sparkConf).asOptions))

  "MongoClientCache" should "create a client and then close the client once released" in {
    val client = clientCache.acquire(clientFactory)
    clientCache.release(client, zeroDuration)
    client.isClosed should be(true)
  }

  it should "create a client and then close the client once released and after the timeout" in {
    val client = clientCache.acquire(clientFactory)
    clientCache.release(client)
    client.isClosed should be(false)
    Thread.sleep(keepAlive.toMillis * 2)
    client.isClosed should be(true)
  }

  it should "return a different client once released " in {
    val client = clientCache.acquire(clientFactory)
    val client2 = clientCache.acquire(clientFactory2)

    client2 should be theSameInstanceAs client

    clientCache.release(client, zeroDuration)
    clientCache.release(client2, zeroDuration)

    val client3 = clientCache.acquire(clientFactory)

    client3 should not be theSameInstanceAs(client)
    clientCache.release(client3, zeroDuration)
  }

  it should "not throw an exception when trying to release unacquired client" in {
    clientCache.release(mongoClient, zeroDuration)
    clientCache.release(mongoClient)
    Thread.sleep(keepAlive.toMillis * 2)
  }

  it should "eventually close all released clients on shutdown" in {
    val client = clientCache.acquire(clientFactory)
    val client2 = clientCache.acquire(clientFactory)

    clientCache.release(client, longDuration)
    clientCache.release(client2, longDuration)

    clientCache.shutdown()

    client.isClosed should be(true)
    client2.isClosed should be(true)
  }

}
object MongoClientCacheSpec {
  implicit class MongoClientWrapper(val mongoClient: MongoClient) extends AnyVal {
    def isClosed: Boolean = mongoClient match {
      case client: TestMongoClient => client.isClosed
      case _                       => false
    }

  }

  case class TestMongoClientFactory(delegate: DefaultMongoClientFactory) extends MongoClientFactory {
    override def create(): MongoClient = TestMongoClient(delegate.create())
  }

  case class TestMongoClient(delegate: MongoClient) extends MongoClient {
    var closed = false;

    def isClosed: Boolean = closed

    override def getDatabase(databaseName: String): MongoDatabase = delegate.getDatabase(databaseName)

    override def startSession(): ClientSession = delegate.startSession()

    override def startSession(options: ClientSessionOptions): ClientSession = delegate.startSession(options)

    override def close(): Unit = {
      closed = true
      delegate.close()
    }

    override def listDatabaseNames(): MongoIterable[String] = delegate.listDatabaseNames()

    override def listDatabaseNames(clientSession: ClientSession): MongoIterable[String] = delegate.listDatabaseNames(clientSession)

    override def listDatabases(): ListDatabasesIterable[Document] = delegate.listDatabases()

    override def listDatabases(clientSession: ClientSession): ListDatabasesIterable[Document] = delegate.listDatabases(clientSession)

    override def listDatabases[TResult](resultClass: Class[TResult]): ListDatabasesIterable[TResult] = delegate.listDatabases(resultClass)

    override def listDatabases[TResult](clientSession: ClientSession, resultClass: Class[TResult]): ListDatabasesIterable[TResult] = delegate.listDatabases(clientSession, resultClass)

    override def watch(): ChangeStreamIterable[Document] = delegate.watch()

    override def watch[TResult](resultClass: Class[TResult]): ChangeStreamIterable[TResult] = delegate.watch(resultClass)

    override def watch(pipeline: util.List[_ <: Bson]): ChangeStreamIterable[Document] = delegate.watch(pipeline)

    override def watch[TResult](pipeline: util.List[_ <: Bson], resultClass: Class[TResult]): ChangeStreamIterable[TResult] = delegate.watch(pipeline, resultClass)

    override def watch(clientSession: ClientSession): ChangeStreamIterable[Document] = delegate.watch(clientSession)

    override def watch[TResult](clientSession: ClientSession, resultClass: Class[TResult]): ChangeStreamIterable[TResult] = delegate.watch(clientSession, resultClass)

    override def watch(clientSession: ClientSession, pipeline: util.List[_ <: Bson]): ChangeStreamIterable[Document] = delegate.watch(clientSession, pipeline)

    override def watch[TResult](clientSession: ClientSession, pipeline: util.List[_ <: Bson], resultClass: Class[TResult]): ChangeStreamIterable[TResult] = delegate.watch(clientSession, pipeline, resultClass)

    override def getClusterDescription: ClusterDescription = delegate.getClusterDescription

  }
}