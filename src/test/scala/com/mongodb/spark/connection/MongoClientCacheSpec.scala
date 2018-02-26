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

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import org.scalamock.scalatest.proxy.MockFactory
import com.mongodb.Implicits._
import com.mongodb.spark.{JavaRequiresMongoDB, RequiresMongoDB}
import com.mongodb.spark.config.ReadConfig

class MongoClientCacheSpec extends RequiresMongoDB with MockFactory {

  val zeroDuration = Duration(0, TimeUnit.MILLISECONDS)
  val longDuration = Duration(1, TimeUnit.MINUTES)
  val keepAlive = Duration(250, TimeUnit.MILLISECONDS) // scalastyle:off
  val clientCache = new MongoClientCache(keepAlive)
  val clientFactory = DefaultMongoClientFactory(ReadConfig(sparkConf).asOptions)
  val clientFactory2 = DefaultMongoClientFactory(ReadConfig(sparkConf).asOptions)

  "MongoClientCache" should "create a client and then close the client once released" in {
    val client = clientCache.acquire(clientFactory)
    clientCache.release(client, zeroDuration)
    client.cluster.isClosed should be(true)
  }

  it should "create a client and then close the client once released and after the timeout" in {
    val client = clientCache.acquire(clientFactory)
    clientCache.release(client)
    client.cluster.isClosed should be(false)
    Thread.sleep(keepAlive.toMillis * 2)
    client.cluster.isClosed should be(true)
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

    client.cluster.isClosed should be(true)
    client2.cluster.isClosed should be(true)
  }

}
