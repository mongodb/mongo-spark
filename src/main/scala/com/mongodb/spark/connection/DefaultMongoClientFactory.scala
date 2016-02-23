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

import com.mongodb.spark.MongoClientFactory
import com.mongodb.{ConnectionString, MongoClient, MongoClientURI, ServerAddress}

private[spark] case class DefaultMongoClientFactory(connectionString: String) extends MongoClientFactory {
  @transient val parsedConnectionString = new ConnectionString(connectionString)

  override def create(): MongoClient = new MongoClient(new MongoClientURI(connectionString))

  override def withServerAddress(serverAddress: ServerAddress): MongoClientFactory = {
    val prefix = "mongodb://"
    val withoutPrefix: String = connectionString.substring(prefix.length)
    val (hostAndAuth, options) = withoutPrefix.lastIndexOf("/") match {
      case n if n > -1 => (withoutPrefix.substring(0, n), withoutPrefix.substring(n))
      case _           => (withoutPrefix, "/")
    }
    val auth = hostAndAuth.lastIndexOf("@") match {
      case n if n > -1 => hostAndAuth.substring(0, n + 1)
      case _           => ""
    }
    val host = serverAddress.getHost match {
      case ipv6 if ipv6.contains(":") => s"[$ipv6]"
      case h                          => h
    }
    val port = serverAddress.getPort
    DefaultMongoClientFactory(s"$prefix$auth$host:$port$options")
  }
}
