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

import com.mongodb.{MongoClient, MongoClientOptions, MongoClientURI, MongoDriverInformation}

import scala.util.Try
import com.mongodb.spark.MongoClientFactory
import com.mongodb.spark.config.{BuildInfo, MongoSharedConfig, ReadConfig}
import org.apache.spark.SPARK_VERSION

private[spark] object DefaultMongoClientFactory {
  def apply(options: collection.Map[String, String]): DefaultMongoClientFactory = {
    val cleanedOptions = ReadConfig.stripPrefix(options)
    require(cleanedOptions.contains(MongoSharedConfig.mongoURIProperty), s"Missing '${MongoSharedConfig.mongoURIProperty}' property from options")
    DefaultMongoClientFactory(
      cleanedOptions.get(MongoSharedConfig.mongoURIProperty).get,
      cleanedOptions.get(ReadConfig.localThresholdProperty).map(_.toInt)
    )
  }
}

private[spark] case class DefaultMongoClientFactory(connectionString: String, localThreshold: Option[Int] = None) extends MongoClientFactory {
  require(Try(new MongoClientURI(connectionString)).isSuccess, s"Invalid '${MongoSharedConfig.mongoURIProperty}' '$connectionString'")

  @transient private lazy val mongoDriverInformation = MongoDriverInformation.builder().driverName("mongo-spark")
    .driverVersion(BuildInfo.version)
    .driverPlatform(s"Scala/${BuildInfo.scalaVersion}:Spark/${SPARK_VERSION}")
    .build()

  override def create(): MongoClient = {
    val builder = new MongoClientOptions.Builder
    localThreshold.map(builder.localThreshold)
    val clientURI = new MongoClientURI(connectionString, builder)
    new MongoClient(clientURI, mongoDriverInformation)
  }
}
