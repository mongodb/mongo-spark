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

import com.mongodb.MongoClient
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.connection.DefaultMongoClientFactory
import org.apache.spark.SparkConf
import org.scalatest.FlatSpec

class MongoConnectorSpec extends RequiresMongoDB {

  "MongoConnector" should "create a MongoClient" in {
    MongoConnector(sparkConf).withMongoClientDo({ client => true }) shouldBe true
  }

  it should "set the correct localThreshold" in {
    val conf = sparkConf.clone().set(s"${ReadConfig.configPrefix}${ReadConfig.localThresholdProperty}", "5")
    MongoConnector(conf).withMongoClientDo({ client =>
      client.getMongoClientOptions.getLocalThreshold == 5
    }) shouldBe true
  }

  it should "Use the cache for MongoClients" in {
    MongoConnector(sparkConf).withMongoClientDo({ client =>
      MongoConnector(sparkConf).withMongoClientDo({ client2 => client == client2 })
    }) shouldBe true
  }

  it should "create a MongoClient with a custom MongoConnectionFactory" in {
    MongoConnector(CustomMongoClientFactory(sparkConf)).withMongoClientDo({ client => true }) shouldBe true
  }

  case class CustomMongoClientFactory(sparkConf: SparkConf) extends MongoClientFactory {
    private final val proxy: DefaultMongoClientFactory = DefaultMongoClientFactory(ReadConfig(sparkConf).asOptions)

    def create(): MongoClient = proxy.create()
  }

}
