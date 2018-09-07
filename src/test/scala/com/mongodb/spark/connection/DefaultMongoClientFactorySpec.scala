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

import com.mongodb.MongoClient
import com.mongodb.spark.RequiresMongoDB
import com.mongodb.spark.config.ReadConfig

class DefaultMongoClientFactorySpec extends RequiresMongoDB {

  "DefaultMongoClientFactory" should "create a MongoClient from the connection string" in {
    val client = DefaultMongoClientFactory(ReadConfig(sparkConf).asOptions).create()
    client shouldBe a[MongoClient]
    client.close()
  }

  it should "implement equals based on the prefix less options map" in {
    val factory1 = DefaultMongoClientFactory(ReadConfig(sparkConf).asOptions)
    val factory2 = DefaultMongoClientFactory(ReadConfig(sparkConf).asOptions)

    factory1 should equal(factory2)
  }

  it should "set the localThreshold correctly" in {
    val conf = sparkConf.clone().set(s"${ReadConfig.configPrefix}${ReadConfig.localThresholdProperty}", "0")
    val client = DefaultMongoClientFactory(ReadConfig(conf).asOptions).create()

    client.getMongoClientOptions.getLocalThreshold should equal(0)
    client.close()
  }

  it should "validate the connection string" in {
    an[IllegalArgumentException] should be thrownBy DefaultMongoClientFactory(Map("uri" -> "invalid connection string"))
  }

}
