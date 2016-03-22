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

class DefaultMongoClientFactorySpec extends RequiresMongoDB {

  "DefaultMongoClientFactory" should "create a MongoClient from the connection string" in {
    val client = DefaultMongoClientFactory(mongoClientURI).create()
    client shouldBe a[MongoClient]
    client.close()
  }

  it should "implement equals based on the connection string" in {
    val factory1 = DefaultMongoClientFactory(mongoClientURI)
    val factory2 = DefaultMongoClientFactory(mongoClientURI)

    factory1 should equal(factory2)
  }

  it should "validate the connection string" in {
    an[IllegalArgumentException] should be thrownBy new DefaultMongoClientFactory("invalid connection string")
  }

}
