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

import org.scalatest.prop.PropertyChecks

import com.mongodb.spark.RequiresMongoDB
import com.mongodb.{MongoClient, ServerAddress}

class DefaultMongoClientFactorySpec extends RequiresMongoDB with PropertyChecks {

  "DefaultMongoClientFactory" should "create a MongoClient from the connection string" in {
    val client = DefaultMongoClientFactory(mongoClientURI).create()
    client shouldBe a[MongoClient]
    client.close()
  }

  it should "be able to recreate the connection string" in {
    forAll(connectionStrings) { (initial: String, serverAddress: ServerAddress, expected: String) =>
      val factory = DefaultMongoClientFactory(initial).withServerAddress(serverAddress).asInstanceOf[DefaultMongoClientFactory]
      factory.connectionString should equal(expected)
    }
  }

  it should "validate the connection string" in {
    an[IllegalArgumentException] should be thrownBy new DefaultMongoClientFactory("invalid connection string")
  }

  val connectionStrings = Table(
    ("initial", "serverAddress", "expected"),
    ("mongodb://localhost", new ServerAddress(), "mongodb://127.0.0.1:27017/"),
    ("mongodb://127.0.0.1/", new ServerAddress("localhost"), "mongodb://localhost:27017/"),
    ("mongodb://127.0.0.1/", new ServerAddress("[::1]", 27018), "mongodb://[::1]:27018/"), // scalastyle:ignore
    ("mongodb://127.0.0.1/", new ServerAddress("[0:0:0:0:0:0:0:1]"), "mongodb://[0:0:0:0:0:0:0:1]:27017/"),
    ("mongodb://127.0.0.1,[::1]:27018,example.com:27019", new ServerAddress("example.com", 27019), "mongodb://example.com:27019/"), // scalastyle:ignore
    ("mongodb://b\\u00fccher.example.com/", new ServerAddress(), "mongodb://127.0.0.1:27017/"),
    ("mongodb://alice:foo@b\\u00fccher.example.com", new ServerAddress("other.example.com"), "mongodb://alice:foo@other.example.com:27017/"),
    ("mongodb://alice:foo@127.0.0.1/test", new ServerAddress("example.com"), "mongodb://alice:foo@example.com:27017/test"),
    ("mongodb://user%40EXAMPLE.COM:secret@localhost/?authMechanismProperties=SERVICE_NAME:other,CANONICALIZE_HOST_NAME:true&authMechanism=GSSAPI",
      new ServerAddress("example.com"),
      "mongodb://user%40EXAMPLE.COM:secret@example.com:27017/?authMechanismProperties=SERVICE_NAME:other,CANONICALIZE_HOST_NAME:true&authMechanism=GSSAPI")
  )

}
