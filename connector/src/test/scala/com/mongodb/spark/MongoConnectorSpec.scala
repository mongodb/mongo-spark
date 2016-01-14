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

import scala.collection.JavaConverters._

import org.scalatest.FlatSpec

import com.mongodb.{MongoClientURI, ServerAddress}

class MongoConnectorSpec extends FlatSpec with RequiresMongoDB {

  "MongoConnector" should "create a MongoClient" in {
    val expectedServerAddresses = new MongoClientURI(mongoClientURI).getHosts.asScala.map(new ServerAddress(_)).asJava

    val mongoConnector = MongoConnector(mongoClientURI, "db", "coll")

    mongoConnector.getMongoClient().getServerAddressList should equal(expectedServerAddresses)
    mongoConnector.getDatabase().getName shouldBe "db"
    mongoConnector.getCollection().getNamespace.getCollectionName shouldBe "coll"
  }

}
