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

import scala.util.{Success, Try}

import org.apache.spark.SparkContext

import org.bson.Document
import com.mongodb.MongoClientURI
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.rdd.MongoRDD

class AuthConnectionSpec extends RequiresMongoDB {

  "MongoRDD" should "be able to connect to an authenticated db" in {
    val sc = new SparkContext(sparkConf)
    val dbName = Option(new MongoClientURI(mongoClientURI).getDatabase).getOrElse(databaseName)
    val readConfig = ReadConfig(sc.getConf, Map("database" -> dbName))
    val mongoRDD: MongoRDD[Document] = MongoSpark.builder().sparkContext(sc).readConfig(readConfig).build().toRDD()

    Try(mongoRDD.count()) shouldBe a[Success[_]]
    sc.stop()
  }

  override def beforeEach() {
  }

  override def beforeAll() {
  }

  override def afterAll() {
    logInfo(s"Ended Test: '$suiteName'")
  }

}
