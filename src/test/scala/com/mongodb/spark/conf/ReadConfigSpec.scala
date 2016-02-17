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

package com.mongodb.spark.conf

import org.scalatest.{FlatSpec, Matchers}

import org.apache.spark.SparkConf

import com.mongodb.{ReadPreference, ReadConcern}

class ReadConfigSpec extends FlatSpec with Matchers {

  "ReadConfig" should "have the expected defaults" in {
    val readConfig = ReadConfig("db", "collection")
    val expectedReadConfig = ReadConfig("db", "collection", 64, "_id", "primary", None, 1000) // scalastyle:ignore

    readConfig should equal(expectedReadConfig)
  }

  it should "be creatable from SparkConfig" in {
    val expectedReadConfig = ReadConfig("db", "collection", 1, "_id", "secondary", Some("local"), 150) // scalastyle:ignore

    ReadConfig(sparkConf) should equal(expectedReadConfig)
  }

  it should "be able mixin user parameters" in {
    val expectedReadConfig = ReadConfig("db", "collection", 10, "ID", "secondaryPreferred", Some("majority"), 200) // scalastyle:ignore

    ReadConfig(sparkConf).withOptions(
      Map(
        "maxChunkSize" -> "10",
        "splitKey" -> "ID",
        "readPreference" -> "secondaryPreferred",
        "readConcernLevel" -> "majority",
        "sampleSize" -> "200"
      )
    ) should equal(expectedReadConfig)
  }

  it should "create the expected ReadPreference and ReadConcern" in {
    val readConfig = ReadConfig(sparkConf)

    readConfig.readPreference should equal(ReadPreference.secondary())
    readConfig.readConcern should equal(ReadConcern.LOCAL)
  }

  it should "validate the values" in {
    an[IllegalArgumentException] should be thrownBy ReadConfig(new SparkConf().set("mongodb.input.collectionName", "coll"))
    an[IllegalArgumentException] should be thrownBy ReadConfig(new SparkConf().set("mongodb.input.databaseName", "db"))
    an[IllegalArgumentException] should be thrownBy ReadConfig("db", "collection", maxChunkSize = -1)
    an[IllegalArgumentException] should be thrownBy ReadConfig("db", "collection", sampleSize = -1)
    an[IllegalArgumentException] should be thrownBy ReadConfig("db", "collection", readPreferenceName = "allTheNodes")
    an[IllegalArgumentException] should be thrownBy ReadConfig("db", "collection", readConcernLevel = Some("allTheNodes"))
  }

  val sparkConf = new SparkConf()
    .set("mongodb.input.databaseName", "db")
    .set("mongodb.input.collectionName", "collection")
    .set("mongodb.input.maxChunkSize", "1")
    .set("mongodb.input.splitKey", "_id")
    .set("mongodb.input.readPreference", "secondary")
    .set("mongo.input.readConcernLevel", "local")
    .set("mongo.input.sampleSize", "150")

}

