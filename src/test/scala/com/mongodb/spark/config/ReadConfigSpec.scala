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

package com.mongodb.spark.config

import org.scalatest.{FlatSpec, Matchers}

import org.apache.spark.SparkConf

import com.mongodb.{ReadConcern, ReadPreference}

class ReadConfigSpec extends FlatSpec with Matchers {

  "ReadConfig" should "have the expected defaults" in {
    val readConfig = ReadConfig("db", "collection")
    val expectedReadConfig = ReadConfig("db", "collection", "primary", None, 1000) // scalastyle:ignore

    readConfig should equal(expectedReadConfig)
  }

  it should "be creatable from SparkConfig" in {
    val expectedReadConfig = ReadConfig("db", "collection", "secondary", Some("local"), 150) // scalastyle:ignore

    ReadConfig(sparkConf) should equal(expectedReadConfig)
  }

  it should "be able mixin user parameters" in {
    val expectedReadConfig = ReadConfig("db", "collection", "secondaryPreferred", Some("majority"), 200) // scalastyle:ignore

    ReadConfig(sparkConf).withOptions(
      Map(
        "readPreference" -> "secondaryPreferred",
        "readConcernLevel" -> "majority",
        "sampleSize" -> "200"
      )
    ) should equal(expectedReadConfig)
  }

  it should "be able to create a map" in {
    val readConfig = ReadConfig("dbName", "collName", "secondaryPreferred", Some("majority"), 200) // scalastyle:ignore
    val expectedReadConfigMap = Map(
      "databaseName" -> "dbName",
      "collectionName" -> "collName",
      "readPreference" -> "secondaryPreferred",
      "readConcernLevel" -> "majority",
      "sampleSize" -> "200"
    )

    readConfig.asOptions should equal(expectedReadConfigMap)
  }

  it should "create the expected ReadPreference and ReadConcern" in {
    val readConfig = ReadConfig(sparkConf)

    readConfig.readPreference should equal(ReadPreference.secondary())
    readConfig.readConcern should equal(ReadConcern.LOCAL)
  }

  it should "validate the values" in {
    an[IllegalArgumentException] should be thrownBy ReadConfig(new SparkConf().set("mongodb.input.collectionName", "coll"))
    an[IllegalArgumentException] should be thrownBy ReadConfig(new SparkConf().set("mongodb.input.databaseName", "db"))
    an[IllegalArgumentException] should be thrownBy ReadConfig("db", "collection", sampleSize = -1)
    an[IllegalArgumentException] should be thrownBy ReadConfig("db", "collection", readPreferenceName = "allTheNodes")
    an[IllegalArgumentException] should be thrownBy ReadConfig("db", "collection", readConcernLevel = Some("allTheNodes"))
  }

  val sparkConf = new SparkConf()
    .set("mongodb.input.databaseName", "db")
    .set("mongodb.input.collectionName", "collection")
    .set("mongodb.input.readPreference", "secondary")
    .set("mongodb.input.readConcernLevel", "local")
    .set("mongodb.input.sampleSize", "150")

}

