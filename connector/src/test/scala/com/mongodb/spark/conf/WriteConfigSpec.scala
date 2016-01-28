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

import org.scalatest.{Matchers, FlatSpec}

import org.apache.spark.SparkConf

import com.mongodb.WriteConcern

class WriteConfigSpec extends FlatSpec with Matchers {

  "WriteConfig" should "have the expected defaults" in {
    val expectedWriteConfig = WriteConfig("db", "collection", WriteConcern.ACKNOWLEDGED)

    WriteConfig("db", "collection") should equal(expectedWriteConfig)
  }

  it should "be creatable from SparkConfig" in {
    val expectedWriteConfig = WriteConfig("db", "collection", WriteConcern.MAJORITY)

    WriteConfig(sparkConf) should equal(expectedWriteConfig)
  }

  it should "validate the values" in {
    an[IllegalArgumentException] should be thrownBy WriteConfig(sparkConf.clone().remove("mongodb.output.databaseName"))
    an[IllegalArgumentException] should be thrownBy WriteConfig(sparkConf.clone().remove("mongodb.output.collectionName"))
    an[IllegalArgumentException] should be thrownBy WriteConfig(sparkConf.set("mongodb.output.writeConcern", "allTheNodes"))
  }

  val sparkConf = new SparkConf()
    .set("mongodb.output.databaseName", "db")
    .set("mongodb.output.collectionName", "collection")
    .set("mongodb.output.writeConcern", "majority")
}
