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

import java.util.concurrent.TimeUnit

import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table
import org.scalatest.{FlatSpec, Matchers}

import org.apache.spark.SparkConf

import com.mongodb.WriteConcern

class WriteConfigSpec extends FlatSpec with Matchers {

  "WriteConfig" should "have the expected defaults" in {
    val expectedWriteConfig = WriteConfig("db", "collection", None, true, 512, MongoSharedConfig.DefaultLocalThreshold,
      WriteConcern.ACKNOWLEDGED, None, false, true)

    WriteConfig("db", "collection") should equal(expectedWriteConfig)
  }

  it should "be creatable from SparkConfig" in {
    val configPrefix = Table("prefix", WriteConfig.configPrefix, "")

    forAll(writeConcerns) { writeConcern: WriteConcern =>
      val expectedWriteConfig = WriteConfig("db", "collection", None, false, 1024, MongoSharedConfig.DefaultLocalThreshold, writeConcern,
        Some("{a: 1, b:1}"), true, false)
      val conf = sparkConf.clone()

      forAll(configPrefix) { prefix: String =>
        Option(writeConcern.getWObject).map(w => conf.set(s"$prefix${WriteConfig.writeConcernWProperty}", w.toString))
        Option(writeConcern.getJournal).map(j => conf.set(s"$prefix${WriteConfig.writeConcernJournalProperty}", j.toString))
        Option(writeConcern.getWTimeout(TimeUnit.MILLISECONDS)).map(t =>
          conf.set(s"$prefix${WriteConfig.writeConcernWTimeoutMSProperty}", t.toString))
        conf.set(s"$prefix${WriteConfig.replaceDocumentProperty}", "false")
        conf.set(s"$prefix${WriteConfig.maxBatchSizeProperty}", "1024")
        conf.set(s"$prefix${WriteConfig.shardKeyProperty}", "{a: 1, b:1}")
        conf.set(s"$prefix${WriteConfig.forceInsertProperty}", "true")
        conf.set(s"$prefix${WriteConfig.orderedProperty}", "false")

        WriteConfig(conf) should equal(expectedWriteConfig)
      }
    }
  }

  it should "round trip options" in {
    val uri = Some("mongodb://localhost/")
    val replaceDocument = false
    val localThreshold = 5
    val maxBatchSize = 1024
    val shardKey = Some("{a: 1}")
    val forceInsert = true
    val ordered = false
    val defaultWriteConfig = WriteConfig("dbName", "collName", uri, replaceDocument, maxBatchSize, localThreshold,
      WriteConcern.ACKNOWLEDGED, shardKey, forceInsert, ordered)
    forAll(writeConcerns) { writeConcern: WriteConcern =>
      val expectedWriteConfig = WriteConfig("db", "collection", uri, replaceDocument, maxBatchSize, localThreshold, writeConcern, shardKey,
        forceInsert, ordered)
      defaultWriteConfig.withOptions(expectedWriteConfig.asOptions) should equal(expectedWriteConfig)
    }
  }

  it should "validate the values" in {
    an[IllegalArgumentException] should be thrownBy WriteConfig("db", "collection", Some("localhost/db.coll"))
    an[IllegalArgumentException] should be thrownBy WriteConfig(new SparkConf().set("spark.mongodb.output.uri", "localhost/db.coll"))
    an[IllegalArgumentException] should be thrownBy WriteConfig(new SparkConf().set(
      "spark.mongodb.output.uri",
      "mongodb://localhost/db.coll/?readPreference=AllNodes"
    ))
    an[IllegalArgumentException] should be thrownBy WriteConfig(new SparkConf().set("spark.mongodb.output.collection", "coll"))
    an[IllegalArgumentException] should be thrownBy WriteConfig(new SparkConf().set("spark.mongodb.output.database", "db"))
    an[IllegalArgumentException] should be thrownBy WriteConfig(sparkConf.clone().set("spark.mongodb.output.maxBatchSize", "-1"))
    an[IllegalArgumentException] should be thrownBy WriteConfig(sparkConf.clone().set("spark.mongodb.output.localThreshold", "-1"))
    an[IllegalArgumentException] should be thrownBy WriteConfig(sparkConf.clone().set("spark.mongodb.output.writeConcern.w", "-1"))
    an[IllegalArgumentException] should be thrownBy WriteConfig(sparkConf.clone().set("spark.mongodb.output.writeConcern.wTimeoutMS", "-1"))
    an[IllegalArgumentException] should be thrownBy WriteConfig(sparkConf.clone().set("spark.mongodb.output.shardKey", "_id"))
  }

  val sparkConf = new SparkConf()
    .set("spark.mongodb.output.database", "db")
    .set("spark.mongodb.output.collection", "collection")

  val writeConcerns = Table(
    "writeConcern",
    WriteConcern.UNACKNOWLEDGED,
    WriteConcern.ACKNOWLEDGED,
    WriteConcern.W1,
    WriteConcern.MAJORITY,
    WriteConcern.W1.withJournal(true),
    WriteConcern.W1.withJournal(true).withWTimeout(1, TimeUnit.MINUTES)
  )
}
