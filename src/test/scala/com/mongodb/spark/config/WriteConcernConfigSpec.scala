/*
 * Copyright 2008-present MongoDB, Inc.
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

import com.mongodb.WriteConcern
import org.apache.spark.SparkConf
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table
import org.scalatest.{FlatSpec, Matchers}

class WriteConcernConfigSpec extends FlatSpec with Matchers {

  "WriteConcernConfig" should "have the expected defaults" in {
    WriteConcern.ACKNOWLEDGED should equal(WriteConcernConfig().writeConcern)
  }

  it should "have the expected writeConcern" in {
    forAll(writeConcerns) { writeConcern: WriteConcern =>
      WriteConcernConfig(writeConcern).writeConcern should equal(writeConcern)
    }
  }

  it should "be creatable from SparkConfig" in {
    val configPrefix = Table("prefix", WriteConfig.configPrefix, "")

    forAll(writeConcerns) { writeConcern: WriteConcern =>
      val conf = sparkConf.clone()

      forAll(configPrefix) { prefix: String =>
        val doc = writeConcern.asDocument
        if (doc.containsKey("w")) {
          val w = doc.get("w")
          if (w.isNumber) {
            conf.set(s"$prefix${WriteConfig.writeConcernWProperty}", w.asNumber().intValue().toString)
          } else {
            conf.set(s"$prefix${WriteConfig.writeConcernWProperty}", w.asString().getValue)
          }
        }

        if (doc.containsKey("j")) {
          conf.set(s"$prefix${WriteConfig.writeConcernJournalProperty}", doc.getBoolean("j").getValue.toString)
        }

        if (doc.containsKey("wtimeout")) {
          conf.set(s"$prefix${WriteConfig.writeConcernWTimeoutMSProperty}", doc.getNumber("wtimeout").longValue().toString)
        }
      }
    }
  }

  it should "roundtrip options" in {
    forAll(writeConcerns) { writeConcern: WriteConcern =>
      WriteConcernConfig().withOptions(WriteConcernConfig(writeConcern).asOptions).writeConcern should equal(writeConcern)
    }
  }

  it should "validate values" in {
    an[IllegalArgumentException] should be thrownBy WriteConcernConfig(w = Some(-1))
    an[IllegalArgumentException] should be thrownBy WriteConcernConfig(w = Some(1), wName = Some("named"))
  }

  val sparkConf = new SparkConf()

  val writeConcerns = Table(
    "writeConcerns",
    WriteConcern.ACKNOWLEDGED,
    WriteConcern.JOURNALED,
    WriteConcern.UNACKNOWLEDGED,
    WriteConcern.W1,
    WriteConcern.W2,
    WriteConcern.W3,
    WriteConcern.ACKNOWLEDGED.withW("named"),
    WriteConcern.ACKNOWLEDGED.withJournal(true).withWTimeout(1, TimeUnit.MILLISECONDS)
  )

}
