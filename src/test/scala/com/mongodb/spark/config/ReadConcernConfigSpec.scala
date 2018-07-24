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

import com.mongodb.ReadConcern
import org.apache.spark.SparkConf
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table
import org.scalatest.{FlatSpec, Matchers}

class ReadConcernConfigSpec extends FlatSpec with Matchers {

  "ReadConcernConfig" should "have the expected defaults" in {
    ReadConcern.DEFAULT should equal(ReadConcernConfig().readConcern)
  }

  it should "have the expected readConcern" in {
    forAll(readConcerns) { readConcern: ReadConcern =>
      ReadConcernConfig(readConcern).readConcern should equal(readConcern)
    }
  }

  it should "be creatable from SparkConfig" in {
    val configPrefix = Table("prefix", ReadConfig.configPrefix, "")

    forAll(readConcerns) { readConcern: ReadConcern =>
      val conf = sparkConf.clone()

      forAll(configPrefix) { prefix: String =>
        if (Option(readConcern.getLevel).isDefined) {
          conf.set(s"$prefix${ReadConfig.readConcernLevelProperty}", readConcern.getLevel.getValue)
        }
        ReadConcernConfig(conf).readConcern should equal(readConcern)
      }
    }
  }

  it should "roundtrip options" in {
    forAll(readConcerns) { readConcern: ReadConcern =>
      ReadConcernConfig().withOptions(ReadConcernConfig(readConcern).asOptions).readConcern should equal(readConcern)
    }
  }

  it should "validate values" in {
    an[IllegalArgumentException] should be thrownBy ReadConcernConfig(readConcernLevel = Some("madeUpValue"))
  }

  val sparkConf = new SparkConf()

  val readConcerns = Table(
    "readConcerns",
    ReadConcern.DEFAULT,
    ReadConcern.LINEARIZABLE,
    ReadConcern.LOCAL,
    ReadConcern.MAJORITY,
    ReadConcern.SNAPSHOT
  )

}
