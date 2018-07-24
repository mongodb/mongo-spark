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

import com.mongodb.{ReadPreference, Tag, TagSet}
import org.apache.spark.SparkConf
import org.bson.BsonDocument
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table
import org.scalatest.{FlatSpec, Matchers}

class ReadPreferenceConfigSpec extends FlatSpec with Matchers {

  "ReadPreferenceConfig" should "have the expected defaults" in {
    ReadPreference.primary() should equal(ReadPreferenceConfig().readPreference)
  }

  it should "have the expected readPreference" in {
    forAll(readPreferences) { readPreference: ReadPreference =>
      ReadPreferenceConfig(readPreference).readPreference should equal(readPreference)
    }
  }

  it should "be creatable from SparkConfig" in {
    val configPrefix = Table("prefix", ReadConfig.configPrefix, "")

    forAll(readPreferences) { readPreference: ReadPreference =>
      val conf = sparkConf.clone()

      forAll(configPrefix) { prefix: String =>
        val doc = readPreference.toDocument
        conf.set(s"$prefix${ReadConfig.readPreferenceNameProperty}", readPreference.getName)
        if (doc.containsKey("tags")) {
          val tags = doc.getArray("tags").toArray().map(_.asInstanceOf[BsonDocument].toJson).mkString("[", ",", "]")
          conf.set(s"$prefix${ReadConfig.readPreferenceTagSetsProperty}", tags)
        }

        ReadPreferenceConfig(conf).readPreference should equal(readPreference)
      }
    }
  }

  it should "roundtrip options" in {
    forAll(readPreferences) { readPreference: ReadPreference =>
      ReadPreferenceConfig().withOptions(ReadPreferenceConfig(readPreference).asOptions).readPreference should equal(readPreference)
    }
  }

  it should "validate values" in {
    an[IllegalArgumentException] should be thrownBy ReadPreferenceConfig(name = "madeUpValue")
    an[IllegalArgumentException] should be thrownBy ReadPreferenceConfig(tagSets = Some("madeUpValue"))
  }

  val sparkConf = new SparkConf()

  val readPreferences = Table(
    "readPreferences",
    ReadPreference.primary(),
    ReadPreference.secondary(),
    ReadPreference.nearest(),
    ReadPreference.nearest(new TagSet(new Tag("a", "b")))
  )

}
