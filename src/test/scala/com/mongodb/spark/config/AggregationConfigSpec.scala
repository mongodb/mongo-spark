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

import com.mongodb.client.model.{Collation, CollationAlternate, CollationCaseFirst, CollationMaxVariable, CollationStrength}
import org.apache.spark.SparkConf
import org.bson.BsonDocument
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table
import org.scalatest.{FlatSpec, Matchers}

class AggregationConfigSpec extends FlatSpec with Matchers {

  "AggregationConfig" should "have the expected defaults" in {
    AggregationConfig().collation shouldBe None
    AggregationConfig().hint shouldBe None
    AggregationConfig().asOptions shouldBe empty
  }

  it should "have the expected values" in {
    forAll(options) { (collation: Collation, hint: BsonDocument) =>
      {
        val config = AggregationConfig(collation, hint)
        config.collation should equal(Some(collation))
        config.hint should equal(Some(hint))
      }
    }
  }

  it should "be creatable from SparkConfig" in {
    val configPrefix = Table("prefix", ReadConfig.configPrefix, "")
    forAll(options) { (collation: Collation, hint: BsonDocument) =>
      {
        val conf = sparkConf.clone()

        forAll(configPrefix) { prefix: String =>
          conf.set(s"$prefix${ReadConfig.collationProperty}", collation.asDocument().toJson())
          conf.set(s"$prefix${ReadConfig.hintProperty}", hint.toJson)

          val aggregationConfig = AggregationConfig(conf)
          aggregationConfig.collation.get should equal(collation)
          aggregationConfig.hint.get should equal(hint)
        }
      }
    }
  }

  it should "roundtrip options" in {
    forAll(options) { (collation: Collation, hint: BsonDocument) =>
      {
        val aggregationConfig = AggregationConfig().withOptions(AggregationConfig(collation, hint).asOptions)
        aggregationConfig.collation.get should equal(collation)
        aggregationConfig.hint.get should equal(hint)
      }
    }
  }

  it should "ignore defaults or undefined collation / hints" in {
    forAll(emptyOptions) { (collation: Option[String], hint: Option[String]) =>
      {
        val aggregationConfig = AggregationConfig().withOptions(AggregationConfig(collation, hint).asOptions)
        aggregationConfig.collation shouldBe None
        aggregationConfig.hint shouldBe None
        aggregationConfig.asOptions shouldBe empty
      }
    }
  }

  it should "validate values" in {
    an[IllegalArgumentException] should be thrownBy AggregationConfig(hintString = Some("madeUpValue"))
    an[IllegalArgumentException] should be thrownBy AggregationConfig(collationString = Some("madeUpValue"))
  }

  val sparkConf = new SparkConf()

  val emptyOptions = Table(
    ("collation", "hint"),
    (None, None),
    (Some("{}"), Some("{}"))
  )

  val options = Table(
    ("collation", "hint"),
    (Collation.builder().collationAlternate(CollationAlternate.SHIFTED).caseLevel(true).numericOrdering(true).build(), BsonDocument.parse("{a: 1}")),
    (Collation.builder().locale("en").caseLevel(true).collationCaseFirst(CollationCaseFirst.OFF)
      .collationStrength(CollationStrength.IDENTICAL).numericOrdering(true).collationAlternate(CollationAlternate.SHIFTED)
      .collationMaxVariable(CollationMaxVariable.SPACE).backwards(true).normalization(true).build(), BsonDocument.parse("{a: 1, b: -1}"))
  )

}
