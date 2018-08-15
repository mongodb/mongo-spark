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
    AggregationConfig().pipeline shouldBe None
    AggregationConfig().collation shouldBe None
    AggregationConfig().hint shouldBe None
    AggregationConfig().allowDiskUse shouldBe true
    AggregationConfig().asOptions shouldBe empty
  }

  it should "have the expected values" in {
    forAll(options) { (pipeline: List[BsonDocument], collation: Collation, hint: BsonDocument, allowDiskUse: Boolean) =>
      {
        val config = AggregationConfig(pipeline, collation, hint, allowDiskUse)
        config.pipeline should equal(Some(pipeline))
        config.collation should equal(Some(collation))
        config.hint should equal(Some(hint))
        config.allowDiskUse should equal(allowDiskUse)
      }
    }
  }

  it should "be creatable from SparkConfig" in {
    val configPrefix = Table("prefix", ReadConfig.configPrefix, "")
    forAll(options) { (pipeline: List[BsonDocument], collation: Collation, hint: BsonDocument, allowDiskUse: Boolean) =>
      {
        val conf = sparkConf.clone()

        forAll(configPrefix) { prefix: String =>
          conf.set(s"$prefix${ReadConfig.pipelineProperty}", pipeline.map(_.toJson).mkString("[", ",", "]"))
          conf.set(s"$prefix${ReadConfig.collationProperty}", collation.asDocument().toJson())
          conf.set(s"$prefix${ReadConfig.hintProperty}", hint.toJson)
          conf.set(s"$prefix${ReadConfig.allowDiskUseProperty}", allowDiskUse.toString)

          val config = AggregationConfig(conf)
          config.pipeline.get should equal(pipeline)
          config.collation.get should equal(collation)
          config.hint.get should equal(hint)
          config.allowDiskUse should equal(allowDiskUse)
        }
      }
    }
  }

  it should "roundtrip options" in {
    forAll(options) { (pipeline: List[BsonDocument], collation: Collation, hint: BsonDocument, allowDiskUse: Boolean) =>
      {
        val config = AggregationConfig().withOptions(AggregationConfig(pipeline, collation, hint, allowDiskUse).asOptions)
        config.pipeline.get should equal(pipeline)
        config.collation.get should equal(collation)
        config.hint.get should equal(hint)
        config.allowDiskUse should equal(allowDiskUse)
      }
    }
  }

  it should "ignore defaults or undefined collation / hints" in {
    forAll(emptyOptions) { (pipeline: Option[String], collation: Option[String], hint: Option[String], allowDiskUse: Boolean) =>
      {
        val aggregationConfig = AggregationConfig().withOptions(AggregationConfig(collation, hint, pipeline).asOptions)
        aggregationConfig.pipeline shouldBe None
        aggregationConfig.collation shouldBe None
        aggregationConfig.hint shouldBe None
        aggregationConfig.allowDiskUse shouldBe true
        aggregationConfig.asOptions shouldBe empty
      }
    }
  }

  it should "support isDefined" in {
    AggregationConfig().isDefined shouldBe false
    AggregationConfig(pipeline = List.empty[BsonDocument]).isDefined shouldBe false
    AggregationConfig(Collation.builder().locale("en").caseLevel(true).build()).isDefined shouldBe true
    AggregationConfig(hint = BsonDocument.parse("{a: 1}")).isDefined shouldBe true
    AggregationConfig(allowDiskUse = false).isDefined shouldBe true
    AggregationConfig(pipeline = List(BsonDocument.parse("{}"))).isDefined shouldBe true
  }

  it should "validate values" in {
    an[IllegalArgumentException] should be thrownBy AggregationConfig(hintString = Some("madeUpValue"))
    an[IllegalArgumentException] should be thrownBy AggregationConfig(collationString = Some("madeUpValue"))
    an[IllegalArgumentException] should be thrownBy AggregationConfig(pipelineString = Some("madeUpValue"))
  }

  val sparkConf = new SparkConf()

  val emptyOptions = Table(
    ("pipeline", "collation", "hint", "allowDiskUse"),
    (None, None, None, true),
    (Some("[]"), Some("{}"), Some("{}"), true)
  )

  val options = Table(
    ("pipeline", "collation", "hint", "allowDiskUse"),
    (List(BsonDocument.parse("{$match: {a: 1}}")), Collation.builder().collationAlternate(CollationAlternate.SHIFTED).caseLevel(true)
      .numericOrdering(true).build(), BsonDocument.parse("{a: 1}"), false),
    (List(BsonDocument.parse("{$match: {a: 1}}")), Collation.builder().locale("en").caseLevel(true).collationCaseFirst(CollationCaseFirst.OFF)
      .collationStrength(CollationStrength.IDENTICAL).numericOrdering(true).collationAlternate(CollationAlternate.SHIFTED)
      .collationMaxVariable(CollationMaxVariable.SPACE).backwards(true).normalization(true).build(), BsonDocument.parse("{a: 1, b: -1}"), false)
  )

}
