/*
 * Copyright 2018 MongoDB, Inc.
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

import java.util

import com.mongodb.client.model.{Collation, CollationAlternate, CollationCaseFirst, CollationMaxVariable, CollationStrength}
import com.mongodb.spark.notNull
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.bson.{BsonDocument, BsonValue}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/**
 * The `AggregationConfig` companion object.
 *
 * @since 2.3
 */
object AggregationConfig extends MongoInputConfig {
  private[spark] val DEFAULT_HINT = new BsonDocument()
  private[spark] val DEFAULT_COLLATION = Collation.builder().build()
  private[spark] val DEFAULT_ALLOW_DISK_USE = true

  override type Self = AggregationConfig

  /**
   * Creates a new Aggregation Config
   *
   * @param collation the collation to use
   * @return the Aggregation config
   */
  def apply(collation: Collation): AggregationConfig = apply(collation, DEFAULT_HINT)

  /**
   * Creates a new Aggregation Config
   *
   * @param hint the hint to use
   * @return the Aggregation config
   */
  def apply(hint: BsonDocument): AggregationConfig = apply(DEFAULT_COLLATION, hint)

  /**
   * Creates a new Aggregation Config
   *
   * @param collation the collation to use
   * @param hint the hint to use
   * @return the Aggregation config
   */
  def apply(collation: Collation, hint: BsonDocument): AggregationConfig = {
    apply(collation, hint, DEFAULT_ALLOW_DISK_USE)
  }

  /**
   * Creates a new Aggregation Config
   *
   * @param collation the collation to use
   * @param hint the hint to use
   * @return the Aggregation config
   *
   * @since 2.3.1
   */
  def apply(collation: Collation, hint: BsonDocument, allowDiskUse: Boolean): AggregationConfig = {
    val collationOption = if (DEFAULT_COLLATION.equals(collation)) None else Some(collation.asDocument().toJson)
    val hintOption = if (DEFAULT_HINT.equals(hint)) None else Some(hint.toJson())

    new AggregationConfig(collationOption, hintOption, allowDiskUse)
  }

  override def apply(options: collection.Map[String, String], default: Option[AggregationConfig]): AggregationConfig = {
    val aggregationConfig = Try({
      val cleanedOptions = stripPrefix(options)

      val defaultAggregationConfig: AggregationConfig = default.getOrElse(AggregationConfig())
      val collationString = cleanedOptions.get(collationProperty).orElse(defaultAggregationConfig.collationString)
      val hintString = cleanedOptions.get(hintProperty).orElse(defaultAggregationConfig.hintString)
      val allowDiskUse = getBoolean(cleanedOptions.get(allowDiskUseProperty), default.map(conf => conf.allowDiskUse), DEFAULT_ALLOW_DISK_USE)

      new AggregationConfig(collationString, hintString, allowDiskUse)
    })

    require(aggregationConfig.isSuccess, s"Invalid Aggregation map $options:%n${aggregationConfig.failed.get}")
    aggregationConfig.get
  }
  /**
   * Creates a new Aggregation Config
   *
   * @param collation the collation to use
   * @return the Aggregation config
   */
  def create(collation: Collation): AggregationConfig = {
    notNull("collation", collation)
    apply(collation)
  }

  /**
   * Creates a new Aggregation Config
   *
   * @param hint the hint to use
   * @return the Aggregation config
   */
  def create(hint: BsonDocument): AggregationConfig = {
    notNull("hint", hint)
    apply(hint)
  }

  /**
   * Creates a new Aggregation Config
   *
   * @param collation the collation to use
   * @param hint the hint to use
   * @return the Aggregation config
   */
  def create(collation: Collation, hint: BsonDocument): AggregationConfig = {
    notNull("collation", collation)
    notNull("hint", hint)
    apply(collation, hint)
  }

  /**
   * Creates a new Aggregation Config
   *
   * @param collation the collation to use
   * @param hint the hint to use
   * @param allowDiskUse allow disk use
   * @return the Aggregation config
   *
   * @since 2.3.1
   */
  def create(collation: Collation, hint: BsonDocument, allowDiskUse: Boolean): AggregationConfig = {
    notNull("collation", collation)
    notNull("hint", hint)
    apply(collation, hint, allowDiskUse)
  }

  override def create(sparkConf: SparkConf): AggregationConfig = {
    notNull("sparkConf", sparkConf)
    apply(sparkConf)
  }

  override def create(options: util.Map[String, String]): AggregationConfig = {
    notNull("options", options)
    apply(options.asScala)
  }

  override def create(options: util.Map[String, String], default: AggregationConfig): AggregationConfig = {
    notNull("options", options)
    notNull("default", default)
    apply(options.asScala, Some(default))
  }

  override def create(javaSparkContext: JavaSparkContext): AggregationConfig = {
    notNull("javaSparkContext", javaSparkContext)
    apply(javaSparkContext.getConf)
  }

  override def create(sparkConf: SparkConf, options: util.Map[String, String]): AggregationConfig = {
    notNull("sparkConf", sparkConf)
    notNull("options", options)
    apply(sparkConf, options.asScala)
  }

  @deprecated("As of Spark 2.0 SQLContext was replaced by SparkSession. Use the SparkSession method instead", "2.0.0")
  override def create(sqlContext: SQLContext): AggregationConfig = {
    notNull("sqlContext", sqlContext)
    create(sqlContext.sparkSession)
  }

  override def create(sparkSession: SparkSession): AggregationConfig = {
    notNull("sparkSession", sparkSession)
    apply(sparkSession)
  }
}

/**
 * The aggregation configuration
 *
 * @param collationString the optional collation config
 * @param hintString the optional hint document in extended json format
 * @param allowDiskUse enables writing to temporary files
 */
case class AggregationConfig(
  private val collationString: Option[String] = None,
  private val hintString:      Option[String] = None,
  allowDiskUse:                Boolean        = AggregationConfig.DEFAULT_ALLOW_DISK_USE
) extends MongoClassConfig {
  require(Try(hint).isSuccess, s"Invalid hint bson document")
  require(Try(collation).isSuccess, s"Invalid collation bson document")

  type Self = AggregationConfig

  override def asOptions: collection.Map[String, String] = {
    val options: mutable.Map[String, String] = mutable.Map()
    collation.map(c => if (!c.equals(AggregationConfig.DEFAULT_COLLATION)) options += AggregationConfig.collationProperty -> collationString.get)
    hint.map(h => if (!h.equals(AggregationConfig.DEFAULT_HINT)) options += AggregationConfig.hintProperty -> hintString.get)
    if (allowDiskUse != AggregationConfig.DEFAULT_ALLOW_DISK_USE) options += AggregationConfig.allowDiskUseProperty -> allowDiskUse.toString
    options.toMap
  }

  override def withOption(key: String, value: String): AggregationConfig = AggregationConfig(this.asOptions + (key -> value))

  override def withOptions(options: collection.Map[String, String]): AggregationConfig = AggregationConfig(options, Some(this))

  override def withOptions(options: util.Map[String, String]): AggregationConfig = withOptions(options.asScala)

  override def asJavaOptions: util.Map[String, String] = asOptions.asJava

  /**
   * The optional hint document
   *
   * @return the bson hint document option
   */
  @transient lazy val hint: Option[BsonDocument] = hintString.map(BsonDocument.parse)

  /**
   * @return the collation option
   */
  @transient lazy val collation: Option[Collation] = collationString.map(createCollation)

  /**
   * @return true if custom aggregation options have been defined.
   */
  def isDefined: Boolean = collationString.isDefined || hintString.isDefined || !allowDiskUse

  // scalastyle:off cyclomatic.complexity
  private def createCollation(collationString: String): Collation = {
    val collation = Try({
      val collationDocument = BsonDocument.parse(collationString)
      val builder = Collation.builder()
      collationDocument.entrySet().asScala.map({ entry: util.Map.Entry[String, BsonValue] =>
        {
          val value = entry.getValue
          entry.getKey match {
            case "alternate"       => builder.collationAlternate(CollationAlternate.fromString(value.asString.getValue))
            case "backwards"       => builder.backwards(value.asBoolean.getValue)
            case "caseFirst"       => builder.collationCaseFirst(CollationCaseFirst.fromString(value.asString.getValue))
            case "caseLevel"       => builder.caseLevel(value.asBoolean().getValue)
            case "locale"          => builder.locale(value.asString.getValue)
            case "maxVariable"     => builder.collationMaxVariable(CollationMaxVariable.fromString(value.asString.getValue))
            case "normalization"   => builder.normalization(value.asBoolean.getValue)
            case "numericOrdering" => builder.numericOrdering(value.asBoolean.getValue)
            case "strength"        => builder.collationStrength(CollationStrength.fromInt(value.asNumber.intValue()))
            case k: String         => throw new IllegalArgumentException(s"Unexpected collation key: $k")
          }
        }
      })
      builder.build()
    })
    require(collation.isSuccess, s"Invalid Collation map $collationString:%n${collation.failed.get}")
    collation.get
  }
  // scalastyle:on cyclomatic.complexity

}
