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

import java.util
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.util.Try

import org.apache.spark.SparkConf

import com.mongodb.WriteConcern
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

import com.mongodb.spark.notNull

/**
 * The `WriteConcernConfig` companion object
 *
 * @since 1.0
 */
object WriteConcernConfig extends MongoOutputConfig {

  type Self = WriteConcernConfig

  /**
   * The default configuration
   */
  val Default = WriteConcernConfig()

  /**
   * Creates a `WriteConcernConfig` from a `WriteConcern` instance
   *
   * @param writeConcern the write concern
   * @return the configuration
   */
  def apply(writeConcern: WriteConcern): WriteConcernConfig = {
    val (wOption: Option[Int], wNameOption: Option[String]) = writeConcern.getWObject.asInstanceOf[Any] match {
      case wInt: Int    => (Some(wInt), None)
      case wStr: String => (None, Some(wStr))
      case _            => (None, None)
    }
    val journalOption = Option(writeConcern.getJournal) match {
      case Some(journal) => Some(journal.booleanValue())
      case None          => None
    }
    val wTimeoutOption = Option(writeConcern.getWTimeout(TimeUnit.MILLISECONDS)).map(ms => Duration(ms.toInt, TimeUnit.MILLISECONDS))
    new WriteConcernConfig(wOption, wNameOption, journalOption, wTimeoutOption)
  }

  override def apply(options: collection.Map[String, String], default: Option[WriteConcernConfig]): WriteConcernConfig = {
    val cleanedOptions = stripPrefix(options)

    val defaultWriteConcernConfig: WriteConcernConfig = Option(connectionString(cleanedOptions).getWriteConcern) match {
      case Some(writeCon) => WriteConcernConfig(writeCon)
      case None           => default.getOrElse(Default)
    }

    val (wOption: Option[Int], wStringOption: Option[String]) = cleanedOptions.get(writeConcernWProperty) match {
      case Some(wValue) => Try(wValue.toInt).map(wInt => (Some(wInt), None)).getOrElse((None, Some(wValue)))
      case None         => (defaultWriteConcernConfig.w, defaultWriteConcernConfig.wName)
    }

    val journalOption = cleanedOptions.get(writeConcernJournalProperty) match {
      case Some(journal) => Some(journal.toBoolean)
      case None          => defaultWriteConcernConfig.journal
    }

    val wTimeoutOption = cleanedOptions.get(writeConcernWTimeoutMSProperty) match {
      case Some(wTimeout) => Some(Duration(wTimeout.toInt, TimeUnit.MILLISECONDS))
      case None           => defaultWriteConcernConfig.wTimeout
    }

    new WriteConcernConfig(wOption, wStringOption, journalOption, wTimeoutOption)
  }

  /**
   * Creates a `WriteConcernConfig` from a `WriteConcern` instance
   *
   * @param writeConcern the write concern
   * @return the configuration
   */
  def create(writeConcern: WriteConcern): WriteConcernConfig = apply(writeConcern)

  override def create(javaSparkContext: JavaSparkContext): WriteConcernConfig = {
    notNull("javaSparkContext", javaSparkContext)
    apply(javaSparkContext.getConf)
  }

  override def create(sparkConf: SparkConf): WriteConcernConfig = {
    notNull("sparkConf", sparkConf)
    apply(sparkConf)
  }

  override def create(options: util.Map[String, String]): WriteConcernConfig = {
    notNull("options", options)
    apply(options.asScala)
  }

  override def create(options: util.Map[String, String], default: WriteConcernConfig): WriteConcernConfig = {
    notNull("options", options)
    notNull("default", default)
    apply(options.asScala, Option(default))
  }

  override def create(sparkConf: SparkConf, options: util.Map[String, String]): WriteConcernConfig = {
    notNull("sparkConf", sparkConf)
    notNull("options", options)
    apply(sparkConf, options.asScala)
  }

  @deprecated("As of Spark 2.0 SQLContext was replaced by SparkSession. Use the SparkSession method instead", "2.0.0")
  override def create(sqlContext: SQLContext): WriteConcernConfig = {
    notNull("sqlContext", sqlContext)
    create(sqlContext.sparkSession)
  }

  override def create(sparkSession: SparkSession): WriteConcernConfig = {
    notNull("sparkSession", sparkSession)
    apply(sparkSession)
  }
}

/**
 * The `WriteConcern` configuration used by the [[WriteConfig]]
 *
 * @param w the optional w integer value
 * @param wName the optional w string value
 * @param journal the optional journal value
 * @param wTimeout the optional timeout value
 * @since 1.0
 */
case class WriteConcernConfig(private val w: Option[Int] = None, private val wName: Option[String] = None,
                              private val journal:  Option[Boolean]  = None,
                              private val wTimeout: Option[Duration] = None) extends MongoClassConfig {
  require(!(w.isDefined && wName.isDefined), s"Invalid WriteConcernConfig configuration - cannot have both w and wName defined: $this")
  require(Try(writeConcern).isSuccess, s"Invalid WriteConcernConfig configuration: $this")

  type Self = WriteConcernConfig

  override def withOption(key: String, value: String): WriteConcernConfig = WriteConcernConfig(this.asOptions + (key -> value))

  override def withOptions(options: collection.Map[String, String]): WriteConcernConfig = WriteConcernConfig(options, Some(this))

  override def asOptions: collection.Map[String, String] = {
    val options: mutable.Map[String, String] = mutable.Map()
    w.map(w => options += WriteConcernConfig.writeConcernWProperty -> w.toString)
    wName.map(w => options += WriteConcernConfig.writeConcernWProperty -> w)
    wTimeout.map(w => options += WriteConcernConfig.writeConcernWTimeoutMSProperty -> w.toMillis.toString)
    journal.map(j => options += WriteConcernConfig.writeConcernJournalProperty -> j.toString)
    options.toMap
  }

  override def withOptions(options: util.Map[String, String]): WriteConcernConfig = withOptions(options.asScala)

  override def asJavaOptions: util.Map[String, String] = asOptions.asJava

  /**
   * The `WriteConcern` that this config represents
   *
   * @return the WriteConcern
   */
  def writeConcern: WriteConcern = {
    var writeConcern = WriteConcern.ACKNOWLEDGED
    writeConcern = w.map(i => writeConcern.withW(i)).getOrElse(writeConcern)
    writeConcern = wName.map(n => writeConcern.withW(n)).getOrElse(writeConcern)
    writeConcern = journal.map(j => writeConcern.withJournal(j)).getOrElse(writeConcern)
    writeConcern = wTimeout.map(d => writeConcern.withWTimeout(d.toMillis, TimeUnit.MILLISECONDS)).getOrElse(writeConcern)
    writeConcern
  }
}
