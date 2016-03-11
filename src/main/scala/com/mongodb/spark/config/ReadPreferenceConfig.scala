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

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.apache.spark.SparkConf

import org.bson.Document
import com.mongodb.{ReadPreference, Tag, TagSet, TaggableReadPreference}

/**
 * The `ReadPreferenceConfig` companion object
 *
 * @since 1.0
 */
object ReadPreferenceConfig extends MongoInputConfig {

  type Self = ReadPreferenceConfig

  /**
   * Default configuration
   *
   * @return the configuration
   */
  def create(): ReadPreferenceConfig = ReadPreferenceConfig()

  /**
   * Creates a `ReadPreferenceConfig` from a `ReadPreference` instance
   *
   * @param readPreference the read preference
   * @return the configuration
   */
  def apply(readPreference: ReadPreference): ReadPreferenceConfig = {
    val tagSets = readPreference match {
      case taggable: TaggableReadPreference =>
        val tagSetList = taggable.getTagSetList.asScala
        tagSetList.nonEmpty match {
          case true => Some(
            tagSetList.map(tagSet => tagSet.iterator().asScala.map(tag => s"""${tag.getName}:"${tag.getValue}"""").mkString("{", ",", "}"))
              .mkString("[", ",", "]")
          )
          case false => None
        }
      case readPref => None
    }
    new ReadPreferenceConfig(readPreference.getName, tagSets)
  }

  /**
   * Creates a `ReadPreferenceConfig` from a `ReadPreference` instance
   *
   * @param readPreference the read preference
   * @return the configuration
   */
  def create(readPreference: ReadPreference): ReadPreferenceConfig = apply(readPreference)

  override def apply(options: scala.collection.Map[String, String], default: Option[ReadPreferenceConfig]): ReadPreferenceConfig = {
    val cleanedOptions = prefixLessOptions(options)
    val defaultReadPreferenceConfig: ReadPreferenceConfig = default.getOrElse(
      Option(connectionString(cleanedOptions).getReadPreference) match {
        case Some(readPref) => ReadPreferenceConfig(readPref)
        case None           => ReadPreferenceConfig()
      }
    )

    val readPrefName = cleanedOptions.get(readPreferenceNameProperty)
    val readPrefTagSets = cleanedOptions.get(readPreferenceTagSetsProperty)

    // Normalize tagSets strings if they exist
    val readPreferenceConfig = readPrefTagSets.isDefined match {
      case true  => readPrefName.map(name => ReadPreferenceConfig(new ReadPreferenceConfig(name, readPrefTagSets).readPreference))
      case false => readPrefName.map(name => new ReadPreferenceConfig(name, None))
    }

    readPreferenceConfig.getOrElse(defaultReadPreferenceConfig)
  }

  override def create(sparkConf: SparkConf): ReadPreferenceConfig = apply(sparkConf)

  override def create(options: util.Map[String, String]): ReadPreferenceConfig = apply(options.asScala)

  override def create(options: util.Map[String, String], default: ReadPreferenceConfig): ReadPreferenceConfig = apply(options.asScala, Option(default))

  private def tagSets(tagSets: String): util.List[TagSet] = {
    val parsedTagSets = Try(Document.parse(s"{tagSets: $tagSets}")).map(doc => doc.get("tagSets", classOf[util.List[Document]]).asScala.map(tagSet).asJava)
    require(parsedTagSets.isSuccess, s"""Invalid tagSet, tagSets must be a Json array of documents eg: [{k1:v1,k2:v2}, {}]. '$tagSets'""")
    parsedTagSets.get
  }

  private def tagSet(tags: Document): TagSet = new TagSet(tags.asScala.map(kv => new Tag(kv._1, kv._2.toString)).toList.asJava)
}

/**
 * The `ReadPreference` configuration used by the [[ReadConfig]]
 *
 * @param name the read preference name
 * @param tagSets optional string of tagSets
 * @since 1.0
 */
case class ReadPreferenceConfig(private val name: String = "primary", private val tagSets: Option[String] = None) extends MongoSparkConfig {
  require(Try(readPreference).isSuccess, s"Invalid ReadPreferenceConfig configuration: $this")

  type Self = ReadPreferenceConfig

  override def withOptions(options: collection.Map[String, String]): Self = ReadPreferenceConfig(options, Some(this))

  override def asOptions: collection.Map[String, String] = {
    val options = Map(ReadPreferenceConfig.readPreferenceNameProperty -> name)
    tagSets match {
      case Some(tagsets) => options ++ Map(ReadPreferenceConfig.readPreferenceTagSetsProperty -> tagsets)
      case None          => options
    }
  }

  override def withJavaOptions(options: util.Map[String, String]): ReadPreferenceConfig = withOptions(options.asScala)

  override def asJavaOptions: util.Map[String, String] = asOptions.asJava

  /**
   * The `ReadPreference` that this configuration represents
   *
   * @return the ReadPreference
   */
  def readPreference: ReadPreference = {
    val tryParsingReadPreference = tagSets match {
      case Some(tagsets) => Try(ReadPreference.valueOf(name, ReadPreferenceConfig.tagSets(tagsets)))
      case None          => Try(ReadPreference.valueOf(name))
    }

    tryParsingReadPreference match {
      case Success(readPref) => readPref
      case Failure(ex)       => throw new IllegalArgumentException(s"Invalid ReadPreference configuration. $name, $tagSets", ex)
    }
  }

}
