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

import scala.collection.JavaConverters._
import org.scalatest.{FlatSpec, Matchers}

import org.apache.spark.SparkConf

import com.mongodb.{Tag, TagSet, ReadConcern, ReadPreference}

// scalastyle:off magic.number
class ReadConfigSpec extends FlatSpec with Matchers {

  "ReadConfig" should "have the expected defaults" in {
    val readConfig = ReadConfig("db", "collection")
    val expectedReadConfig = ReadConfig("db", "collection", 1000, 64, "_id",
      ReadPreferenceConfig(ReadPreference.primary()), ReadConcernConfig(ReadConcern.DEFAULT))

    readConfig should equal(expectedReadConfig)
  }

  it should "be creatable from SparkConfig" in {
    val expectedReadConfig = ReadConfig("db", "collection", 150, 32, "ID",
      ReadPreferenceConfig(ReadPreference.secondary()), ReadConcernConfig(ReadConcern.LOCAL))

    ReadConfig(sparkConf) should equal(expectedReadConfig)
  }

  it should "use the URI for default values" in {
    val uri =
      "mongodb://localhost/db.collection?readPreference=secondaryPreferred&readPreferenceTags=dc:east,use:production&readPreferenceTags=&readconcernlevel=local"
    val readConfig = ReadConfig(Map("uri" -> uri))

    val expectedReadConfig = ReadConfig("db", "collection", 1000, 64, "_id",
      ReadPreferenceConfig(ReadPreference.secondaryPreferred(List(
        new TagSet(List(new Tag("dc", "east"), new Tag("use", "production")).asJava),
        new TagSet()
      ).asJava)),
      ReadConcernConfig(ReadConcern.LOCAL))

    readConfig should equal(expectedReadConfig)
  }

  it should "override URI values with named values" in {
    val uri =
      "mongodb://localhost/db.collection?readPreference=secondaryPreferred&readconcernlevel=local"
    val readConfig = ReadConfig(Map("uri" -> uri, "readPreference.name" -> "primaryPreferred", "readConcern.level" -> "majority"))

    val expectedReadConfig = ReadConfig("db", "collection", 1000, 64, "_id",
      ReadPreferenceConfig(ReadPreference.primaryPreferred()), ReadConcernConfig(ReadConcern.MAJORITY))

    readConfig should equal(expectedReadConfig)
  }

  it should "round trip options" in {
    val defaultReadConfig = ReadConfig(sparkConf)
    val expectedReadConfig = ReadConfig("db", "collection", 200, 20, "foo",
      ReadPreferenceConfig(ReadPreference.secondaryPreferred(new TagSet(List(new Tag("dc", "east"), new Tag("use", "production")).asJava))),
      ReadConcernConfig(ReadConcern.MAJORITY))

    defaultReadConfig.withOptions(expectedReadConfig.asOptions) should equal(expectedReadConfig)
  }

  it should "be able to create a map" in {
    val readConfig = ReadConfig("dbName", "collName", 200, 20, "foo",
      ReadPreferenceConfig(ReadPreference.secondaryPreferred(List(
        new TagSet(List(new Tag("dc", "east"), new Tag("use", "production")).asJava),
        new TagSet()
      ).asJava)),
      ReadConcernConfig(ReadConcern.MAJORITY))

    val expectedReadConfigMap = Map(
      "database" -> "dbName",
      "collection" -> "collName",
      "maxchunksize" -> "20",
      "splitkey" -> "foo",
      "readpreference.name" -> "secondaryPreferred",
      "readpreference.tagsets" -> """[{dc:"east",use:"production"},{}]""",
      "readconcern.level" -> "majority",
      "samplesize" -> "200"
    )

    readConfig.asOptions should equal(expectedReadConfigMap)
  }

  it should "create the expected ReadPreference and ReadConcern" in {
    val readConfig = ReadConfig(sparkConf)

    readConfig.readPreference should equal(ReadPreference.secondary())
    readConfig.readConcern should equal(ReadConcern.LOCAL)
  }

  it should "validate the values" in {
    an[IllegalArgumentException] should be thrownBy ReadConfig("db", "collection", sampleSize = -1)
    an[IllegalArgumentException] should be thrownBy ReadConfig(new SparkConf().set("spark.mongodb.input.collection", "coll"))
    an[IllegalArgumentException] should be thrownBy ReadConfig(new SparkConf().set("spark.mongodb.input.database", "db"))
    an[IllegalArgumentException] should be thrownBy ReadConfig(sparkConf.clone().set("spark.mongodb.input.readPreference.tagSets", "[1, 2]"))
    an[IllegalArgumentException] should be thrownBy ReadConfig(sparkConf.clone().set("spark.mongodb.input.readPreference.tagSets", "-1]"))
    an[IllegalArgumentException] should be thrownBy ReadConfig(sparkConf.clone().set("spark.mongodb.input.readConcern.level", "Alpha"))
  }

  val sparkConf = new SparkConf()
    .set("spark.mongodb.input.database", "db")
    .set("spark.mongodb.input.collection", "collection")
    .set("spark.mongodb.input.maxChunkSize", "32")
    .set("spark.mongodb.input.splitKey", "ID")
    .set("spark.mongodb.input.readPreference.name", "secondary")
    .set("spark.mongodb.input.readConcern.level", "local")
    .set("spark.mongodb.input.sampleSize", "150")

}
// scalastyle:on magic.number

