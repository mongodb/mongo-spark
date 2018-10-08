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

import com.mongodb.client.model.{Collation, CollationAlternate, CollationCaseFirst, CollationMaxVariable, CollationStrength}

import scala.collection.JavaConverters._
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.SparkConf
import com.mongodb.spark.rdd.partitioner.{DefaultMongoPartitioner, MongoShardedPartitioner, MongoSplitVectorPartitioner}
import com.mongodb.{ReadConcern, ReadPreference, Tag, TagSet}
import org.bson.BsonDocument

// scalastyle:off magic.number
class ReadConfigSpec extends FlatSpec with Matchers {

  "ReadConfig" should "have the expected defaults" in {
    val readConfig = ReadConfig("db", "collection")
    val expectedReadConfig = ReadConfig("db", "collection", None, 1000, DefaultMongoPartitioner, Map.empty[String, String], 15,
      ReadPreferenceConfig(ReadPreference.primary()), ReadConcernConfig(ReadConcern.DEFAULT), AggregationConfig())

    readConfig should equal(expectedReadConfig)
  }

  it should "be creatable from SparkConfig" in {
    val collation = Collation.builder().locale("en").caseLevel(true).collationCaseFirst(CollationCaseFirst.OFF)
      .collationStrength(CollationStrength.IDENTICAL).numericOrdering(true).collationAlternate(CollationAlternate.SHIFTED)
      .collationMaxVariable(CollationMaxVariable.SPACE).backwards(true).normalization(true).build()
    val hint = BsonDocument.parse("{a: 1, b: -1}")
    val expectedReadConfig = ReadConfig("db", "collection", None, 150, MongoShardedPartitioner, Map("shardkey" -> "ID"), 0,
      ReadPreferenceConfig(ReadPreference.secondary()), ReadConcernConfig(ReadConcern.LOCAL),
      AggregationConfig(List(BsonDocument.parse("""{ "$match" : { "a" : 1 } }""")), collation, hint), samplePoolSize = 1500)

    val readConfig = ReadConfig(sparkConf)
    readConfig.databaseName should equal(expectedReadConfig.databaseName)
    readConfig.collectionName should equal(expectedReadConfig.collectionName)
    readConfig.connectionString should equal(expectedReadConfig.connectionString)
    readConfig.sampleSize should equal(expectedReadConfig.sampleSize)
    readConfig.partitioner.getClass should equal(expectedReadConfig.partitioner.getClass)
    readConfig.partitionerOptions should equal(expectedReadConfig.partitionerOptions)
    readConfig.localThreshold should equal(expectedReadConfig.localThreshold)
    readConfig.readPreferenceConfig should equal(expectedReadConfig.readPreferenceConfig)
    readConfig.readConcernConfig should equal(expectedReadConfig.readConcernConfig)
    readConfig.inferSchemaMapTypesMinimumKeys should equal(expectedReadConfig.inferSchemaMapTypesMinimumKeys)
    readConfig.inferSchemaMapTypesEnabled should equal(expectedReadConfig.inferSchemaMapTypesEnabled)
    readConfig.pipelineIncludeNullFilters should equal(expectedReadConfig.pipelineIncludeNullFilters)
    readConfig.pipelineIncludeFiltersAndProjections should equal(expectedReadConfig.pipelineIncludeFiltersAndProjections)
    readConfig.aggregationConfig should equal(expectedReadConfig.aggregationConfig)
  }

  it should "use the URI for default values" in {
    val uri =
      "mongodb://localhost/db.collection?readPreference=secondaryPreferred&readPreferenceTags=dc:east,use:production&readPreferenceTags=&readconcernlevel=local"
    val readConfig = ReadConfig(Map("uri" -> uri))

    val expectedReadConfig = ReadConfig("db", "collection", Some(uri), 1000, DefaultMongoPartitioner, Map.empty[String, String], 15,
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

    val expectedReadConfig = ReadConfig("db", "collection", Some(uri), 1000, DefaultMongoPartitioner, Map.empty[String, String], 15,
      ReadPreferenceConfig(ReadPreference.primaryPreferred()), ReadConcernConfig(ReadConcern.MAJORITY))

    readConfig should equal(expectedReadConfig)
  }

  it should "round trip options" in {
    val defaultReadConfig = ReadConfig(sparkConf.remove("spark.mongodb.input.partitionerOptions.shardKey"))
    val expectedReadConfig = ReadConfig("db", "collection", Some("mongodb://localhost/"), 200, MongoSplitVectorPartitioner,
      Map("partitioneroptions.partitionsizemb" -> "15"), 0,
      ReadPreferenceConfig(ReadPreference.secondaryPreferred(new TagSet(List(new Tag("dc", "east"), new Tag("use", "production")).asJava))),
      ReadConcernConfig(ReadConcern.MAJORITY), AggregationConfig(
        List(BsonDocument.parse("""{ "$match" : { "a" : 1 } }""")),
        Collation.builder().locale("en").build(), BsonDocument.parse("{a : 1 }")
      ),
      inferSchemaMapTypesEnabled = false,
      inferSchemaMapTypesMinimumKeys = 999,
      pipelineIncludeNullFilters = false,
      pipelineIncludeFiltersAndProjections = false)

    val readConfig = defaultReadConfig.withOptions(expectedReadConfig.asOptions)

    readConfig.databaseName should equal(expectedReadConfig.databaseName)
    readConfig.collectionName should equal(expectedReadConfig.collectionName)
    readConfig.connectionString should equal(expectedReadConfig.connectionString)
    readConfig.sampleSize should equal(expectedReadConfig.sampleSize)
    readConfig.partitioner should equal(expectedReadConfig.partitioner)
    readConfig.partitionerOptions should equal(expectedReadConfig.partitionerOptions)
    readConfig.localThreshold should equal(expectedReadConfig.localThreshold)
    readConfig.readPreferenceConfig should equal(expectedReadConfig.readPreferenceConfig)
    readConfig.readConcernConfig should equal(expectedReadConfig.readConcernConfig)
    readConfig.aggregationConfig should equal(expectedReadConfig.aggregationConfig)
    readConfig.inferSchemaMapTypesMinimumKeys should equal(expectedReadConfig.inferSchemaMapTypesMinimumKeys)
    readConfig.inferSchemaMapTypesEnabled should equal(expectedReadConfig.inferSchemaMapTypesEnabled)
    readConfig.pipelineIncludeNullFilters should equal(expectedReadConfig.pipelineIncludeNullFilters)
    readConfig.pipelineIncludeFiltersAndProjections should equal(expectedReadConfig.pipelineIncludeFiltersAndProjections)
  }

  it should "set new options when using withOptions" in {
    val defaultReadConfig = ReadConfig(Map("uri" -> "mongodb://localhost/db.coll"))
    val options = Map(
      "collation" -> """{ "locale" : "en" }""",
      "collection" -> "collName",
      "database" -> "dbName",
      "hint" -> """{ "a" : 1 }""",
      "localthreshold" -> "99",
      "partitioner" -> "com.mongodb.spark.rdd.partitioner.MongoSplitVectorPartitioner$",
      "partitioneroptions.partitionsizemb" -> "15",
      "pipeline" -> """[{ "$match" : { "a" : 1 } }]""",
      "readconcern.level" -> "majority",
      "readpreference.name" -> "secondaryPreferred",
      "readpreference.tagsets" -> """[{dc:"east", use:"production"}]""",
      "registersqlhelperfunctions" -> "false",
      "samplesize" -> "999",
      "sql.inferschema.maptypes.enabled" -> "false",
      "sql.inferschema.maptypes.minimumkeys" -> "900",
      "sql.pipeline.includefiltersandprojections" -> "false",
      "sql.pipeline.includenullfilters" -> "false",
      "uri" -> "mongodb://127.0.0.1/"
    )

    val expectedReadConfig = ReadConfig("dbName", "collName", Some("mongodb://127.0.0.1/"), 999, MongoSplitVectorPartitioner,
      Map("partitionsizemb" -> "15"), 99,
      ReadPreferenceConfig(ReadPreference.secondaryPreferred(new TagSet(List(new Tag("dc", "east"), new Tag("use", "production")).asJava))),
      ReadConcernConfig(ReadConcern.MAJORITY), AggregationConfig(
        List(BsonDocument.parse("""{ "$match" : { "a" : 1 } }""")),
        Collation.builder().locale("en").build(), BsonDocument.parse("{a : 1 }")
      ),
      inferSchemaMapTypesEnabled = false,
      inferSchemaMapTypesMinimumKeys = 900,
      pipelineIncludeNullFilters = false,
      pipelineIncludeFiltersAndProjections = false)

    defaultReadConfig.withOptions(options) should equal(expectedReadConfig)
  }

  it should "be able to create a map" in {
    val readConfig = ReadConfig("dbName", "collName", Some("mongodb://localhost/"), 200, MongoSplitVectorPartitioner,
      Map("partitionsizemb" -> "15"), 10,
      ReadPreferenceConfig(ReadPreference.secondaryPreferred(List(
        new TagSet(List(new Tag("dc", "east"), new Tag("use", "production")).asJava),
        new TagSet()
      ).asJava)),
      ReadConcernConfig(ReadConcern.MAJORITY),
      AggregationConfig(List(BsonDocument.parse("{$match: {a: 1}}")), Collation.builder().build(), new BsonDocument()),
      registerSQLHelperFunctions = false,
      inferSchemaMapTypesEnabled = false,
      inferSchemaMapTypesMinimumKeys = 999,
      pipelineIncludeNullFilters = false,
      pipelineIncludeFiltersAndProjections = false,
      samplePoolSize = 12000)

    val expectedReadConfigMap = Map(
      "database" -> "dbName",
      "collection" -> "collName",
      "uri" -> "mongodb://localhost/",
      "partitioner" -> "com.mongodb.spark.rdd.partitioner.MongoSplitVectorPartitioner$",
      "partitioneroptions.partitionsizemb" -> "15",
      "localthreshold" -> "10",
      "readpreference.name" -> "secondaryPreferred",
      "readpreference.tagsets" -> """[{dc:"east",use:"production"},{}]""",
      "readconcern.level" -> "majority",
      "samplesize" -> "200",
      "samplepoolsize" -> "12000",
      "registersqlhelperfunctions" -> "false",
      "sql.inferschema.maptypes.enabled" -> "false",
      "sql.inferschema.maptypes.minimumkeys" -> "999",
      "sql.pipeline.includenullfilters" -> "false",
      "sql.pipeline.includefiltersandprojections" -> "false",
      "pipeline" -> """[{ "$match" : { "a" : 1 } }]"""
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
    an[IllegalArgumentException] should be thrownBy ReadConfig("db", "collection", Some("localhost/db.coll"))
    an[IllegalArgumentException] should be thrownBy ReadConfig(new SparkConf().set("spark.mongodb.input.uri", "localhost/db.coll"))
    an[IllegalArgumentException] should be thrownBy ReadConfig(new SparkConf().set(
      "spark.mongodb.input.uri",
      "mongodb://localhost/db.coll/?readPreference=AllNodes"
    ))
    an[IllegalArgumentException] should be thrownBy ReadConfig(new SparkConf().set("spark.mongodb.input.collection", "coll"))
    an[IllegalArgumentException] should be thrownBy ReadConfig(new SparkConf().set("spark.mongodb.input.database", "db"))
    an[IllegalArgumentException] should be thrownBy ReadConfig(sparkConf.clone().set("spark.mongodb.input.localThreshold", "-1"))
    an[IllegalArgumentException] should be thrownBy ReadConfig(sparkConf.clone().set("spark.mongodb.input.readPreference.tagSets", "[1, 2]"))
    an[IllegalArgumentException] should be thrownBy ReadConfig(sparkConf.clone().set("spark.mongodb.input.readPreference.tagSets", "-1]"))
    an[IllegalArgumentException] should be thrownBy ReadConfig(sparkConf.clone().set("spark.mongodb.input.readConcern.level", "Alpha"))
    an[IllegalArgumentException] should be thrownBy ReadConfig(sparkConf.clone().set("spark.mongodb.input.hint", "notADoc"))
    an[IllegalArgumentException] should be thrownBy ReadConfig(sparkConf.clone().set("spark.mongodb.input.collation", "notADoc"))
    an[IllegalArgumentException] should be thrownBy ReadConfig(sparkConf.clone().set("spark.mongodb.input.pipeline", "notADoc"))
  }

  val sparkConf = new SparkConf()
    .set("spark.mongodb.input.database", "db")
    .set("spark.mongodb.input.collection", "collection")
    .set("spark.mongodb.input.partitioner", "MongoShardedPartitioner$")
    .set("spark.mongodb.input.partitionerOptions.shardKey", "ID")
    .set("spark.mongodb.input.localThreshold", "0")
    .set("spark.mongodb.input.readPreference.name", "secondary")
    .set("spark.mongodb.input.readConcern.level", "local")
    .set("spark.mongodb.input.sampleSize", "150")
    .set("spark.mongodb.input.samplePoolSize", "1500")
    .set("spark.mongodb.input.collation", Collation.builder().locale("en").caseLevel(true).collationCaseFirst(CollationCaseFirst.OFF)
      .collationStrength(CollationStrength.IDENTICAL).numericOrdering(true).collationAlternate(CollationAlternate.SHIFTED)
      .collationMaxVariable(CollationMaxVariable.SPACE).backwards(true).normalization(true).build().asDocument().toJson)
    .set("spark.mongodb.input.hint", """{ "a" : 1, "b" : -1 }""")
    .set("spark.mongodb.input.pipeline", """[{ "$match" : { "a" : 1 } }]""")

}
// scalastyle:on magic.number

