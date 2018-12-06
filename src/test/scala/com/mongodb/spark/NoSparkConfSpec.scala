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

package com.mongodb.spark

import com.mongodb.MongoClient
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.connection.DefaultMongoClientFactory
import com.mongodb.spark.sql.{Character, _}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

class NoSparkConfSpec extends RequiresMongoDB {

  "MongoRDD" should "be able to accept just Read / Write Configs" in {
    val writeConfig = WriteConfig(Map("uri" -> mongoClientURI, "database" -> databaseName, "collection" -> collectionName))
    val readConfig = ReadConfig(Map("uri" -> mongoClientURI, "database" -> databaseName, "collection" -> collectionName,
      "partitioner" -> "TestPartitioner$"))

    val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))
    documents.saveToMongoDB(writeConfig = writeConfig)

    val rdd = MongoSpark.builder().sparkContext(sc).readConfig(readConfig).build().toRDD()
    rdd.count() should equal(10) //scalastyle:ignore
    rdd.map(_.getInteger("test")).collect().toList should equal((1 to 10).toList)
  }

  "DataFrame Readers and Writers" should "be able to accept just options" in {
    val characters = Seq(Character("Gandalf", 1000), Character("Bilbo Baggins", 50)) //scalastyle:ignore

    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    sc.parallelize(characters).toDF().write
      .option("mode", "Overwrite")
      .option("uri", mongoClientURI)
      .option("database", databaseName)
      .option("collection", collectionName)
      .mongo()

    val ds = sparkSession.read
      .option("uri", mongoClientURI)
      .option("database", databaseName)
      .option("collection", collectionName)
      .option("partitioner", "TestPartitioner$")
      .mongo().as[Character]

    ds.collect().toList should equal(characters)
  }

  it should "use the passed in connector" in {
    val writeConfig = WriteConfig(Map("uri" -> mongoClientURI, "database" -> databaseName, "collection" -> collectionName))
    val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))
    documents.saveToMongoDB(writeConfig = writeConfig)

    val connector = MongoConnector(CustomMongoClientFactory(mongoClientURI))
    val readConfig = ReadConfig(Map("uri" -> "mongodb://example.com/", "database" -> databaseName, "collection" -> collectionName,
      "partitioner" -> "TestPartitioner$"))
    val mongoSpark = MongoSpark.builder().sparkContext(sc).connector(connector).readConfig(readConfig).build()

    mongoSpark.toRDD().collect().size should equal(10)
    mongoSpark.toDF().count() should equal(10)
  }

  val sc: SparkContext = TestHelper.getOrCreateSparkContext(
    new SparkConf().setMaster("local").setAppName("MongoSparkConnector"),
    requiredCustomConf = true
  )

}

case class CustomMongoClientFactory(mongoClientURI: String) extends MongoClientFactory {
  private final val proxy: DefaultMongoClientFactory = DefaultMongoClientFactory(mongoClientURI)
  def create(): MongoClient = proxy.create()
}
