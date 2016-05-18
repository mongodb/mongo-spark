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

package com.mongodb.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import com.mongodb.spark.RequiresMongoDB

class MongoDataFrameNoConfSpec extends RequiresMongoDB {

  "DataFrame Readers and Writers" should "be able to accept just options" in {
    val characters = Seq(Character("Gandalf", 1000), Character("Bilbo Baggins", 50)) //scalastyle:ignore

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    sc.parallelize(characters).toDF().write
      .option("mode", "Overwrite")
      .option("uri", mongoClientURI)
      .option("database", databaseName)
      .option("collection", collectionName)
      .mongo()

    val ds = sqlContext.read
      .option("uri", mongoClientURI)
      .option("database", databaseName)
      .option("collection", collectionName)
      .mongo().as[Character]

    ds.collect().toList should equal(characters)
  }

  private lazy val sc: SparkContext = {
    new SparkContext(
      new SparkConf()
        .setMaster("local")
        .setAppName("MongoSparkConnector")
    )
  }

  override def beforeEach() {}

  override def afterAll() {
    super.afterAll()
    sc.stop()
  }

}
