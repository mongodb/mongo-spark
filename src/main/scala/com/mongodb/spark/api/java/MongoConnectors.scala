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

package com.mongodb.spark.api.java

import com.mongodb.spark.{MongoClientFactory, MongoConnector, notNull}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.{Function0 => JFunction0}
import org.apache.spark.sql.SQLContext

/**
 * A helper class to create a MongoConnector
 *
 * @since 1.0
 */
object MongoConnectors {

  /**
   * Creates a MongoConnector
   *
   * @param sqlContext the SQLContext
   * @return the MongoConnector
   */
  def create(sqlContext: SQLContext): MongoConnector = {
    notNull("sqlContext", sqlContext)
    MongoConnector(sqlContext.sparkContext)
  }

  /**
   * Creates a MongoConnector
   *
   * @param javaSparkContext the Java Spark context
   * @return the MongoConnector
   */
  def create(javaSparkContext: JavaSparkContext): MongoConnector = {
    notNull("javaSparkContext", javaSparkContext)
    MongoConnector(javaSparkContext.sc)
  }

  /**
   * Creates a MongoConnector
   *
   * @param sparkConf the spark configuration
   * @return the MongoConnector
   */
  def create(sparkConf: SparkConf): MongoConnector = {
    notNull("sparkConf", sparkConf)
    MongoConnector(sparkConf)
  }

  /**
   * Creates a MongoConnector
   *
   * @param connectionString            the MongoClient connection string
   * @return the MongoConnector
   */
  def create(connectionString: String): MongoConnector = {
    notNull("connectionString", connectionString)
    MongoConnector(connectionString)
  }

  /**
   * Creates a MongoConnector
   *
   * @param mongoClientFactory the factory for creating the MongoClient
   * @return the MongoConnector
   */
  def create(mongoClientFactory: MongoClientFactory): MongoConnector = {
    notNull("mongoClientFactory", mongoClientFactory)
    MongoConnector(mongoClientFactory)
  }
}
