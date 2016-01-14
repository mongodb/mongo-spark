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

import org.apache.spark.SparkConf
import org.apache.spark.api.java.function.{Function0 => JFunction0}

import com.mongodb.MongoClient
import com.mongodb.spark.MongoConnector

/**
 * A helper class to create a MongoConnector
 *
 * @since 1.0
 */
object MongoConnectors {
  /**
   * Creates a MongoConnector
   *
   * @param sparkConf the spark configuration
   * @return the MongoConnector
   */
  def create(sparkConf: SparkConf): MongoConnector = MongoConnector(sparkConf)

  /**
   * Creates a MongoConnector
   *
   * @param uri            the MongoClient connection string
   * @param databaseName   the name of the database to use
   * @param collectionName the name of the collection to use
   * @return the MongoConnector
   */
  def create(uri: String, databaseName: String, collectionName: String): MongoConnector =
    MongoConnector(uri, databaseName, collectionName)

  /**
   * Creates a MongoConnector
   *
   * @param mongoClientFactory the factory for creating the MongoClient
   * @param databaseName       the name of the database to use
   * @param collectionName     the name of the collection to use
   * @return the MongoConnector
   */
  def create(mongoClientFactory: JFunction0[MongoClient], databaseName: String, collectionName: String): MongoConnector =
    MongoConnector(() => mongoClientFactory.call(), databaseName, collectionName)
}
