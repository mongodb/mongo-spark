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

import scala.reflect.ClassTag

import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}

import org.bson.Document
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.rdd.api.java.JavaMongoRDD
import com.mongodb.spark.rdd.{DocumentRDDFunctions, MongoRDD}

object MongoSpark {

  /**
   * Load data from MongoDB
   *
   * @param sc the Spark context containing the MongoDB connection configuration
   * @return a MongoRDD
   */
  def load(sc: JavaSparkContext): JavaMongoRDD[Document] = load(sc, classOf[Document])

  /**
   * Load data from MongoDB
   *
   * @param sc    the Spark context containing the MongoDB connection configuration
   * @param clazz the class of the return type for the RDD
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def load[D](sc: JavaSparkContext, clazz: Class[D]): JavaMongoRDD[D] = {
    notNull("sc", sc)
    notNull("clazz", clazz)
    implicit def ct: ClassTag[D] = ClassTag(clazz)
    MongoRDD[D](sc.sc).toJavaRDD()
  }

  /**
   * Load data from MongoDB
   *
   * @param sc        the Spark context
   * @param connector the { @link MongoConnector}
   * @return a MongoRDD
   */
  def load(sc: JavaSparkContext, connector: MongoConnector): JavaMongoRDD[Document] = load(sc, connector, classOf[Document])

  /**
   * Load data from MongoDB
   *
   * @param sc        the Spark context
   * @param connector the { @link MongoConnector}
   * @param clazz the class of the return type for the RDD
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def load[D](sc: JavaSparkContext, connector: MongoConnector, clazz: Class[D]): JavaMongoRDD[D] = {
    notNull("sc", sc)
    notNull("connector", connector)
    notNull("clazz", clazz)
    implicit def ct: ClassTag[D] = ClassTag(clazz)
    MongoRDD(sc.sc, connector).toJavaRDD()
  }

  /**
   * Load data from MongoDB
   *
   * @param sc        the Spark context
   * @param connector the { @link MongoConnector}
   * @param maxChunkSize the maximum chunkSize for non-sharded collections
   * @param splitKey     the key to split the collection by for non-sharded collections
   * @return a MongoRDD
   */
  def load(sc: JavaSparkContext, connector: MongoConnector, splitKey: String, maxChunkSize: Int): JavaMongoRDD[Document] =
    load(sc, connector, splitKey, maxChunkSize, classOf[Document])

  /**
   * Load data from MongoDB
   *
   * @param sc        the Spark context
   * @param connector the { @link MongoConnector}
   * @param splitKey     the key to split the collection by for non-sharded collections
   * @param maxChunkSize the maximum chunkSize for non-sharded collections
   * @param clazz        the class of the return type for the RDD
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def load[D](sc: JavaSparkContext, connector: MongoConnector, splitKey: String, maxChunkSize: Int, clazz: Class[D]): JavaMongoRDD[D] = {
    notNull("sc", sc)
    notNull("connector", connector)
    notNull("splitKey", splitKey)
    notNull("clazz", clazz)
    implicit def ct: ClassTag[D] = ClassTag(clazz)
    MongoRDD(sc.sc, connector, splitKey, maxChunkSize).toJavaRDD()
  }

  /**
   * Save data to MongoDB
   *
   * Uses the `SparkConf` for the database and collection information
   *
   * @param javaRDD the RDD data to save to MongoDB
   * @return the javaRDD
   */
  def save(javaRDD: JavaRDD[Document]): JavaRDD[Document] =
    save(javaRDD, classOf[Document])

  /**
   * Save data to MongoDB
   *
   * Uses the `SparkConf` for the database and collection information
   * Requires a codec for the data type
   *
   * @param javaRDD the RDD data to save to MongoDB
   * @param clazz   the class of the data contained in the RDD
   * @tparam D the type of the data in the RDD
   * @return the javaRDD
   */
  def save[D](javaRDD: JavaRDD[D], clazz: Class[D]): JavaRDD[D] = {
    notNull("javaRDD", javaRDD)
    implicit def ct: ClassTag[D] = ClassTag(clazz)
    DocumentRDDFunctions(JavaRDD.toRDD(javaRDD)).saveToMongoDB()
    javaRDD
  }

  /**
   * Save data to MongoDB
   *
   * Uses the `MongoConnector` for the database and collection information
   *
   * @param javaRDD   the RDD data to save to MongoDB
   * @param connector the { @link MongoConnector}
   * @return the javaRDD
   */
  def save(javaRDD: JavaRDD[Document], connector: MongoConnector): JavaRDD[Document] =
    save(javaRDD, connector, classOf[Document])

  /**
   * Save data to MongoDB
   *
   * Uses the `MongoConnector` for the database and collection information
   * Requires a codec for the data type
   *
   * @param javaRDD   the RDD data to save to MongoDB
   * @param connector the { @link MongoConnector}
   * @param clazz the class of the data contained in the RDD
   * @tparam D the type of the data in the RDD
   * @return the javaRDD
   */
  def save[D](javaRDD: JavaRDD[D], connector: MongoConnector, clazz: Class[D]): JavaRDD[D] = {
    notNull("javaRDD", javaRDD)
    notNull("connector", connector)
    implicit def ct: ClassTag[D] = ClassTag(clazz)
    DocumentRDDFunctions(JavaRDD.toRDD(javaRDD), connector).saveToMongoDB()
    javaRDD
  }

  /**
   * Save data to MongoDB
   *
   * Uses the `SparkConf` for the database information
   *
   * @param javaRDD        the RDD data to save to MongoDB
   * @param collectionName the name of the collection to use
   * @return the javaRDD
   */
  def save(javaRDD: JavaRDD[Document], collectionName: String): JavaRDD[Document] =
    save(javaRDD, collectionName, classOf[Document])

  /**
   * Save data to MongoDB
   *
   * Uses the `SparkConf` for the database information
   * Requires a codec for the data type
   *
   * @param javaRDD        the RDD data to save to MongoDB
   * @param collectionName the name of the collection to use
   * @param clazz          the class of the data contained in the RDD
   * @tparam D the type of the data in the RDD
   * @return the javaRDD
   */
  def save[D](javaRDD: JavaRDD[D], collectionName: String, clazz: Class[D]): JavaRDD[D] = {
    notNull("javaRDD", javaRDD)
    notNull("collectionName", collectionName)
    implicit def ct: ClassTag[D] = ClassTag(clazz)
    DocumentRDDFunctions(JavaRDD.toRDD(javaRDD), collectionName).saveToMongoDB()
    javaRDD
  }

  /**
   * Save data to MongoDB
   *
   * Uses the `MongoConnector` for the database information
   *
   * @param javaRDD   the RDD data to save to MongoDB
   * @param connector the { @link MongoConnector}
   * @param collectionName the name of the collection to use
   * @return the javaRDD
   */
  def save(javaRDD: JavaRDD[Document], connector: MongoConnector, collectionName: String): JavaRDD[Document] =
    save(javaRDD, connector, collectionName, classOf[Document])

  /**
   * Save data to MongoDB
   *
   * Uses the `MongoConnector` for the database information
   * Requires a codec for the data type
   *
   * @param javaRDD   the RDD data to save to MongoDB
   * @param connector the { @link MongoConnector}
   * @param collectionName the name of the collection to use
   * @param clazz          the class of the data contained in the RDD
   * @tparam D the type of the data in the RDD
   * @return the javaRDD
   */
  def save[D](javaRDD: JavaRDD[D], connector: MongoConnector, collectionName: String, clazz: Class[D]): JavaRDD[D] = {
    notNull("javaRDD", javaRDD)
    notNull("connector", connector)
    notNull("collectionName", collectionName)
    implicit def ct: ClassTag[D] = ClassTag(clazz)
    DocumentRDDFunctions(JavaRDD.toRDD(javaRDD), connector, collectionName).saveToMongoDB()
    javaRDD
  }

  /**
   * Save data to MongoDB
   *
   * Requires a codec for the data type
   *
   * @param javaRDD   the RDD data to save to MongoDB
   * @param connector the { @link MongoConnector}
   * @param databaseName   the name of the database to use
   * @param collectionName the name of the collection to use
   * @return the javaRDD
   */
  def save(javaRDD: JavaRDD[Document], connector: MongoConnector, databaseName: String, collectionName: String): JavaRDD[Document] =
    save(javaRDD, connector, databaseName, collectionName, classOf[Document])

  /**
   * Save data to MongoDB
   *
   * Requires a codec for the data type
   *
   * @param javaRDD   the RDD data to save to MongoDB
   * @param connector the { @link MongoConnector}
   * @param databaseName   the name of the database to use
   * @param collectionName the name of the collection to use
   * @param clazz          the class of the data contained in the RDD
   * @tparam D the type of the data in the RDD
   * @return the javaRDD
   */
  def save[D](javaRDD: JavaRDD[D], connector: MongoConnector, databaseName: String, collectionName: String, clazz: Class[D]): JavaRDD[D] = {
    notNull("javaRDD", javaRDD)
    notNull("connector", connector)
    notNull("databaseName", databaseName)
    notNull("collectionName", collectionName)
    implicit def ct: ClassTag[D] = ClassTag(clazz)
    DocumentRDDFunctions(JavaRDD.toRDD(javaRDD), connector, databaseName, collectionName).saveToMongoDB()
    javaRDD
  }

  private def notNull[T](name: String, value: T): Unit = {
    if (Option(value).isEmpty) throw new IllegalArgumentException(name + " can not be null")
  }
}
