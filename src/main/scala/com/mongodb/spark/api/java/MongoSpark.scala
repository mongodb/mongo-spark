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

import java.util
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}

import org.bson.Document
import org.bson.conversions.Bson
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.{PartitionConfig, WriteConfig, ReadConfig}
import com.mongodb.spark.rdd.api.java.JavaMongoRDD
import com.mongodb.spark.rdd.{DocumentRDDFunctions, MongoRDD}
import com.mongodb.spark.notNull

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
  def load[D](sc: JavaSparkContext, clazz: Class[D]): JavaMongoRDD[D] = load(sc, MongoConnectors.create(sc.getConf), clazz)

  /**
   * Load data from MongoDB
   *
   * @param sc        the Spark context
   * @param connector the[[com.mongodb.spark.MongoConnector]]
   * @return a MongoRDD
   */
  def load(sc: JavaSparkContext, connector: MongoConnector): JavaMongoRDD[Document] = load(sc, connector, classOf[Document])

  /**
   * Load data from MongoDB
   *
   * @param sc        the Spark context
   * @param connector the[[com.mongodb.spark.MongoConnector]]
   * @param clazz the class of the return type for the RDD
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def load[D](sc: JavaSparkContext, connector: MongoConnector, clazz: Class[D]): JavaMongoRDD[D] =
    load(sc, connector, ReadConfig(sc.getConf), clazz)

  /**
   * Load data from MongoDB
   *
   * @param sc        the Spark context
   * @param connector the[[com.mongodb.spark.MongoConnector]]
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @return a MongoRDD
   */
  def load(sc: JavaSparkContext, connector: MongoConnector, readConfig: ReadConfig): JavaMongoRDD[Document] =
    load(sc, connector, readConfig, classOf[Document])

  /**
   * Load data from MongoDB
   *
   * @param sc        the Spark context
   * @param connector the[[com.mongodb.spark.MongoConnector]]
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @param clazz        the class of the return type for the RDD
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def load[D](sc: JavaSparkContext, connector: MongoConnector, readConfig: ReadConfig, clazz: Class[D]): JavaMongoRDD[D] =
    load(sc, connector, readConfig, PartitionConfig(sc.getConf), clazz)

  /**
   * Load data from MongoDB
   *
   * @param sc        the Spark context
   * @param connector the[[com.mongodb.spark.MongoConnector]]
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @param partitionConfig the [[com.mongodb.spark.config.PartitionConfig]]
   * @return a MongoRDD
   */
  def load(sc: JavaSparkContext, connector: MongoConnector, readConfig: ReadConfig, partitionConfig: PartitionConfig): JavaMongoRDD[Document] =
    load(sc, connector, readConfig, partitionConfig, classOf[Document])

  /**
   * Load data from MongoDB
   *
   * @param sc        the Spark context
   * @param connector the[[com.mongodb.spark.MongoConnector]]
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @param partitionConfig the [[com.mongodb.spark.config.PartitionConfig]]
   * @param clazz        the class of the return type for the RDD
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def load[D](sc: JavaSparkContext, connector: MongoConnector, readConfig: ReadConfig, partitionConfig: PartitionConfig, clazz: Class[D]): JavaMongoRDD[D] =
    load(sc, connector, readConfig, partitionConfig, util.Collections.emptyList(), clazz)

  /**
   * Load data from MongoDB
   *
   * @param sc        the Spark context
   * @param connector the[[com.mongodb.spark.MongoConnector]]
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @param partitionConfig the [[com.mongodb.spark.config.PartitionConfig]]
   * @param pipeline aggregate pipeline
   * @return a MongoRDD
   */
  def load(sc: JavaSparkContext, connector: MongoConnector, readConfig: ReadConfig, partitionConfig: PartitionConfig,
           pipeline: util.List[Bson]): JavaMongoRDD[Document] =
    load(sc, connector, readConfig, partitionConfig, pipeline, classOf[Document])

  /**
   * Load data from MongoDB
   *
   * @param sc        the Spark context
   * @param connector the[[com.mongodb.spark.MongoConnector]]
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @param partitionConfig the [[com.mongodb.spark.config.PartitionConfig]]
   * @param pipeline aggregate pipeline
   * @param clazz        the class of the return type for the RDD
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def load[D](sc: JavaSparkContext, connector: MongoConnector, readConfig: ReadConfig, partitionConfig: PartitionConfig,
              pipeline: util.List[Bson], clazz: Class[D]): JavaMongoRDD[D] = {
    notNull("sc", sc)
    notNull("connector", connector)
    notNull("readConfig", readConfig)
    notNull("partitionConfig", partitionConfig)
    notNull("clazz", clazz)
    implicit def ct: ClassTag[D] = ClassTag(clazz)
    MongoRDD(sc.sc, connector, readConfig, partitionConfig, pipeline.asScala).toJavaRDD()
  }

  /**
   * Save data to MongoDB
   *
   * Uses the `SparkConf` for the database and collection information
   *
   * @param javaRDD the RDD data to save to MongoDB
   * @return the javaRDD
   */
  def save(javaRDD: JavaRDD[Document]): Unit = save(javaRDD, classOf[Document])

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
  def save[D](javaRDD: JavaRDD[D], clazz: Class[D]): Unit = {
    notNull("javaRDD", javaRDD)
    implicit def ct: ClassTag[D] = ClassTag(clazz)
    DocumentRDDFunctions(JavaRDD.toRDD(javaRDD)).saveToMongoDB()
  }

  /**
   * Save data to MongoDB
   *
   * Uses the `SparkConf` for the database information
   *
   * @param javaRDD        the RDD data to save to MongoDB
   * @param writeConfig the [[com.mongodb.spark.config.WriteConfig]]
   * @return the javaRDD
   */
  def save(javaRDD: JavaRDD[Document], writeConfig: WriteConfig): Unit =
    save(javaRDD, writeConfig, classOf[Document])

  /**
   * Save data to MongoDB
   *
   * Uses the `writeConfig` for the database information
   * Requires a codec for the data type
   *
   * @param javaRDD        the RDD data to save to MongoDB
   * @param writeConfig the [[com.mongodb.spark.config.WriteConfig]]
   * @param clazz          the class of the data contained in the RDD
   * @tparam D the type of the data in the RDD
   * @return the javaRDD
   */
  def save[D](javaRDD: JavaRDD[D], writeConfig: WriteConfig, clazz: Class[D]): Unit = {
    notNull("javaRDD", javaRDD)
    notNull("writeConfig", writeConfig)
    implicit def ct: ClassTag[D] = ClassTag(clazz)
    DocumentRDDFunctions(JavaRDD.toRDD(javaRDD)).saveToMongoDB(writeConfig)
  }

}
