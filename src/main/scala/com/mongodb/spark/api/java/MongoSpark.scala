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

import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.rdd.api.java.JavaMongoRDD
import com.mongodb.spark.rdd.{DocumentRDDFunctions, MongoRDD}
import com.mongodb.spark.sql._
import com.mongodb.spark.{MongoConnector, notNull}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, SQLContext}
import org.bson.Document
import org.bson.conversions.Bson

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object MongoSpark {

  /**
   * Load data from MongoDB
   *
   * @param jsc the Spark context containing the MongoDB connection configuration
   * @return a MongoRDD
   */
  def load(jsc: JavaSparkContext): JavaMongoRDD[Document] = load(new SQLContext(jsc), classOf[Document])

  /**
   * Load data from MongoDB
   *
   * @param jsc    the Spark context containing the MongoDB connection configuration
   * @param clazz the class of the return type for the RDD
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def load[D](jsc: JavaSparkContext, clazz: Class[D]): JavaMongoRDD[D] = load(new SQLContext(jsc), MongoConnectors.create(jsc.getConf), clazz)

  /**
   * Load data from MongoDB
   *
   * @param jsc        the Java Spark context
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @return a MongoRDD
   */
  def load(jsc: JavaSparkContext, readConfig: ReadConfig): JavaMongoRDD[Document] = load(jsc, MongoConnectors.create(jsc), readConfig)

  /**
   * Load data from MongoDB
   *
   * @param jsc        the Java Spark context
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @param clazz        the class of the return type for the RDD
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def load[D](jsc: JavaSparkContext, readConfig: ReadConfig, clazz: Class[D]): JavaMongoRDD[D] =
    load(jsc, MongoConnectors.create(jsc), readConfig, clazz)

  /**
   * Load data from MongoDB
   *
   * @param jsc        the Java Spark context
   * @param connector the[[com.mongodb.spark.MongoConnector]]
   * @return a MongoRDD
   */
  def load(jsc: JavaSparkContext, connector: MongoConnector): JavaMongoRDD[Document] = load(new SQLContext(jsc), connector, classOf[Document])

  /**
   * Load data from MongoDB
   *
   * @param jsc        the Java Spark context
   * @param connector the[[com.mongodb.spark.MongoConnector]]
   * @param clazz the class of the return type for the RDD
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def load[D](jsc: JavaSparkContext, connector: MongoConnector, clazz: Class[D]): JavaMongoRDD[D] =
    load(new SQLContext(jsc), connector, ReadConfig(jsc.getConf), clazz)

  /**
   * Load data from MongoDB
   *
   * @param jsc        the Java Spark context
   * @param connector the[[com.mongodb.spark.MongoConnector]]
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @return a MongoRDD
   */
  def load(jsc: JavaSparkContext, connector: MongoConnector, readConfig: ReadConfig): JavaMongoRDD[Document] =
    load(new SQLContext(jsc), connector, readConfig, classOf[Document])

  /**
   * Load data from MongoDB
   *
   * @param jsc        the Java Spark context
   * @param connector the[[com.mongodb.spark.MongoConnector]]
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @param clazz        the class of the return type for the RDD
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def load[D](jsc: JavaSparkContext, connector: MongoConnector, readConfig: ReadConfig, clazz: Class[D]): JavaMongoRDD[D] =
    load(new SQLContext(jsc), connector, readConfig, clazz)

  /**
   * Load data from MongoDB
   *
   * @param jsc        the Java Spark context
   * @param connector the[[com.mongodb.spark.MongoConnector]]
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @param pipeline aggregate pipeline
   * @return a MongoRDD
   */
  def load(jsc: JavaSparkContext, connector: MongoConnector, readConfig: ReadConfig, pipeline: util.List[Bson]): JavaMongoRDD[Document] =
    load(new SQLContext(jsc), connector, readConfig, pipeline, classOf[Document])

  /**
   * Load data from MongoDB
   *
   * @param jsc        the Java Spark context
   * @param connector the[[com.mongodb.spark.MongoConnector]]
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @param pipeline aggregate pipeline
   * @param clazz        the class of the return type for the RDD
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def load[D](jsc: JavaSparkContext, connector: MongoConnector, readConfig: ReadConfig, pipeline: util.List[Bson], clazz: Class[D]): JavaMongoRDD[D] =
    load(new SQLContext(jsc), connector, readConfig, pipeline, clazz)

  /**
   * Load data from MongoDB
   *
   * @param sqlContext the Spark context containing the MongoDB connection configuration
   * @return a MongoRDD
   */
  def load(sqlContext: SQLContext): JavaMongoRDD[Document] = load(sqlContext, classOf[Document])

  /**
   * Load data from MongoDB
   *
   * @param sqlContext    the Spark context containing the MongoDB connection configuration
   * @param clazz the class of the return type for the RDD
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def load[D](sqlContext: SQLContext, clazz: Class[D]): JavaMongoRDD[D] = load(sqlContext, MongoConnectors.create(sqlContext), clazz)

  /**
   * Load data from MongoDB
   *
   * @param sqlContext        the SQLContext
   * @param connector the[[com.mongodb.spark.MongoConnector]]
   * @return a MongoRDD
   */
  def load(sqlContext: SQLContext, connector: MongoConnector): JavaMongoRDD[Document] = load(sqlContext, connector, classOf[Document])

  /**
   * Load data from MongoDB
   *
   * @param sqlContext        the SQLContext
   * @param connector the[[com.mongodb.spark.MongoConnector]]
   * @param clazz the class of the return type for the RDD
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def load[D](sqlContext: SQLContext, connector: MongoConnector, clazz: Class[D]): JavaMongoRDD[D] =
    load(sqlContext, connector, ReadConfig(sqlContext.sparkContext), clazz)

  /**
   * Load data from MongoDB
   *
   * @param sqlContext        the SQLContext
   * @param connector the[[com.mongodb.spark.MongoConnector]]
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @return a MongoRDD
   */
  def load(sqlContext: SQLContext, connector: MongoConnector, readConfig: ReadConfig): JavaMongoRDD[Document] =
    load(sqlContext, connector, readConfig, classOf[Document])

  /**
   * Load data from MongoDB
   *
   * @param sqlContext        the SQLContext
   * @param connector the[[com.mongodb.spark.MongoConnector]]
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @param clazz        the class of the return type for the RDD
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def load[D](sqlContext: SQLContext, connector: MongoConnector, readConfig: ReadConfig, clazz: Class[D]): JavaMongoRDD[D] =
    load(sqlContext, connector, readConfig, util.Collections.emptyList[Bson](), clazz)

  /**
   * Load data from MongoDB
   *
   * @param sqlContext        the SQLContext
   * @param connector the[[com.mongodb.spark.MongoConnector]]
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @param pipeline aggregate pipeline
   * @return a MongoRDD
   */
  def load(sqlContext: SQLContext, connector: MongoConnector, readConfig: ReadConfig, pipeline: util.List[Bson]): JavaMongoRDD[Document] =
    load(sqlContext, connector, readConfig, pipeline, classOf[Document])

  /**
   * Load data from MongoDB
   *
   * @param sqlContext        the SQLContext
   * @param connector the[[com.mongodb.spark.MongoConnector]]
   * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
   * @param pipeline aggregate pipeline
   * @param clazz        the class of the return type for the RDD
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def load[D](sqlContext: SQLContext, connector: MongoConnector, readConfig: ReadConfig, pipeline: util.List[Bson], clazz: Class[D]): JavaMongoRDD[D] = {
    notNull("sqlContext", sqlContext)
    notNull("connector", connector)
    notNull("readConfig", readConfig)
    notNull("clazz", clazz)
    implicit def ct: ClassTag[D] = ClassTag(clazz)
    MongoRDD(sqlContext, connector, readConfig, pipeline.asScala).toJavaRDD()
  }

  /**
   * Load data from MongoDB
   *
   * @param dataFrameReader the DataFrameReader to load
   * @return a DataFrame
   */
  def load(dataFrameReader: DataFrameReader): DataFrame = load(dataFrameReader, null) // scalastyle:ignore

  /**
   * Load data from MongoDB
   *
   * @param dataFrameReader  the DataFrameReader to load
   * @param clazz the java Bean class representing the Schema for the DataFrame
   * @tparam D the type of Document to return
   * @return a DataFrame
   */
  def load[D](dataFrameReader: DataFrameReader, clazz: Class[D]): DataFrame = {
    Option(clazz) match {
      case Some(c) => dataFrameReader.mongo(MongoInferSchema.reflectSchema(c))
      case None    => dataFrameReader.mongo()
    }
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

  /**
   * Creates a DataFrameReader with the `MongoDB` underlying output data source.
   *
   * Uses the `SparkConf` for the database and collection information
   *
   * @param sqlContext the SQLContext
   * @return the DataFrameReader
   */
  def read(sqlContext: SQLContext): DataFrameReader = sqlContext.read.format("com.mongodb.spark.sql")

  /**
   * Creates a DataFrameWriter with the `MongoDB` underlying output data source.
   *
   * Uses the `SparkConf` for the database and collection information
   *
   * @param dataFrame the DataFrame to convert into a DataFrameWriter
   * @return the DataFrameWriter
   */
  def write(dataFrame: DataFrame): DataFrameWriter = dataFrame.write.format("com.mongodb.spark.sql")

}
