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

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.{BulkWriteOptions, InsertManyOptions, InsertOneModel, ReplaceOneModel, ReplaceOptions, UpdateOneModel, UpdateOptions}
import com.mongodb.spark.DefaultHelper.DefaultsTo
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.rdd.api.java.JavaMongoRDD
import com.mongodb.spark.sql.MapFunctions.rowToDocumentMapper
import com.mongodb.spark.sql.{MongoInferSchema, MongoRelation, helpers}
import org.apache.spark.SparkContext
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Dataset, Encoders, SQLContext, SparkSession}
import org.bson.conversions.Bson
import org.bson.{BsonDocument, Document}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * The MongoSpark helper allows easy creation of RDDs, DataFrames or Datasets from MongoDB.
 *
 * @since 1.0
 */
object MongoSpark {

  /**
   * The default source string for creating DataFrames from MongoDB
   */
  val defaultSource = "com.mongodb.spark.sql.DefaultSource"

  /**
   * Create a builder for configuring the [[MongoSpark]]
   *
   * @return a MongoSession Builder
   */
  def builder(): Builder = new Builder

  /**
   * Load data from MongoDB
   *
   * @param sc the Spark context containing the MongoDB connection configuration
   * @return a MongoRDD
   */
  def load[D: ClassTag](sc: SparkContext)(implicit e: D DefaultsTo Document): MongoRDD[D] = load(sc, ReadConfig(sc))

  /**
   * Load data from MongoDB
   *
   * @param sc the Spark context containing the MongoDB connection configuration
   * @param readConfig the custom readConfig
   * @return a MongoRDD
   */
  def load[D: ClassTag](sc: SparkContext, readConfig: ReadConfig)(implicit e: D DefaultsTo Document): MongoRDD[D] =
    builder().sparkContext(sc).readConfig(readConfig).build().toRDD[D]()

  /**
   * Load data from MongoDB
   *
   * @param sparkSession the SparkSession containing the MongoDB connection configuration
   * @tparam D The optional class defining the schema for the data
   * @return a MongoRDD
   */
  def load[D <: Product: TypeTag](sparkSession: SparkSession): DataFrame = builder().sparkSession(sparkSession).build().toDF[D]()

  /**
   * Load data from MongoDB
   *
   * @param sparkSession the SparkSession containing the MongoDB connection configuration
   * @param readConfig the custom readConfig
   * @tparam D The optional class defining the schema for the data
   * @return a MongoRDD
   */
  def load[D <: Product: TypeTag](sparkSession: SparkSession, readConfig: ReadConfig): DataFrame =
    builder().sparkSession(sparkSession).readConfig(readConfig).build().toDF[D]()

  /**
   * Save data to MongoDB
   *
   * Uses the `SparkConf` for the database and collection information
   * Requires a codec for the data type
   *
   * @param rdd the RDD data to save to MongoDB
   * @tparam D the type of the data in the RDD
   */
  def save[D: ClassTag](rdd: RDD[D]): Unit = save(rdd, WriteConfig(rdd.sparkContext))

  /**
   * Save data to MongoDB
   *
   * @param rdd the RDD data to save to MongoDB
   * @param writeConfig the writeConfig
   * @tparam D the type of the data in the RDD
   */
  def save[D: ClassTag](rdd: RDD[D], writeConfig: WriteConfig): Unit = {
    val mongoConnector = MongoConnector(writeConfig.asOptions)
    rdd.foreachPartition(iter => if (iter.nonEmpty) {
      mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[D] =>
        iter.grouped(writeConfig.maxBatchSize).foreach(batch => collection.insertMany(
          batch.toList.asJava,
          new InsertManyOptions().ordered(writeConfig.ordered)
        ))
      })
    })
  }

  /**
   * Save data to MongoDB
   *
   * Uses the `SparkConf` for the database and collection information
   *
   * '''Note:''' If the dataFrame contains an `_id` field the data will upserted and replace any existing documents in the collection.
   *
   * @param dataset the dataset to save to MongoDB
   * @tparam D
   * @since 1.1.0
   */
  def save[D](dataset: Dataset[D]): Unit = save(dataset, WriteConfig(dataset.sparkSession.sparkContext.getConf))

  /**
   * Save data to MongoDB
   *
   * '''Note:''' If the dataFrame contains an `_id` field the data will upserted and replace any existing documents in the collection.
   *
   * @param dataset the dataset to save to MongoDB
   * @param writeConfig the writeConfig
   * @tparam D
   * @since 1.1.0
   */
  def save[D](dataset: Dataset[D], writeConfig: WriteConfig): Unit = {
    val mongoConnector = MongoConnector(writeConfig.asOptions)
    val dataSet = dataset.toDF()
    val mapper = rowToDocumentMapper(dataSet.schema)
    val documentRdd: RDD[BsonDocument] = dataSet.rdd.map(row => mapper(row))
    val fieldNames = dataset.schema.fieldNames.toList
    val queryKeyList = BsonDocument.parse(writeConfig.shardKey.getOrElse("{_id: 1}")).keySet().asScala.toList

    if (writeConfig.forceInsert || !queryKeyList.forall(fieldNames.contains(_))) {
      MongoSpark.save(documentRdd, writeConfig)
    } else {
      documentRdd.foreachPartition(iter => if (iter.nonEmpty) {
        mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[BsonDocument] =>
          iter.grouped(writeConfig.maxBatchSize).foreach(batch => {
            val requests = batch.map(doc =>
              if (queryKeyList.forall(doc.containsKey(_))) {
                val queryDocument = new BsonDocument()
                queryKeyList.foreach(key => queryDocument.append(key, doc.get(key)))
                if (writeConfig.replaceDocument) {
                  new ReplaceOneModel[BsonDocument](queryDocument, doc, new ReplaceOptions().upsert(true))
                } else {
                  queryDocument.keySet().asScala.foreach(doc.remove(_))
                  new UpdateOneModel[BsonDocument](queryDocument, new BsonDocument("$set", doc), new UpdateOptions().upsert(true))
                }
              } else {
                new InsertOneModel[BsonDocument](doc)
              })
            collection.bulkWrite(requests.toList.asJava, new BulkWriteOptions().ordered(writeConfig.ordered))
          })
        })
      })
    }
  }

  /**
   * Save data to MongoDB
   *
   * Uses the `SparkConf` for the database and collection information
   *
   * @param dataFrameWriter the DataFrameWriter save to MongoDB
   */
  def save(dataFrameWriter: DataFrameWriter[_]): Unit = dataFrameWriter.format(defaultSource).save()

  /**
   * Save data to MongoDB
   *
   * @param dataFrameWriter the DataFrameWriter save to MongoDB
   * @param writeConfig the writeConfig
   */
  def save(dataFrameWriter: DataFrameWriter[_], writeConfig: WriteConfig): Unit =
    dataFrameWriter.format(defaultSource).options(writeConfig.asOptions).save()

  /**
   * Creates a DataFrameReader with `MongoDB` as the source
   *
   * @param sparkSession the SparkSession
   * @return the DataFrameReader
   */
  def read(sparkSession: SparkSession): DataFrameReader = sparkSession.read.format("com.mongodb.spark.sql")

  /**
   * Creates a DataFrameWriter with the `MongoDB` underlying output data source.
   *
   * @param dataset the Dataset to convert into a DataFrameWriter
   * @return the DataFrameWriter
   */
  def write[T](dataset: Dataset[T]): DataFrameWriter[T] = dataset.write.format("com.mongodb.spark.sql")

  /**
   * Builder for configuring and creating a [[MongoSpark]]
   *
   * It requires a `SparkSession` or the `SparkContext`
   */
  class Builder {
    private var sparkSession: Option[SparkSession] = None
    private var connector: Option[MongoConnector] = None
    private var readConfig: Option[ReadConfig] = None
    private var pipeline: Seq[Bson] = Nil
    private var options: collection.Map[String, String] = Map()

    def build(): MongoSpark = {
      require(sparkSession.isDefined, "The SparkSession must be set, either explicitly or via the SparkContext")
      val session = sparkSession.get

      val mergedConf = if (readConfig.isDefined) ReadConfig(options, readConfig) else ReadConfig(session.sparkContext.getConf, options)
      val readConf = if (pipeline.isEmpty) mergedConf else mergedConf.withPipeline(pipeline)
      val mongoConnector = connector.getOrElse(MongoConnector(readConf))
      new MongoSpark(session, mongoConnector, readConf)
    }

    /**
     * Sets the SparkSession from the sparkContext
     *
     * @param sparkSession for the RDD
     */
    def sparkSession(sparkSession: SparkSession): Builder = {
      this.sparkSession = Option(sparkSession)
      this
    }

    /**
     * Sets the SparkSession from the sparkContext
     *
     * @param sparkContext for the RDD
     */
    def sparkContext(sparkContext: SparkContext): Builder = {
      this.sparkSession = Option(SparkSession.builder().config(sparkContext.getConf).getOrCreate())
      this
    }

    /**
     * Sets the SparkSession from the javaSparkContext
     *
     * @param javaSparkContext for the RDD
     */
    def javaSparkContext(javaSparkContext: JavaSparkContext): Builder = sparkContext(javaSparkContext.sc)

    /**
     * Sets the SparkSession
     *
     * @param sqlContext for the RDD
     */
    @deprecated("As of Spark 2.0 SQLContext was replaced by SparkSession. Use the SparkSession method instead", "2.0.0")
    def sqlContext(sqlContext: SQLContext): Builder = sparkSession(sqlContext.sparkSession)

    /**
     * Append a configuration option
     *
     * These options can be used to configure all aspects of how to connect to MongoDB
     *
     * @param key the configuration key
     * @param value the configuration value
     */
    def option(key: String, value: String): Builder = {
      this.options = this.options + (key -> value)
      this
    }

    /**
     * Set configuration options
     *
     * These options can configure all aspects of how to connect to MongoDB
     *
     * @param options the configuration options
     */
    def options(options: collection.Map[String, String]): Builder = {
      this.options = options
      this
    }

    /**
     * Set configuration options
     *
     * These options can configure all aspects of how to connect to MongoDB
     *
     * @param options the configuration options
     */
    def options(options: java.util.Map[String, String]): Builder = {
      this.options = options.asScala
      this
    }

    /**
     * Sets the [[com.mongodb.spark.MongoConnector]] to use
     *
     * @param connector the MongoConnector
     */
    def connector(connector: MongoConnector): Builder = {
      this.connector = Option(connector)
      this
    }

    /**
     * Sets the [[com.mongodb.spark.config.ReadConfig]] to use
     *
     * @param readConfig the readConfig
     */
    def readConfig(readConfig: ReadConfig): Builder = {
      this.readConfig = Option(readConfig)
      this
    }

    /**
     * Sets the aggregation pipeline to use
     *
     * @param pipeline the aggregation pipeline
     */
    def pipeline(pipeline: Seq[Bson]): Builder = {
      this.pipeline = pipeline
      this
    }
  }

  /*
   * Java API helpers
   */

  /**
   * Load data from MongoDB
   *
   * @param jsc the Spark context containing the MongoDB connection configuration
   * @return a MongoRDD
   */
  def load(jsc: JavaSparkContext): JavaMongoRDD[Document] = builder().javaSparkContext(jsc).build().toJavaRDD()

  /**
   * Load data from MongoDB
   *
   * @param jsc the Spark context containing the MongoDB connection configuration
   * @return a MongoRDD
   */
  def load(jsc: JavaSparkContext, readConfig: ReadConfig): JavaMongoRDD[Document] =
    builder().javaSparkContext(jsc).readConfig(readConfig).build().toJavaRDD()

  /**
   * Load data from MongoDB
   *
   * @param jsc the Spark context containing the MongoDB connection configuration
   * @param clazz   the class of the data contained in the RDD
   * @tparam D the type of the data in the RDD
   * @return a MongoRDD
   */
  def load[D](jsc: JavaSparkContext, readConfig: ReadConfig, clazz: Class[D]): JavaMongoRDD[D] =
    builder().javaSparkContext(jsc).readConfig(readConfig).build().toJavaRDD(clazz)

  /**
   * Load data from MongoDB
   *
   * @param jsc the Spark context containing the MongoDB connection configuration
   * @param clazz   the class of the data contained in the RDD
   * @tparam D the type of the data in the RDD
   * @return a MongoRDD
   */
  def load[D](jsc: JavaSparkContext, clazz: Class[D]): JavaMongoRDD[D] = builder().javaSparkContext(jsc).build().toJavaRDD(clazz)

  /**
   * Load data from MongoDB
   *
   * @param sparkSession the SparkSession containing the MongoDB connection configuration
   * @return the Dataset
   * @since 2.1
   */
  def loadAndInferSchema(sparkSession: SparkSession): DataFrame = builder().sparkSession(sparkSession).build().toDF()

  /**
   * Load data from MongoDB
   *
   * @param sparkSession the SparkSession containing the MongoDB connection configuration
   * @param readConfig the custom readConfig
   * @return the Dataset
   * @since 2.1
   */
  def loadAndInferSchema(sparkSession: SparkSession, readConfig: ReadConfig): DataFrame =
    builder().sparkSession(sparkSession).readConfig(readConfig).build().toDF()

  /**
   * Load data from MongoDB
   *
   * @param sparkSession the SparkSession containing the MongoDB connection configuration
   * @param clazz   the class of the data representing the Dataset
   * @tparam D The bean class defining the schema for the data
   * @return the Dataset
   * @since 2.1
   */
  def load[D](sparkSession: SparkSession, clazz: Class[D]): Dataset[D] = builder().sparkSession(sparkSession).build().toDS(clazz)

  /**
   * Load data from MongoDB
   *
   * @param sparkSession the SparkSession containing the MongoDB connection configuration
   * @param readConfig the custom readConfig
   * @param clazz   the class of the data contained in the RDD
   * @tparam D The bean class defining the schema for the data
   * @return the Dataset
   */
  def load[D](sparkSession: SparkSession, readConfig: ReadConfig, clazz: Class[D]): Dataset[D] =
    builder().sparkSession(sparkSession).readConfig(readConfig).build().toDS(clazz)

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
    save[D](javaRDD.rdd)
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
    save[D](javaRDD.rdd, writeConfig)
  }

  // Deprecated APIs
  /**
   * Load data from MongoDB
   *
   * @param sqlContext the SQLContext containing the MongoDB connection configuration
   * @tparam D The optional class defining the schema for the data
   * @return a MongoRDD
   */
  @deprecated("As of Spark 2.0 SQLContext was replaced by SparkSession. Use the SparkSession method instead", "2.0.0")
  def load[D <: Product: TypeTag](sqlContext: SQLContext): DataFrame = load[D](sqlContext.sparkSession)

  /**
   * Load data from MongoDB
   *
   * @param sqlContext the SQLContext containing the MongoDB connection configuration
   * @tparam D The optional class defining the schema for the data
   * @return a MongoRDD
   */
  @deprecated("As of Spark 2.0 SQLContext was replaced by SparkSession. Use the SparkSession method instead", "2.0.0")
  def load[D <: Product: TypeTag](sqlContext: SQLContext, readConfig: ReadConfig): DataFrame =
    load[D](sqlContext.sparkSession, readConfig)

  /**
   * Load data from MongoDB
   *
   * @param sqlContext the SQL context containing the MongoDB connection configuration
   * @param clazz   the class of the data contained in the RDD
   * @tparam D The bean class defining the schema for the data
   * @return a MongoRDD
   */
  @deprecated("As of Spark 2.0 SQLContext was replaced by SparkSession. Use the SparkSession method instead", "2.0.0")
  def load[D](sqlContext: SQLContext, readConfig: ReadConfig, clazz: Class[D]): DataFrame =
    builder().sparkSession(sqlContext.sparkSession).readConfig(readConfig).build().toDF(clazz)

  /**
   * Creates a DataFrameReader with `MongoDB` as the source
   *
   * @param sqlContext the SQLContext
   * @return the DataFrameReader
   */
  @deprecated("As of Spark 2.0 SQLContext was replaced by SparkSession. Use the SparkSession method instead", "2.0.0")
  def read(sqlContext: SQLContext): DataFrameReader = read(sqlContext.sparkSession)

}
// scalastyle:on number.of.methods

/**
 * The MongoSpark class
 *
 * '''Note:''' Creation of the class should be via [[MongoSpark$.builder]].
 *
 * @since 1.0
 */
case class MongoSpark(sparkSession: SparkSession, connector: MongoConnector, readConfig: ReadConfig) {

  private def rdd[D: ClassTag]()(implicit e: D DefaultsTo Document): MongoRDD[D] =
    new MongoRDD[D](sparkSession, sparkSession.sparkContext.broadcast(connector), readConfig)

  if (readConfig.registerSQLHelperFunctions) {
    helpers.UDF.registerFunctions(sparkSession)
  }

  /**
   * Creates a `RDD` for the collection
   *
   * @tparam D the datatype for the collection
   * @return a MongoRDD[D]
   */
  def toRDD[D: ClassTag]()(implicit e: D DefaultsTo Document): MongoRDD[D] = rdd[D]

  /**
   * Creates a `JavaRDD` for the collection
   *
   * @return a JavaMongoRDD[Document]
   */
  def toJavaRDD(): JavaMongoRDD[Document] = rdd[Document].toJavaRDD()

  /**
   * Creates a `JavaRDD` for the collection
   *
   * @param clazz   the class of the data contained in the RDD
   * @tparam D the type of the data in the RDD
   * @return the javaRDD
   */
  def toJavaRDD[D](clazz: Class[D]): JavaMongoRDD[D] = {
    implicit def ct: ClassTag[D] = ClassTag(clazz)
    rdd[D].toJavaRDD()
  }

  /**
   * Creates a `DataFrame` based on the schema derived from the optional type.
   *
   * '''Note:''' Prefer [[toDS[T<:Product]()*]] as computations will be more efficient.
   *  The rdd must contain an `_id` for MongoDB versions < 3.2.
   *
   * @tparam T The optional type of the data from MongoDB, if not provided the schema will be inferred from the collection
   * @return a DataFrame
   */
  def toDF[T <: Product: TypeTag](): DataFrame = {
    val schema: StructType = MongoInferSchema.reflectSchema[T]() match {
      case Some(reflectedSchema) => reflectedSchema
      case None                  => MongoInferSchema(toBsonDocumentRDD)
    }
    toDF(schema)
  }

  /**
   * Creates a `DataFrame` based on the schema derived from the bean class.
   *
   * '''Note:''' Prefer [[toDS[T](beanClass:Class[T])*]] as computations will be more efficient.
   *
   * @param beanClass encapsulating the data from MongoDB
   * @tparam T The bean class type to shape the data from MongoDB into
   * @return a DataFrame
   */
  def toDF[T](beanClass: Class[T]): DataFrame = toDF(MongoInferSchema.reflectSchema[T](beanClass))

  /**
   * Creates a `DataFrame` based on the provided schema.
   *
   * @param schema the schema representing the DataFrame.
   * @return a DataFrame.
   */
  def toDF(schema: StructType): DataFrame = {
    sparkSession.baseRelationToDataFrame(MongoRelation(toBsonDocumentRDD, Option(schema))(sparkSession.sqlContext).asInstanceOf[BaseRelation])
  }

  /**
   * Creates a `Dataset` from the collection strongly typed to the provided case class.
   *
   * @tparam T The type of the data from MongoDB
   * @return
   */
  def toDS[T <: Product: TypeTag: NotNothing](): Dataset[T] = {
    val dataFrame: DataFrame = toDF[T]()
    import dataFrame.sqlContext.implicits._
    dataFrame.as[T]
  }

  /**
   * Creates a `Dataset` from the RDD strongly typed to the provided java bean.
   *
   * @tparam T The type of the data from MongoDB
   * @return
   */
  def toDS[T](beanClass: Class[T]): Dataset[T] = toDF[T](beanClass).as(Encoders.bean(beanClass))

  private def toBsonDocumentRDD: MongoRDD[BsonDocument] = {
    MongoSpark.builder()
      .sparkSession(sparkSession)
      .connector(connector)
      .readConfig(readConfig)
      .build()
      .toRDD[BsonDocument]()
  }

}

