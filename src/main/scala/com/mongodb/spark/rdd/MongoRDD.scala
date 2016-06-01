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

package com.mongodb.spark.rdd

import java.util

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.Try

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}

import org.bson.conversions.Bson
import org.bson.{BsonDocument, Document}
import com.mongodb.MongoClient
import com.mongodb.client.MongoCursor
import com.mongodb.connection.ServerVersion
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.rdd.api.java.JavaMongoRDD
import com.mongodb.spark.rdd.partitioner.{MongoPartition, MongoPartitioner}
import com.mongodb.spark.{MongoConnector, MongoSpark, NotNothing, classTagToClassOf}

/**
 * MongoRDD Class
 *
 * @param connector the [[com.mongodb.spark.MongoConnector]]
 * @param mongoPartitioner the [[com.mongodb.spark.rdd.partitioner.MongoPartitioner]]
 * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
 * @param pipeline aggregate pipeline
 * @tparam D the type of the collection documents
 */
class MongoRDD[D: ClassTag](
    @transient val sqlContext:           SQLContext,
    private[spark] val connector:        Broadcast[MongoConnector],
    private[spark] val mongoPartitioner: MongoPartitioner,
    private[spark] val readConfig:       ReadConfig,
    private[spark] val pipeline:         Seq[Bson]
) extends RDD[D](sqlContext.sparkContext, Nil) {

  @transient val sc: SparkContext = sqlContext.sparkContext
  private def mongoSpark = MongoSpark(sqlContext, mongoPartitioner, connector.value, readConfig, pipeline)

  override def toJavaRDD(): JavaMongoRDD[D] = JavaMongoRDD(this)

  override def getPreferredLocations(split: Partition): Seq[String] = split.asInstanceOf[MongoPartition].locations

  /**
   * Creates a `DataFrame` based on the schema derived from the optional type.
   *
   * '''Note:''' Prefer [[toDS[T<:Product]()*]] as computations will be more efficient.
   *  The rdd must contain an `_id` for MongoDB versions < 3.2.
   *
   * @tparam T The optional type of the data from MongoDB, if not provided the schema will be inferred from the collection
   * @return a DataFrame
   */
  def toDF[T <: Product: TypeTag](): DataFrame = mongoSpark.toDF[T]()

  /**
   * Creates a `DataFrame` based on the schema derived from the bean class.
   *
   * '''Note:''' Prefer [[toDS[T](beanClass:Class[T])*]] as computations will be more efficient.
   *
   * @param beanClass encapsulating the data from MongoDB
   * @tparam T The bean class type to shape the data from MongoDB into
   * @return a DataFrame
   */
  def toDF[T](beanClass: Class[T]): DataFrame = mongoSpark.toDF(beanClass)

  /**
   * Creates a `Dataset` from the collection strongly typed to the provided case class.
   *
   * @tparam T The type of the data from MongoDB
   * @return
   */
  def toDS[T <: Product: TypeTag: NotNothing](): Dataset[T] = mongoSpark.toDS[T]()

  /**
   * Creates a `Dataset` from the RDD strongly typed to the provided java bean.
   *
   * @tparam T The type of the data from MongoDB
   * @return
   */
  def toDS[T](beanClass: Class[T]): Dataset[T] = mongoSpark.toDS[T](beanClass)

  /**
   * Returns a copy with the specified aggregation pipeline
   *
   * @param pipeline the aggregation pipeline to use
   * @return the updated MongoRDD
   */
  def withPipeline[B <: Bson](pipeline: Seq[B]): MongoRDD[D] = copy(pipeline = pipeline)

  /**
   * Allows to copying of this RDD with changing some of the properties
   */
  def copy(
    connector:        Broadcast[MongoConnector] = connector,
    mongoPartitioner: MongoPartitioner          = mongoPartitioner,
    readConfig:       ReadConfig                = readConfig,
    pipeline:         Seq[Bson]                 = pipeline
  ): MongoRDD[D] = {
    checkSparkContext()
    new MongoRDD[D](
      sqlContext = sqlContext,
      connector = connector,
      mongoPartitioner = mongoPartitioner,
      readConfig = readConfig,
      pipeline = pipeline.map(x => x.toBsonDocument(classOf[Document], connector.value.codecRegistry)) // Convert to serializable BsonDocuments
    )
  }

  override protected def getPartitions: Array[Partition] = {
    checkSparkContext()
    mongoPartitioner.partitions(connector.value, readConfig).asInstanceOf[Array[Partition]]
  }

  override def compute(split: Partition, context: TaskContext): Iterator[D] = {
    val client = connector.value.acquireClient()
    val cursor = getCursor(client, split.asInstanceOf[MongoPartition])
    context.addTaskCompletionListener((ctx: TaskContext) => {
      log.debug("Task completed closing the MongoDB cursor")
      Try(cursor.close())
      connector.value.releaseClient(client)
    })
    cursor.asScala
  }

  /**
   * Retrieves the partition's data from the collection based on the bounds of the partition.
   *
   * @return the cursor
   */
  private def getCursor(client: MongoClient, partition: MongoPartition)(implicit ct: ClassTag[D]): MongoCursor[D] = {
    val partitionPipeline: Seq[Bson] = new BsonDocument("$match", partition.queryBounds) +: pipeline
    client.getDatabase(readConfig.databaseName)
      .getCollection[D](readConfig.collectionName, classTagToClassOf(ct))
      .withReadConcern(readConfig.readConcern)
      .withReadPreference(readConfig.readPreference)
      .aggregate(partitionPipeline.asJava)
      .iterator
  }

  private def checkSparkContext(): Unit = {
    require(
      Option(sc).isDefined,
      """RDD transformation requires a non-null SparkContext.
        |Unfortunately SparkContext in this MongoRDD is null.
        |This can happen after MongoRDD has been deserialized.
        |SparkContext is not Serializable, therefore it deserializes to null.
        |RDD transformations are not allowed inside lambdas used in other RDD transformations.""".stripMargin
    )
  }

  private[spark] lazy val hasSampleAggregateOperator: Boolean = {
    val buildInfo: BsonDocument = connector.value.withDatabaseDo(
      readConfig.copy(databaseName = "test"),
      { db => db.runCommand(new Document("buildInfo", 1), classOf[BsonDocument]) }
    )
    val versionArray: util.List[Integer] = buildInfo.getArray("versionArray").asScala.take(3).map(_.asInt32().getValue.asInstanceOf[Integer]).asJava
    new ServerVersion(versionArray).compareTo(new ServerVersion(3, 2)) >= 0
  }

  private[spark] def appendPipeline[B <: Bson](extraPipeline: Seq[B]): MongoRDD[D] = withPipeline(pipeline ++ extraPipeline)
}
