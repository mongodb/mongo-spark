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

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.Try
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.bson.conversions.Bson
import org.bson.{BsonDocument, Document}
import com.mongodb.{MongoClient, MongoCursorNotFoundException}
import com.mongodb.client.{AggregateIterable, MongoCursor}
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.exceptions.MongoSparkCursorNotFoundException
import com.mongodb.spark.rdd.api.java.JavaMongoRDD
import com.mongodb.spark.rdd.partitioner.{MongoPartition, MongoSinglePartitioner}
import com.mongodb.spark.{MongoConnector, MongoSpark, NotNothing, classTagToClassOf}

import scala.collection.Iterator

/**
 * MongoRDD Class
 *
 * @param connector the [[com.mongodb.spark.MongoConnector]]
 * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
 * @tparam D the type of the collection documents
 */
class MongoRDD[D: ClassTag](
    @transient val sparkSession:   SparkSession,
    private[spark] val connector:  Broadcast[MongoConnector],
    private[spark] val readConfig: ReadConfig
) extends RDD[D](sparkSession.sparkContext, Nil) {

  @transient val sc: SparkContext = sparkSession.sparkContext
  private def mongoSpark = {
    checkSparkContext()
    MongoSpark(sparkSession, connector.value, readConfig)
  }

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
   * Creates a `DataFrame` based on the provided schema.
   *
   * @param schema the schema representing the DataFrame.
   * @return a DataFrame.
   */
  def toDF(schema: StructType): DataFrame = mongoSpark.toDF(schema)

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
  def withPipeline[B <: Bson](pipeline: Seq[B]): MongoRDD[D] = copy(readConfig = readConfig.withPipeline(pipeline))

  /**
   * Allows to copying of this RDD with changing some of the properties
   */
  def copy(
    connector:  Broadcast[MongoConnector] = connector,
    readConfig: ReadConfig                = readConfig
  ): MongoRDD[D] = {
    checkSparkContext()
    new MongoRDD[D](
      sparkSession = sparkSession,
      connector = connector,
      readConfig = readConfig
    )
  }

  override protected def getPartitions: Array[Partition] = {
    checkSparkContext()
    try {
      val partitions = readConfig.partitioner.partitions(connector.value, readConfig, readConfig.pipeline.toArray)
      logDebug(s"Created partitions: ${partitions.toList}")
      partitions.asInstanceOf[Array[Partition]]
    } catch {
      case t: Throwable =>
        logError(
          s"""
             |-----------------------------
             |WARNING: Partitioning failed.
             |-----------------------------
             |
            |Partitioning using the '${readConfig.partitioner.getClass.getSimpleName}' failed.
             |
            |Please check the stacktrace to determine the cause of the failure or check the Partitioner API documentation.
             |Note: Not all partitioners are suitable for all toplogies and not all partitioners support views.%n
             |
            |-----------------------------
             |""".stripMargin
        )
        throw t
    }

  }

  override def compute(split: Partition, context: TaskContext): Iterator[D] = {
    val client = connector.value.acquireClient()
    val cursor = getCursor(client, split.asInstanceOf[MongoPartition])
    context.addTaskCompletionListener((ctx: TaskContext) => {
      logDebug("Task completed closing the MongoDB cursor")
      Try(cursor.close())
      connector.value.releaseClient(client)
    })
    MongoCursorIterator(cursor)
  }

  /**
   * Retrieves the partition's data from the collection based on the bounds of the partition.
   *
   * @return the cursor
   */
  private def getCursor(client: MongoClient, partition: MongoPartition)(implicit ct: ClassTag[D]): MongoCursor[D] = {
    val partitionPipeline: Seq[BsonDocument] = if (partition.queryBounds.isEmpty) {
      readConfig.pipeline
    } else {
      new BsonDocument("$match", partition.queryBounds) +: readConfig.pipeline
    }

    logDebug(s"Creating cursor for partition #${partition.index}. pipeline = ${partitionPipeline.map(_.toJson).mkString("[", ", ", "]")}")
    val aggregateIterable: AggregateIterable[D] = client.getDatabase(readConfig.databaseName)
      .getCollection[D](readConfig.collectionName, classTagToClassOf(ct))
      .withReadConcern(readConfig.readConcern)
      .withReadPreference(readConfig.readPreference)
      .aggregate(partitionPipeline.asJava)

    readConfig.aggregationConfig.hint.map(aggregateIterable.hint)
    readConfig.aggregationConfig.collation.map(aggregateIterable.collation)
    aggregateIterable.allowDiskUse(readConfig.aggregationConfig.allowDiskUse)
    readConfig.batchSize.map(s => aggregateIterable.batchSize(s))
    aggregateIterable.iterator
  }

  private case class MongoCursorIterator(cursor: MongoCursor[D]) extends Iterator[D] {
    override def hasNext: Boolean = try {
      cursor.hasNext
    } catch {
      case e: MongoCursorNotFoundException => throw new MongoSparkCursorNotFoundException(e)
    }

    override def next(): D = try {
      cursor.next()
    } catch {
      case e: MongoCursorNotFoundException => throw new MongoSparkCursorNotFoundException(e)
    }
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

  private[spark] lazy val hasSampleAggregateOperator: Boolean = connector.value.hasSampleAggregateOperator(readConfig)

  private[spark] def appendPipeline[B <: Bson](extraPipeline: Seq[B]): MongoRDD[D] = withPipeline(readConfig.pipeline ++ extraPipeline)
}
