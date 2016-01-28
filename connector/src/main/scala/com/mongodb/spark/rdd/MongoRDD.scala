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

import scala.reflect.ClassTag
import scala.collection.JavaConverters._

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import org.bson.{BsonDocument, Document}
import org.bson.codecs.configuration.CodecRegistry
import org.bson.conversions.Bson
import com.mongodb.client.MongoCursor
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.conf.ReadConfig
import com.mongodb.spark.rdd.api.java.JavaMongoRDD
import com.mongodb.spark.rdd.partitioner.{MongoPartition, MongoSplitKeyPartitioner, MongoPartitioner}

/**
 * The MongoRDD companion object
 *
 * @since 1.0
 */
object MongoRDD {

  /**
   * Creates a MongoRDD
   *
   * @param sc the Spark context
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def apply[D: ClassTag](sc: SparkContext): MongoRDD[D] = apply(sc, MongoConnector(sc.getConf))

  /**
   * Creates a MongoRDD
   *
   * @param sc the Spark context
   * @param readConfig the [[com.mongodb.spark.conf.ReadConfig]]
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def apply[D: ClassTag](sc: SparkContext, readConfig: ReadConfig): MongoRDD[D] = apply(sc, MongoConnector(sc.getConf), readConfig)

  /**
   * Creates a MongoRDD
   *
   * @param sc the Spark context
   * @param connector the [[com.mongodb.spark.MongoConnector]]
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def apply[D: ClassTag](sc: SparkContext, connector: MongoConnector): MongoRDD[D] = apply(sc, connector, ReadConfig(sc.getConf))

  /**
   * Creates a MongoRDD
   *
   * @param sc the Spark context
   * @param connector the [[com.mongodb.spark.MongoConnector]]
   * @param readConfig the [[com.mongodb.spark.conf.ReadConfig]]
   * @param pipeline optional aggregate pipeline
   * @tparam D the type of Document to return
   * @return a MongoRDD
   */
  def apply[D: ClassTag](sc: SparkContext, connector: MongoConnector, readConfig: ReadConfig, pipeline: Seq[Bson] = Seq()): MongoRDD[D] = {
    val sharedConnector: Broadcast[MongoConnector] = sc.broadcast(connector)
    new MongoRDD[D](sc, sharedConnector, MongoSplitKeyPartitioner(readConfig), readConfig, pipeline)
  }

}

/**
 * MongoRDD Class
 *
 * @param sc the Spark context
 * @param connector the [[com.mongodb.spark.MongoConnector]]
 * @param mongoPartitioner the [[com.mongodb.spark.rdd.partitioner.MongoPartitioner]]
 * @param readConfig the [[com.mongodb.spark.conf.ReadConfig]]
 * @param pipeline aggregate pipeline
 * @tparam D the type of the collection documents
 */
class MongoRDD[D: ClassTag](
    @transient val sc:                   SparkContext,
    private[spark] val connector:        Broadcast[MongoConnector],
    private[spark] val mongoPartitioner: MongoPartitioner,
    private[spark] val readConfig:       ReadConfig,
    private[spark] val pipeline:         Seq[Bson]
) extends RDD[D](sc, Nil) {

  override def toJavaRDD(): JavaMongoRDD[D] = JavaMongoRDD(this)

  /**
   * Returns a copy with the specified aggregation pipeline
   *
   * @param pipeline the aggregation pipeline to use
   * @return the updated MongoRDD
   */
  def withPipeline[B <: Bson](pipeline: Seq[B]): MongoRDD[D] = {
    val codecRegistry: CodecRegistry = connector.value.getMongoClient().getMongoClientOptions.getCodecRegistry
    copy(pipeline = pipeline.map(x => x.toBsonDocument(classOf[Document], codecRegistry))) // Convert to serializable BsonDocuments
  }

  /**
   * Allows to copying of this RDD with changing some of the properties
   */
  protected def copy(
    connector:        Broadcast[MongoConnector] = connector,
    mongoPartitioner: MongoPartitioner          = mongoPartitioner,
    readConfig:       ReadConfig                = readConfig,
    pipeline:         Seq[Bson]                 = pipeline
  ): MongoRDD[D] = {
    checkSparkContext()
    new MongoRDD[D](
      sc = sc,
      connector = connector,
      mongoPartitioner = mongoPartitioner,
      readConfig = readConfig,
      pipeline = pipeline
    )
  }

  override protected def getPartitions: Array[Partition] = {
    checkSparkContext()
    mongoPartitioner.partitions(connector.value)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[D] = {
    val cursor: MongoCursor[D] = getCursor(split.asInstanceOf[MongoPartition])
    context.addTaskCompletionListener((ctx: TaskContext) => {
      log.debug("Task completed closing the MongoDB cursor")
      cursor.close()
    })
    cursor.asScala
  }

  /**
   * Retrieves the partition's data from the collection based on the bounds of the partition.
   *
   * @return the cursor
   */
  private def getCursor(partition: MongoPartition): MongoCursor[D] = {
    val partitionPipeline: Seq[Bson] = new BsonDocument("$match", partition.queryBounds) +: pipeline
    connector.value.collection[D](readConfig.databaseName, readConfig.collectionName).aggregate(partitionPipeline.asJava).iterator
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

}
