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

package com.mongodb.spark.rdd.api.java

import java.util

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.{DataFrame, Dataset}

import org.bson.conversions.Bson
import com.mongodb.spark.notNull
import com.mongodb.spark.rdd.MongoRDD

/**
 * Java specific version of [[com.mongodb.spark.rdd.MongoRDD]].
 *
 * @since 1.0
 */
case class JavaMongoRDD[D](override val rdd: MongoRDD[D])(implicit override val classTag: ClassTag[D]) extends JavaRDD[D](rdd)(classTag) {

  /**
   * Returns a copy with the specified aggregation pipeline
   *
   * @param pipeline the aggregation pipeline to use
   * @return the updated MongoJavaRDD
   */
  def withPipeline[B <: Bson](pipeline: util.List[B]): JavaMongoRDD[D] = {
    notNull("pipeline", pipeline)
    JavaMongoRDD(rdd.withPipeline(pipeline.asScala))
  }

  /**
   * Creates a `DataFrame` inferring the schema by sampling data from MongoDB.
   *
   * '''Note:''' Prefer [[toDS[T](beanClass:Class[T])*]] as any computations will be more efficient.
   *  The rdd must contain an `_id` for MongoDB versions < 3.2.
   *
   * @return a DataFrame
   */
  def toDF(): DataFrame = rdd.toDF()

  /**
   * Creates a `DataFrame` based on the schema derived from the bean class
   *
   * @param beanClass encapsulating the data from MongoDB
   * @tparam T The bean class type to shape the data from MongoDB into
   * @return a DataFrame
   */
  def toDF[T](beanClass: Class[T]): DataFrame = {
    notNull("beanClass", beanClass)
    rdd.toDF(beanClass)
  }

  /**
   * Creates a `Dataset` from the RDD strongly typed to the provided java bean.
   *
   * @param beanClass encapsulating the data from MongoDB
   * @tparam T The type of the data from MongoDB
   * @return a Dataset
   */
  def toDS[T](beanClass: Class[T]): Dataset[T] = {
    notNull("beanClass", beanClass)
    rdd.toDS(beanClass)
  }

}
