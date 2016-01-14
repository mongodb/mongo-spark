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

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.apache.spark.api.java.JavaRDD

import org.bson.conversions.Bson

case class MongoJavaRDD[D](override val rdd: MongoRDD[D])(implicit override val classTag: ClassTag[D]) extends JavaRDD[D](rdd)(classTag) {

  /**
   * Returns a copy with the specified aggregation pipeline
   *
   * @param pipeline the aggregation pipeline to use
   * @return the updated MongoJavaRDD
   */
  def withPipeline[B <: Bson](pipeline: java.util.List[B]): MongoJavaRDD[D] = {
    MongoJavaRDD(rdd.withPipeline(pipeline.toList))
  }

}
