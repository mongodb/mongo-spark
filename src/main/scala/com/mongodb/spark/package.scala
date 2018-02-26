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
package com.mongodb

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.bson.Document
import com.mongodb.spark.DefaultHelper.DefaultsTo
import com.mongodb.spark.annotation.DeveloperApi
import com.mongodb.spark.rdd.DocumentRDDFunctions

/**
 * The MongoDB Spark Connector
 *
 * @since 1.0
 */
package object spark {

  /**
   * :: DeveloperApi ::
   *
   * Helper to get the class from a classTag
   *
   * @param ct the classTag we want to implicitly get the class of
   * @tparam C the class type
   * @return the classOf[C]
   */
  @DeveloperApi
  implicit def classTagToClassOf[C](ct: ClassTag[C]): Class[C] = ct.runtimeClass.asInstanceOf[Class[C]]

  /**
   * :: DeveloperApi ::
   *
   * Helper to implicitly add MongoDB based functions to a SparkContext
   *
   * @param sc the current SparkContext
   * @return the MongoDB based Spark Context
   */
  @DeveloperApi
  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions = SparkContextFunctions(sc)

  /**
   * :: DeveloperApi ::
   *
   * Helper to implicitly add MongoDB based functions to a SparkContext
   *
   * @param rdd the RDD to save to MongoDB
   * @return the MongoDB based Spark Context
   */
  @DeveloperApi
  implicit def toDocumentRDDFunctions[D](rdd: RDD[D])(implicit e: D DefaultsTo Document, ct: ClassTag[D]): DocumentRDDFunctions[D] =
    DocumentRDDFunctions(rdd)

  private[spark] def notNull[T](name: String, value: T): Unit = require(Option(value).isDefined, s"$name cannot be null")
}
