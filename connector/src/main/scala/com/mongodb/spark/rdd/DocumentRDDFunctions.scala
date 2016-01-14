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

import org.apache.spark.rdd.RDD

import org.bson.Document
import com.mongodb.spark.DefaultHelper.DefaultsTo
import com.mongodb.spark.{MongoConnector, classTagToClassOf}

/**
 * MongoDB Document RDD functions
 */
object DocumentRDDFunctions {

  /**
   * Creates the DocumentRDDFunctions instance
   *
   * @param rdd the rdd
   * @tparam D the type of data in the RDD
   * @return a DocumentRDDFunctions instance
   */
  def apply[D: ClassTag](rdd: RDD[D]): DocumentRDDFunctions[D] = apply(rdd, MongoConnector(rdd.context.getConf))

  /**
   * Creates the DocumentRDDFunctions instance
   * @param rdd the rdd
   *
   * @param collectionName the name of the collection to save data to
   * @tparam D the type of data in the RDD
   */
  def apply[D: ClassTag](rdd: RDD[D], collectionName: String): DocumentRDDFunctions[D] =
    apply(rdd, MongoConnector(rdd.context.getConf), collectionName)

  /**
   * Creates the DocumentRDDFunctions instance
   *
   * @param rdd the rdd
   * @param connector the [[MongoConnector]]
   * @tparam D the type of data in the RDD
   * @return a DocumentRDDFunctions instance
   */
  def apply[D: ClassTag](rdd: RDD[D], connector: MongoConnector): DocumentRDDFunctions[D] =
    apply(rdd, connector, connector.databaseName, connector.collectionName)

  /**
   * Functions for RDD's that allow the data to be saved to MongoDB.
   *
   * @param rdd the rdd
   * @param connector the [[MongoConnector]]
   * @param collectionName the name of the collection to save data to
   * @tparam D the type of data in the RDD
   */
  def apply[D: ClassTag](rdd: RDD[D], connector: MongoConnector, collectionName: String): DocumentRDDFunctions[D] =
    new DocumentRDDFunctions[D](rdd, connector, connector.databaseName, collectionName)

}

/**
 * Functions for RDD's that allow the data to be saved to MongoDB.
 *
 * @param rdd the rdd
 * @param connector the [[MongoConnector]]
 * @param databaseName the name of the database to save the data to
 * @param collectionName the name of the collection to save data to
 * @param e the implicit datatype of the rdd
 * @param ct the implicit ClassTag of the datatype of the rdd
 * @tparam D the type of data in the RDD
 */
case class DocumentRDDFunctions[D](rdd: RDD[D], connector: MongoConnector, databaseName: String,
                                   collectionName: String)(implicit e: D DefaultsTo Document, ct: ClassTag[D]) {

  /**
   * Saves the RDD data to MongoDB
   *
   * @return the RDD
   */
  def saveToMongoDB(): RDD[D] = {
    rdd.foreachPartition(iter => if (iter.nonEmpty) {
      connector.getMongoClient().getDatabase(databaseName).getCollection(collectionName, ct).insertMany(iter.toList.asJava)
    })
    rdd
  }

}
