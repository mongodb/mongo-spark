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

package com.mongodb.spark.sql.helpers

import org.apache.spark.sql.types.{DataTypes, StructField}

import org.bson.BsonValue
import com.mongodb.spark.sql.types.BsonCompatibility
import com.mongodb.spark.sql.types.BsonCompatibility.CompatibilityBase

/**
 * The types package provides StructFields representing unsupported Bson Types in Spark.
 *
 * These types follow the [[http://docs.mongodb.com/manual/reference/mongodb-extended-json MongoDB Extended Json]] format.
 *
 * @since 1.1.0
 */
object StructFields {

  /**
   * Represents the Bson Binary type
   *
   * @see [[UDF.binary]] and [[UDF.binaryWithSubType]] for easy filtering of these types.
   * @param fieldName the name of the field
   * @param nullable  if the field is nullable
   * @return the StructField containing a Bson Binary value.
   */
  def binary(fieldName: String, nullable: Boolean): StructField = createStructField(fieldName, BsonCompatibility.Binary, nullable)

  /**
   * Represents the Bson DBPointer type
   *
   * @see [[UDF.dbPointer]] for easy filtering of these types.
   * @param fieldName the name of the field
   * @param nullable  if the field is nullable
   * @return the StructField containing a Bson DBPointer value.
   */
  def dbPointer(fieldName: String, nullable: Boolean): StructField = createStructField(fieldName, BsonCompatibility.DbPointer, nullable)

  /**
   * Represents the Bson JavaScript type
   *
   * @see [[UDF.javaScript]] for easy filtering of these types.
   * @param fieldName the name of the field
   * @param nullable  if the field is nullable
   * @return the StructField containing a Bson JavaScript value.
   */
  def javaScript(fieldName: String, nullable: Boolean): StructField = createStructField(fieldName, BsonCompatibility.JavaScript, nullable)

  /**
   * Represents the Bson JavaScript with Scope type
   *
   * @see [[UDF.javaScriptWithScope]] for easy filtering of these types.
   * @param fieldName the name of the field
   * @param nullable  if the field is nullable
   * @return the StructField containing a Bson JavaScript with Scope value.
   */
  def javaScriptWithScope(fieldName: String, nullable: Boolean): StructField =
    createStructField(fieldName, BsonCompatibility.JavaScriptWithScope, nullable)

  /**
   * Represents the Bson MaxKey type
   *
   * @see [[UDF.maxKey]] for easy filtering of these types.
   * @param fieldName the name of the field
   * @param nullable  if the field is nullable
   * @return the StructField containing a Bson MaxKey value.
   */
  def maxKey(fieldName: String, nullable: Boolean): StructField = createStructField(fieldName, BsonCompatibility.MaxKey, nullable)

  /**
   * Represents the Bson MinKey type
   *
   * @see [[UDF.minKey]] for easy filtering of these types.
   * @return the StructField containing a Bson MinKey value.
   */
  def minKey(fieldName: String, nullable: Boolean): StructField = createStructField(fieldName, BsonCompatibility.MinKey, nullable)

  /**
   * Represents the Bson ObjectId type
   *
   * @see [[UDF.objectId]] for easy filtering of these types.
   * @param fieldName the name of the field
   * @param nullable  if the field is nullable
   * @return the StructField containing a Bson ObjectId value.
   */
  def objectId(fieldName: String, nullable: Boolean): StructField = createStructField(fieldName, BsonCompatibility.ObjectId, nullable)

  /**
   * Represents the Bson RegularExpression type
   *
   * @see [[UDF.regularExpression]] for easy filtering of these types.
   * @param fieldName the name of the field
   * @param nullable  if the field is nullable
   * @return the StructField containing a Bson RegularExpression value.
   */
  def regularExpression(fieldName: String, nullable: Boolean): StructField = createStructField(fieldName, BsonCompatibility.RegularExpression, nullable)

  /**
   * Represents the Bson Symbol type
   *
   * @see [[UDF.symbol]] for easy filtering of these types.
   * @param fieldName the name of the field
   * @param nullable  if the field is nullable
   * @return the StructField containing a Bson Symbol value.
   */
  def symbol(fieldName: String, nullable: Boolean): StructField = createStructField(fieldName, BsonCompatibility.Symbol, nullable)

  /**
   * Represents the Bson Timestamp type
   *
   * @see [[UDF.timestamp]] for easy filtering of these types.
   * @param fieldName the name of the field
   * @param nullable  if the field is nullable
   * @return the StructField containing a Bson Timestamp value.
   */
  def timestamp(fieldName: String, nullable: Boolean): StructField = createStructField(fieldName, BsonCompatibility.Timestamp, nullable)

  private def createStructField[T <: BsonValue](fieldName: String, compat: CompatibilityBase[T], nullable: Boolean): StructField =
    DataTypes.createStructField(fieldName, DataTypes.createStructType(compat.fields.toArray), nullable)
}
