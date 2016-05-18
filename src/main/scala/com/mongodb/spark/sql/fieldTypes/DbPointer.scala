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

package com.mongodb.spark.sql.fieldTypes

import org.bson.BsonDbPointer

/**
 * The DbPointer companion object
 *
 * @since 1.0
 */
object DbPointer {
  /**
   * Create a new instance
   *
   * @param ref the namespace string
   * @param oid the ObjectId
   * @return a new instance
   */
  def apply(ref: String, oid: org.bson.types.ObjectId): DbPointer = new DbPointer(ref, oid.toHexString)
}

/**
 * A case class representing the Bson DbPointer type
 *
 * @param ref the namespace string
 * @param oid the ObjectId hexString
 * @since 1.0
 */
case class DbPointer(ref: String, oid: String) extends FieldType[BsonDbPointer] {
  lazy val underlying = new BsonDbPointer(ref, new org.bson.types.ObjectId(oid))
}
