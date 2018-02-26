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

import org.bson.BsonObjectId

/**
 * The ObjectId companion object
 *
 * @since 1.0
 */
object ObjectId {
  /**
   * Create a new instance
   *
   * @param oid the ObjectId
   * @return the new instance
   */
  def apply(oid: org.bson.types.ObjectId): ObjectId = new ObjectId(oid.toHexString)
}

/**
 * A case class representing the Bson ObjectId type
 *
 * @param oid the ObjectId hex string representation
 * @since 1.0
 */
case class ObjectId(oid: String) extends FieldType[BsonObjectId] {
  lazy val underlying: BsonObjectId = new BsonObjectId(new org.bson.types.ObjectId(oid))
}
