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

import org.bson.{BsonBinary, BsonBinarySubType}

/**
 * The Binary companion object
 *
 * @since 1.0
 */
object Binary {
  /**
   * Create a new instance
   * @param data the binary data
   * @return a new instance
   */
  def apply(data: Array[Byte]): Binary = new Binary(BsonBinarySubType.BINARY.getValue, data)
}

/**
 * A case class representing the Bson Binary type
 *
 * @param subType the binary sub type
 * @param data the data
 * @since 1.0
 */
case class Binary(subType: Byte, data: Array[Byte]) extends FieldType[BsonBinary] {
  lazy val underlying = new BsonBinary(subType, data)
}
