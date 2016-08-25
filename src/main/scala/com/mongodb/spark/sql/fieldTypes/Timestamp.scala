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

import java.util.Date

import org.bson.BsonTimestamp

/**
 * The Timestamp companion object
 *
 * @since 1.0
 */
object Timestamp {
  /**
   * Create a new instance
   *
   * @param date the date representing the seconds since epoch
   * @param inc an incrementing ordinal for operations within a given second
   * @return the new instance
   */
  def apply(date: Date, inc: Int): Timestamp = new Timestamp((date.getTime / 1000).toInt, inc)
  /**
   * Create a new instance
   *
   * @param date the date representing the seconds since epoch
   * @param inc an incrementing ordinal for operations within a given second
   * @return the new instance
   */
  def apply(date: java.sql.Date, inc: Int): Timestamp = new Timestamp((date.getTime / 1000).toInt, inc)
}

/**
 * A case class representing the Bson Timestamp type
 *
 * @param time the time in seconds since epoch
 * @param inc an incrementing ordinal for operations within a given second
 * @since 1.0
 */
case class Timestamp(time: Int, inc: Int) extends FieldType[BsonTimestamp] {
  lazy val underlying: BsonTimestamp = new BsonTimestamp(time, inc)
}
