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

package com.mongodb.spark.sql.types

import java.io.Serializable

import org.apache.spark.sql.types.DataType

/**
 * A type marking fields in the document structure to schema translation process that should be skipped.
 *
 * The main example use case is skipping a field with an empty array as the value.
 */
class SkipFieldType private () extends DataType with Serializable {
  def defaultSize: Int = 0

  def asNullable: DataType = this
  override def toString: String = "SkipFieldType"
}

case object SkipFieldType extends SkipFieldType
