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

import org.apache.spark.sql.types.DataType

/**
 * A type marking fields in the document structure to schema translation process that should be skipped.
 *
 * The main example use case is skipping a field with an empty array as the value.
 */
private[sql] class SkipFieldType private () extends DataType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "SkipFieldType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  override def defaultSize: Int = 1

  override def asNullable: SkipFieldType = this
}

private[sql] case object SkipFieldType extends SkipFieldType
