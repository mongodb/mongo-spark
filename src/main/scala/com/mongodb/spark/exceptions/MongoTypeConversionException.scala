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

package com.mongodb.spark.exceptions

import com.mongodb.MongoException

/**
 * A class for exceptions that come when trying to convert Bson Types into Spark DataTypes or vice versa.
 */
class MongoTypeConversionException(message: String, throwable: Throwable) extends MongoException(message, throwable) {

  def this(message: String) {
    this(message, null) // scalastyle:ignore
  }

}
