/*
 * Copyright 2018 MongoDB, Inc.
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

import com.mongodb.{MongoCursorNotFoundException, MongoException}

/**
 * A class for exceptions if a cursor is not found.
 */
class MongoSparkCursorNotFoundException(message: String, throwable: MongoCursorNotFoundException) extends MongoException(message, throwable) {

  def this(originalError: MongoCursorNotFoundException) {
    this(
      s"""Cursor not found / no longer available.
         |
         |By default cursors timeout after 10 minutes and are cleaned up by MongoDB.
         |To prevent cursors being removed before being used, ensure that all data is read from MongoDB into Spark.
         |The best time to do this is before entering expensive / slow computations or merges.
         |
         |See: https://docs.mongodb.com/manual/reference/parameters/#param.cursorTimeoutMillis for information about changing the default timeout.
         |
         |Original error message: ${originalError.getErrorMessage}
        """.stripMargin, originalError
    )
  }

}
