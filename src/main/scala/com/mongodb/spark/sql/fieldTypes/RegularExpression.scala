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

import java.util.regex.Pattern
import scala.util.matching.Regex

import org.bson.BsonRegularExpression

/**
 * The RegularExpression companion object
 *
 * @since 1.0
 */
object RegularExpression {
  /**
   * Create a new instance
   *
   * @param regex the regular expression
   * @return the new instance
   */
  def apply(regex: Pattern): RegularExpression = {
    new RegularExpression(regex.pattern(), getOptionsAsString(regex))
  }

  /**
   * Create a new instance
   *
   * @param regex the regular expression
   * @return the new instance
   */
  def apply(regex: Regex): RegularExpression = this(regex.pattern)

  private def getOptionsAsString(pattern: Pattern): String = {
    val GLOBAL_FLAG: Int = 256
    val regexFlags = Seq(
      (Pattern.CANON_EQ, 'c'),
      (Pattern.UNIX_LINES, 'd'),
      (GLOBAL_FLAG, 'g'),
      (Pattern.CASE_INSENSITIVE, 'i'),
      (Pattern.MULTILINE, 'm'),
      (Pattern.DOTALL, 's'),
      (Pattern.LITERAL, 't'),
      (Pattern.UNICODE_CASE, 'u'),
      (Pattern.COMMENTS, 'x')
    )
    val buf: StringBuilder = new StringBuilder
    val (flags, options) = regexFlags.foldLeft((pattern.flags, ""))({
      case ((remainingFlags, opts), (flag, chr)) =>
        (pattern.flags & flag) > 0 match {
          case true  => (remainingFlags - flag, opts + chr)
          case false => (remainingFlags, opts)
        }
    })

    flags == 0 match {
      case true  => options
      case false => throw new IllegalArgumentException("Some flags could not be recognized.")
    }
  }
}

/**
 * A case class representing the Bson RegularExpression type
 *
 * @param regex the regular expression
 * @param options the options
 * @since 1.0
 */
case class RegularExpression(regex: String, options: String) extends FieldType[BsonRegularExpression] {
  lazy val underlying: BsonRegularExpression = new BsonRegularExpression(regex, options)
}
