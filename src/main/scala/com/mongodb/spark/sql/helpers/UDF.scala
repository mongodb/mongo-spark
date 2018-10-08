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

import javax.xml.bind.DatatypeConverter

import org.apache.spark.sql.SparkSession

import com.mongodb.spark.sql.fieldTypes

/**
 * The udf package provides User Defined Functions that can be used to support querying unsupported Bson Types in Spark.
 *
 * The unsupported types follow the [[http://docs.mongodb.com/manual/reference/mongodb-extended-json MongoDB Extended Json]] format.
 *
 * @see the [[StructFields]] helpers which convert unsupported bson types into `StructTypes` so that they can be queried
 * @since 1.1.0
 */
object UDF {

  /**
   * This method can be used as a user defined function to aid the querying binary values.
   *
   * Usage example:
   * {{{
   * sqlContext.udf.register("Binary", udf.binary _)
   * df.filter(s"binary = Binary('\$base64')")
   * }}}
   *
   * @param base64 the base 64 string that represents a binary value
   * @return a fieldType that can be used to query against
   */
  def binary(base64: String): fieldTypes.Binary = fieldTypes.Binary(DatatypeConverter.parseBase64Binary(base64))

  /**
   * This method can be used as a user defined function to aid the querying of binary values that contain a sub type.
   *
   * Usage example:
   * {{{
   * sqlContext.udf.register("BinaryWithSubType", udf.binaryWithSubType _)
   * df.filter(s"binaryWithSubType = BinaryWithSubType(\$subType, '\$base64')")
   * }}}
   *
   * @param subType the binary sub type
   * @param base64 the base 64 string that represents a binary value
   * @return a fieldType that can be used to query against
   */
  def binaryWithSubType(subType: Byte, base64: String): fieldTypes.Binary = fieldTypes.Binary(subType, DatatypeConverter.parseBase64Binary(base64))

  /**
   * This method can be used as a user defined function to aid the querying of DbPointer values.
   *
   * Usage example:
   * {{{
   * sqlContext.udf.register("DbPointer", udf.dbPointer _)
   * df.filter(s"dbPointer = DbPointer('\$ref', '\$oid')")
   * }}}
   *
   * @param ref the namespace string
   * @param oid the ObjectId string
   * @return a fieldType that can be used to query against
   */
  def dbPointer(ref: String, oid: String): fieldTypes.DbPointer = fieldTypes.DbPointer(ref, oid)

  /**
   * This method can be used as a user defined function to aid the querying of JavaScript values.
   *
   * Usage example:
   * {{{
   * sqlContext.udf.register("JSNoScope", udf.javaScript _)
   * df.filter(s"code = JSNoScope('\$code')")
   * }}}
   *
   * @param code the JavaScript code
   * @return a fieldType that can be used to query against
   */
  def javaScript(code: String): fieldTypes.JavaScript = fieldTypes.JavaScript(code)

  /**
   * This method can be used as a user defined function to aid the querying of JavaScript with scope values.
   *
   * Usage example:
   * {{{
   * sqlContext.udf.register("JavaScript", udf.javaScriptWithScope _)
   * df.filter(s"codeWithScope = JavaScript('\$code', '\$scope')")
   * }}}
   *
   * @param code the JavaScript code
   * @param scope the Json representation of the scope
   * @return a fieldType that can be used to query against
   */
  def javaScriptWithScope(code: String, scope: String): fieldTypes.JavaScriptWithScope = fieldTypes.JavaScriptWithScope(code, scope)

  /**
   * This method can be used as a user defined function to aid the querying of maxKey values.
   *
   * Usage example:
   * {{{
   * sqlContext.udf.register("maxKey", udf.maxKey _)
   * df.filter(s"maxKey = maxKey()")
   * }}}
   *
   * @return a fieldType that can be used to query against
   */
  def maxKey(): fieldTypes.MaxKey = fieldTypes.MaxKey()

  /**
   * This method can be used as a user defined function to aid the querying of minKey values.
   *
   * Usage example:
   * {{{
   * sqlContext.udf.register("minKey", udf.minKey _)
   * df.filter(s"minKey = minKey()")
   * }}}
   *
   * @return a fieldType that can be used to query against
   */
  def minKey(): fieldTypes.MinKey = fieldTypes.MinKey()

  /**
   * This method can be used as a user defined function to aid the querying of ObjectId values.
   *
   * Usage example:
   * {{{
   * sqlContext.udf.register("ObjectId", udf.objectId _)
   * df.filter(s"objectId = ObjectId('\$oid')")
   * }}}
   *
   * @param oid the ObjectId string
   * @return a fieldType that can be used to query against
   */
  def objectId(oid: String): fieldTypes.ObjectId = fieldTypes.ObjectId(oid)

  /**
   * This method can be used as a user defined function to aid the querying of regular expression values.
   *
   * Usage example:
   * {{{
   * sqlContext.udf.register("RegexNoOptions", udf.regularExpression _)
   * df.filter(s"regex = RegexNoOptions('\$regex')")
   * }}}
   *
   * @param regex the regular expression string
   * @return a fieldType that can be used to query against
   */
  def regularExpression(regex: String): fieldTypes.RegularExpression = fieldTypes.RegularExpression(regex, "")

  /**
   * This method can be used as a user defined function to aid the querying of regular expression values with options.
   *
   * Usage example:
   * {{{
   * sqlContext.udf.register("Regex", udf.regularExpressionWithOptions _)
   * df.filter(s"regexWithOptions = Regex('\$regex', '\$options')")
   * }}}
   *
   * @param regex the regular expression string
   * @param options the options
   * @return a fieldType that can be used to query against
   */
  def regularExpressionWithOptions(regex: String, options: String): fieldTypes.RegularExpression = fieldTypes.RegularExpression(regex, options)

  /**
   * This method can be used as a user defined function to aid the querying of Symbol values.
   *
   * Usage example:
   * {{{
   * sqlContext.udf.register("Symbol", udf.symbol _)
   * df.filter(s"symbol = Symbol('\$symbol')")
   * }}}
   *
   * @param symbol the symbol
   * @return a fieldType that can be used to query against
   */
  def symbol(symbol: String): fieldTypes.Symbol = fieldTypes.Symbol(symbol)

  /**
   * This method can be used as a user defined function to aid the querying of Timestamp values.
   *
   * Usage example:
   * {{{
   * sqlContext.udf.register("Timestamp", udf.timestamp _)
   * df.filter(s"timestamp = Timestamp(\$time, \$inc)")
   * }}}
   *
   * @param time the time in seconds since epoch
   * @param inc an incrementing ordinal for operations within a given second
   * @return a fieldType that can be used to query against
   */
  def timestamp(time: Int, inc: Int): fieldTypes.Timestamp = fieldTypes.Timestamp(time, inc)

  private[spark] def registerFunctions(sparkSession: SparkSession): Unit = {
    sparkSession.udf.register("Binary", UDF.binary _)
    sparkSession.udf.register("BinaryWithSubType", UDF.binaryWithSubType _)
    sparkSession.udf.register("DbPointer", UDF.dbPointer _)
    sparkSession.udf.register("JSNoScope", UDF.javaScript _)
    sparkSession.udf.register("JavaScript", UDF.javaScriptWithScope _)
    sparkSession.udf.register("maxKey", UDF.maxKey _)
    sparkSession.udf.register("minKey", UDF.minKey _)
    sparkSession.udf.register("ObjectId", UDF.objectId _)
    sparkSession.udf.register("RegexNoOptions", UDF.regularExpression _)
    sparkSession.udf.register("Regex", UDF.regularExpressionWithOptions _)
    sparkSession.udf.register("Symbol", UDF.symbol _)
    sparkSession.udf.register("Timestamp", UDF.timestamp _)
  }
}
