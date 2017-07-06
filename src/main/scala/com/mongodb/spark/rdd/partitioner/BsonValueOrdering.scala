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

package com.mongodb.spark.rdd.partitioner

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import org.bson._
import com.mongodb.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 *
 * Ordering implement for BsonValues
 *
 * @since 1.0
 */
@DeveloperApi
trait BsonValueOrdering extends Ordering[BsonValue] {

  // scalastyle:off cyclomatic.complexity
  private val bsonTypeComparisonMap: Map[BsonType, Int] = Map(
    BsonType.MIN_KEY -> 1,
    BsonType.NULL -> 2,
    BsonType.INT32 -> 3,
    BsonType.INT64 -> 3,
    BsonType.DOUBLE -> 3,
    BsonType.DECIMAL128 -> 3,
    BsonType.SYMBOL -> 4,
    BsonType.STRING -> 4,
    BsonType.DOCUMENT -> 5,
    BsonType.ARRAY -> 6,
    BsonType.BINARY -> 7,
    BsonType.OBJECT_ID -> 8,
    BsonType.BOOLEAN -> 9,
    BsonType.DATE_TIME -> 10,
    BsonType.TIMESTAMP -> 11,
    BsonType.REGULAR_EXPRESSION -> 12,
    BsonType.MAX_KEY -> 13
  )

  /**
   *
   * Returns an integer whose sign communicates how x compares to y.
   *
   * The result sign has the following meaning:
   *
   * - negative if x < y
   * - positive if x > y
   * - zero otherwise (if x == y)
   *
   * When comparing values of different BSON types, MongoDB uses the following comparison order, from lowest to highest:
   *
   * 1. MinKey (internal type)
   * 2. Null
   * 3. Numbers (ints, longs, doubles, decimals)
   * 4. Symbol, String
   * 5. Object
   * 6. Array
   * 7. BinData
   * 8. ObjectId
   * 9. Boolean
   * 10. Date
   * 11. Timestamp
   * 12. Regular Expression
   * 13. MaxKey (internal type)
   */
  override def compare(x: BsonValue, y: BsonValue): Int = {
    (x.getBsonType, y.getBsonType) match {
      case (BsonType.MIN_KEY, BsonType.MIN_KEY)     => 0
      case (BsonType.NULL, BsonType.NULL)           => 0
      case (isBsonNumber(), isBsonNumber())         => compareNumbers(x, y)
      case (isString(), isString())                 => compareStrings(x, y)
      case (BsonType.DOCUMENT, BsonType.DOCUMENT)   => compareDocuments(x.asDocument, y.asDocument)
      case (BsonType.ARRAY, BsonType.ARRAY)         => compareArrays(x.asArray, y.asArray)
      case (BsonType.BINARY, BsonType.BINARY)       => compareBinary(x.asBinary, y.asBinary)
      case (BsonType.OBJECT_ID, BsonType.OBJECT_ID) => x.asObjectId.getValue.compareTo(y.asObjectId.getValue)
      case (BsonType.BOOLEAN, BsonType.BOOLEAN)     => x.asBoolean.getValue.compareTo(y.asBoolean().getValue)
      case (BsonType.DATE_TIME, BsonType.DATE_TIME) => x.asDateTime.getValue.compareTo(y.asDateTime().getValue)
      case (BsonType.TIMESTAMP, BsonType.TIMESTAMP) => compareTimestamps(x.asTimestamp, y.asTimestamp)
      case (BsonType.REGULAR_EXPRESSION, BsonType.REGULAR_EXPRESSION) =>
        compareRegularExpressions(x.asRegularExpression, y.asRegularExpression)
      case (xType, yType) =>
        val xSortScore = bsonTypeComparisonMap.getOrElse(x.getBsonType, 14) // scalastyle:ignore
        val ySortScore = bsonTypeComparisonMap.getOrElse(y.getBsonType, 14) // scalastyle:ignore
        xSortScore.compareTo(ySortScore)
    }
  }

  private def compareDocuments(x: BsonDocument, y: BsonDocument): Int = {
    val xKeyValues: Seq[(String, BsonValue)] = documentKeyValues(x)
    val yKeyValues: Seq[(String, BsonValue)] = documentKeyValues(y)

    compareKeyValues(xKeyValues zip yKeyValues) match {
      case 0 => xKeyValues.length.compareTo(yKeyValues.length)
      case v => v
    }
  }

  private def compareArrays(x: BsonArray, y: BsonArray): Int = {
    compareBsonValues(x.getValues.asScala zip y.getValues.asScala) match {
      case 0 => x.getValues.size.compareTo(y.getValues.size)
      case v => v
    }
  }

  private def compareRegularExpressions(x: BsonRegularExpression, y: BsonRegularExpression): Int = {
    x.getPattern.compareTo(y.getPattern) match {
      case 0 => x.getOptions.compareTo(y.getOptions)
      case v => v
    }
  }

  private def compareBinary(x: BsonBinary, y: BsonBinary): Int = {
    x.getData.length.compareTo(y.getData.length) match {
      case 0 =>
        x.getType.compareTo(y.getType) match {
          case 0 => compareBytes(x.getData zip y.getData)
          case v => v
        }
      case v => v
    }
  }

  private def compareTimestamps(x: BsonTimestamp, y: BsonTimestamp): Int = {
    x.getTime.compareTo(y.getTime) match {
      case 0 => x.getInc.compareTo(y.getInc)
      case v => v
    }
  }

  @tailrec
  private def compareKeyValues(kvs: Seq[((String, BsonValue), (String, BsonValue))]): Int = {
    kvs.headOption match {
      case Some(kv) => compareKeyValues(kv._1, kv._2) match {
        case 0 => compareKeyValues(kvs.tail)
        case v => v
      }
      case None => 0
    }
  }

  private def compareKeyValues(x: (String, BsonValue), y: (String, BsonValue)): Int = {
    x._1.compareTo(y._1) match {
      case 0 => compare(x._2, y._2)
      case v => v
    }
  }

  @tailrec
  private def compareBsonValues(values: Seq[(BsonValue, BsonValue)]): Int = {
    values.headOption match {
      case Some(value) => compare(value._1, value._2) match {
        case 0 => compareBsonValues(values.tail)
        case v => v
      }
      case None => 0
    }
  }

  @tailrec
  private def compareBytes(values: Seq[(Byte, Byte)]): Int = {
    values.headOption match {
      case Some(value) => value._1.compareTo(value._2) match {
        case 0 => compareBytes(values.tail)
        case v => v
      }
      case None => 0
    }
  }

  private def compareStrings(x: BsonValue, y: BsonValue): Int = {
    val (xString, yString) = (x.getBsonType, y.getBsonType) match {
      case (BsonType.STRING, BsonType.STRING) => (x.asString.getValue, y.asString.getValue)
      case (BsonType.SYMBOL, BsonType.STRING) => (x.asSymbol.getSymbol, y.asString.getValue)
      case (BsonType.STRING, BsonType.SYMBOL) => (x.asString.getValue, y.asSymbol.getSymbol)
      case (BsonType.SYMBOL, BsonType.SYMBOL) => (x.asSymbol.getSymbol, y.asSymbol.getSymbol)
      case _                                  => throw new UnsupportedOperationException(s"Cannot compare $x with $y. Not string compatible types.")
    }
    compareBytes(xString.getBytes("utf-8") zip yString.getBytes("utf-8"))
  }

  private def compareNumbers(x: BsonValue, y: BsonValue): Int = {
    (x.getBsonType, y.getBsonType) match {
      case (_, BsonType.DECIMAL128) => compareNumberWithDecimal(x.asNumber(), y.asDecimal128())
      case (BsonType.DECIMAL128, _) => -compareNumberWithDecimal(y.asNumber(), x.asDecimal128())
      case (_, _)                   => compareBsonNumbers(x.asNumber(), y.asNumber())
    }
  }

  private def compareBsonNumbers(x: BsonNumber, y: BsonNumber): Int = {
    (x.getBsonType, y.getBsonType) match {
      case (BsonType.INT64, BsonType.DOUBLE) => compareLongToDouble(x.longValue, y.doubleValue)
      case (BsonType.DOUBLE, BsonType.INT64) => -compareLongToDouble(y.longValue, x.doubleValue)
      case _                                 => x.doubleValue.compareTo(y.doubleValue)
    }
  }

  private def compareNumberWithDecimal(x: BsonNumber, y: BsonDecimal128): Int = {
    val decimalValue = x.getBsonType match {
      case BsonType.INT64  => BigDecimal(x.longValue())
      case BsonType.INT32  => BigDecimal(x.intValue())
      case BsonType.DOUBLE => BigDecimal(x.doubleValue())
      case _               => throw new UnsupportedOperationException(s"Unsupported numeric type: ${x.getBsonType}")
    }
    decimalValue.compare(BigDecimal(y.decimal128Value().bigDecimalValue()))
  }

  private def compareLongToDouble(x: Long, y: Double): Int = {
    val END_OF_PRECISE_DOUBLES: Double = 1 << 53 // positive 2**63
    val BOUND_OF_LONG_RANGE: Double = -Long.MinValue.toDouble

    if (y.compareTo(Double.NaN) == 0) {
      // All Longs are > NaN
      1
    } else if (x <= END_OF_PRECISE_DOUBLES && x >= -END_OF_PRECISE_DOUBLES) {
      // Ints with magnitude <= 2**53 can be precisely represented as doubles.
      // Additionally, doubles outside of this range can't have a fractional component.
      x.toDouble.compareTo(y)
    } else if (y >= BOUND_OF_LONG_RANGE) {
      // Large magnitude doubles (including +/- Inf) are strictly > or < all Longs.
      // Can't be represented in a Long.
      -1
    } else if (y < -BOUND_OF_LONG_RANGE) {
      1 // Can be represented in a Long.
    } else {
      // Remaining Doubles can have their integer component precisely represented as longs.
      // If they have a fractional component, they must be strictly > or < lhs even after
      // truncation of the fractional component since low-magnitude lhs were handled above.
      x.compareTo(y.toLong)
    }
  }

  private def documentKeyValues(document: BsonDocument): Seq[(String, BsonValue)] = {
    val it = document.entrySet().iterator()
    new Iterator[(String, BsonValue)] {
      override def hasNext = it.hasNext
      override def next() = {
        val next = it.next()
        (next.getKey, next.getValue)
      }
    }.toSeq
  }

  private object isBsonNumber extends Serializable {
    val bsonNumberTypes = Set(BsonType.INT32, BsonType.INT64, BsonType.DOUBLE, BsonType.DECIMAL128)

    def unapply(x: BsonType): Boolean = bsonNumberTypes.contains(x)
  }

  private object isString extends Serializable {
    val bsonStringTypes = Set(BsonType.SYMBOL, BsonType.STRING)

    def unapply(x: BsonType): Boolean = bsonStringTypes.contains(x)
  }

}

// scalastyle:on cyclomatic.complexity
