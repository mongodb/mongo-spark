/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.mongodb.spark.sql.connector.read.partitioner;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonNumber;
import org.bson.BsonRegularExpression;
import org.bson.BsonType;
import org.bson.BsonValue;

/**
 * A Bson value comparator.
 *
 * <p>Used to ensure the validity of field list partitioners
 *
 * <p>See: <a href="https://docs.mongodb.com/manual/reference/bson-type-comparison-order/">Bson
 * Comparison/Sort Order</a>
 */
final class BsonValueComparator implements Comparator<BsonValue> {

  private static final Map<BsonType, Integer> BSON_TYPES_ORDER_MAP = new HashMap<>();
  private static final double END_OF_PRECISE_DOUBLES = 1 << 53;
  private static final double BOUND_OF_LONG_RANGE = -(double) Long.MIN_VALUE; // positive 2**63

  static {
    BSON_TYPES_ORDER_MAP.put(BsonType.MIN_KEY, 1);
    BSON_TYPES_ORDER_MAP.put(BsonType.NULL, 2);

    BSON_TYPES_ORDER_MAP.put(BsonType.INT32, 3);
    BSON_TYPES_ORDER_MAP.put(BsonType.INT64, 3);
    BSON_TYPES_ORDER_MAP.put(BsonType.DOUBLE, 3);
    BSON_TYPES_ORDER_MAP.put(BsonType.DECIMAL128, 3);

    BSON_TYPES_ORDER_MAP.put(BsonType.SYMBOL, 4);
    BSON_TYPES_ORDER_MAP.put(BsonType.STRING, 4);

    BSON_TYPES_ORDER_MAP.put(BsonType.DOCUMENT, 5);
    BSON_TYPES_ORDER_MAP.put(BsonType.ARRAY, 6);
    BSON_TYPES_ORDER_MAP.put(BsonType.BINARY, 7);
    BSON_TYPES_ORDER_MAP.put(BsonType.OBJECT_ID, 8);
    BSON_TYPES_ORDER_MAP.put(BsonType.BOOLEAN, 9);
    BSON_TYPES_ORDER_MAP.put(BsonType.DATE_TIME, 10);
    BSON_TYPES_ORDER_MAP.put(BsonType.TIMESTAMP, 11);
    BSON_TYPES_ORDER_MAP.put(BsonType.REGULAR_EXPRESSION, 12);
    BSON_TYPES_ORDER_MAP.put(BsonType.MAX_KEY, 13);
  }

  /**
   * The Bson value comparator.
   *
   * <p>See: <a href="https://docs.mongodb.com/manual/reference/bson-type-comparison-order/">Bson
   * Comparison/Sort Order</a>
   */
  static final Comparator<BsonValue> BSON_VALUE_COMPARATOR = new BsonValueComparator();

  /**
   * Compares two BsonValues
   *
   * <p>When comparing values of different BSON types, MongoDB uses the following comparison order,
   * from lowest to highest:
   *
   * <ol>
   *   <li>MinKey (internal type)
   *   <li>Null
   *   <li>Numbers (ints, longs, doubles, decimals)
   *   <li>Symbol, String
   *   <li>Object
   *   <li>Array
   *   <li>BinData
   *   <li>ObjectId
   *   <li>Boolean
   *   <li>Date
   *   <li>Timestamp
   *   <li>Regular Expression
   *   <li>MaxKey (internal type)
   * </ol>
   *
   * @param x the first value to be compared.
   * @param y the second value to be compared.
   * @return a negative integer, zero, or a positive integer as the first argument is less than,
   *     equal to, or greater than the second.
   * @see <a href="https://docs.mongodb.com/manual/reference/bson-type-comparison-order/">Bson types
   *     comparison order</a>
   */
  @Override
  public int compare(final BsonValue x, final BsonValue y) {
    Integer xOrderInt = BSON_TYPES_ORDER_MAP.getOrDefault(x.getBsonType(), 14);
    Integer yOrderInt = BSON_TYPES_ORDER_MAP.getOrDefault(y.getBsonType(), 14);

    if (!xOrderInt.equals(yOrderInt)) {
      return xOrderInt.compareTo(yOrderInt);
    } else if (x.isNumber() || x.isDecimal128()) {
      return compareNumbers(x, y);
    } else if (x.isString() || x.isSymbol()) {
      return compareStrings(x, y);
    } else if (x.isDocument()) {
      return compareDocuments(x.asDocument(), y.asDocument());
    } else if (x.isArray()) {
      return compareArrays(x.asArray(), y.asArray());
    } else if (x.isBinary()) {
      return compareBinary(x.asBinary(), y.asBinary());
    } else if (x.isObjectId()) {
      return x.asObjectId().compareTo(y.asObjectId());
    } else if (x.isBoolean()) {
      return x.asBoolean().compareTo(y.asBoolean());
    } else if (x.isDateTime()) {
      return x.asDateTime().compareTo(y.asDateTime());
    } else if (x.isTimestamp()) {
      return x.asTimestamp().compareTo(y.asTimestamp());
    } else if (x.isRegularExpression()) {
      return comapareRegularExpressions(x.asRegularExpression(), y.asRegularExpression());
    }

    // both null || min_key || max_key
    return 0;
  }

  /** See: https://github.com/mongodb/mongo/blob/r5.2.1/src/mongo/base/compare_numbers.h */
  private int compareNumbers(final BsonValue x, final BsonValue y) {
    if (x.isDecimal128() && y.isDecimal128()) {
      return x.asDecimal128().decimal128Value().compareTo(y.asDecimal128().decimal128Value());
    } else if (y.isDecimal128()) {
      return compareNumberWithDecimal(x.asNumber(), y.asDecimal128());
    } else if (x.isDecimal128()) {
      return -compareNumberWithDecimal(y.asNumber(), x.asDecimal128());
    } else {
      return compareBsonNumbers(x.asNumber(), y.asNumber());
    }
  }

  private int compareNumberWithDecimal(final BsonNumber x, final BsonDecimal128 y) {
    BigDecimal decimalValue;
    if (x.isInt64()) {
      decimalValue = BigDecimal.valueOf(x.longValue());
    } else if (x.isInt32()) {
      decimalValue = BigDecimal.valueOf(x.intValue());
    } else {
      decimalValue = BigDecimal.valueOf(x.doubleValue());
    }
    return decimalValue.compareTo(y.decimal128Value().bigDecimalValue());
  }

  private int compareBsonNumbers(final BsonNumber x, final BsonNumber y) {
    if (x.isInt64() && y.isDouble()) {
      return compareLongToDouble(x.longValue(), y.doubleValue());
    } else if (x.isDouble() && y.isInt64()) {
      return -compareLongToDouble(y.longValue(), x.doubleValue());
    } else if (x.isInt32() && y.isInt32()) {
      return Integer.compare(x.intValue(), y.intValue());
    } else if (x.isInt64() && y.isInt64()) {
      return Long.compare(x.longValue(), y.longValue());
    }
    return Double.compare(x.doubleValue(), y.doubleValue());
  }

  private int compareLongToDouble(final Long x, final Double y) {

    if (y.compareTo(Double.NaN) == 0) {
      // All Longs are < NaN
      return -1;
    } else if (x <= END_OF_PRECISE_DOUBLES && x >= -END_OF_PRECISE_DOUBLES) {
      // Longs with magnitude <= 2**53 can be precisely represented as doubles.
      // Additionally, doubles outside of this range can't have a fractional component.
      return Double.compare(x.doubleValue(), y);
    } else if (y >= BOUND_OF_LONG_RANGE) {
      // Large magnitude doubles (including +/- Inf) are strictly > or < all Longs.
      // Can't be represented in a Long.
      return -1;
    } else if (y < -BOUND_OF_LONG_RANGE) {
      return 1; // Can be represented in a Long.
    }
    // Remaining Doubles can have their integer component precisely represented as longs.
    // If they have a fractional component, they must be strictly > or < lhs even after
    // truncation of the fractional component since low-magnitude lhs were handled above.
    return x.compareTo(y.longValue());
  }

  private int compareStrings(final BsonValue x, final BsonValue y) {
    String xString = x.isString() ? x.asString().getValue() : x.asSymbol().getSymbol();
    String yString = y.isString() ? y.asString().getValue() : y.asSymbol().getSymbol();
    return xString.compareTo(yString);
  }

  /**
   * Recursively compare key-value pairs in the order that they appear within the BSON object.
   *
   * <ol>
   *   <li>Compare the field types. MongoDB uses the following comparison order for field types,
   *       from lowest to highest:
   *       <ol>
   *         <li>MinKey (internal type)
   *         <li>Null
   *         <li>Numbers (ints, longs, doubles, decimals)
   *         <li>Symbol, String
   *         <li>Object
   *         <li>Array
   *         <li>BinData
   *         <li>ObjectId
   *         <li>Boolean
   *         <li>Date
   *         <li>Timestamp
   *         <li>Regular Expression
   *         <li>MaxKey (internal type)
   *       </ol>
   *   <li>If the field types are equal, compare the key field names.
   *   <li>If the key field names are equal, compare the field values.
   *   <li>If the field values are equal, compare the next key/value pair (return to step 1). An
   *       object without further pairs is less than an object with further pairs.
   * </ol>
   *
   * @see <a
   *     href="https://docs.mongodb.com/manual/reference/bson-type-comparison-order/#objects">Bson
   *     types comparison order</a>.
   */
  private int compareDocuments(final BsonDocument x, final BsonDocument y) {
    List<Map.Entry<String, BsonValue>> xEntries = new ArrayList<>(x.entrySet());
    List<Map.Entry<String, BsonValue>> yEntries = new ArrayList<>(y.entrySet());

    for (int i = 0; i < Math.min(x.size(), y.size()); i++) {
      Map.Entry<String, BsonValue> xEntry = xEntries.get(i);
      Map.Entry<String, BsonValue> yEntry = yEntries.get(i);

      // Compare types
      Integer xOrderInt = BSON_TYPES_ORDER_MAP.getOrDefault(xEntry.getValue().getBsonType(), 14);
      Integer yOrderInt = BSON_TYPES_ORDER_MAP.getOrDefault(yEntry.getValue().getBsonType(), 14);
      int comparison = xOrderInt.compareTo(yOrderInt);
      if (comparison != 0) {
        return comparison;
      }

      // Compare field names
      comparison = xEntry.getKey().compareTo(yEntry.getKey());
      if (comparison != 0) {
        return comparison;
      }

      // Compare field values
      comparison = compare(xEntry.getValue(), yEntry.getValue());
      if (comparison != 0) {
        return comparison;
      }
    }

    // An object without further pairs is less than an object with further pairs.
    return Integer.compare(x.size(), y.size());
  }

  private int compareArrays(final BsonArray x, final BsonArray y) {
    for (int i = 0; i < Math.min(x.size(), y.size()); i++) {
      int comparison = compare(x.get(i), y.get(i));
      if (comparison != 0) {
        return comparison;
      }
    }

    // An array without further data is less than an object with further data.
    return Integer.compare(x.size(), y.size());
  }

  /**
   * MongoDB sorts BinData in the following order:
   *
   * <ol>
   *   <li>The length or size of the data.
   *   <li>The BSON one-byte subtype.
   *   <li>The data, performing a byte-by-byte comparison.
   * </ol>
   *
   * @see <a href="https://docs.mongodb.com/manual/reference/bson-type-comparison-order/#bindata"/>
   *     </a>
   */
  private int compareBinary(final BsonBinary x, final BsonBinary y) {
    // Compare length
    int comparison = Integer.compare(x.getData().length, y.getData().length);
    if (comparison != 0) {
      return comparison;
    }

    // Compare types
    comparison = Integer.compare(x.getType(), y.getType());
    if (comparison != 0) {
      return comparison;
    }

    // Compare each byte
    for (int i = 0; i < x.getData().length; i++) {
      comparison = Byte.compare(x.getData()[i], y.getData()[i]);
      if (comparison != 0) {
        return comparison;
      }
    }
    return 0;
  }

  private int comapareRegularExpressions(
      final BsonRegularExpression x, final BsonRegularExpression y) {
    int comparison = x.getPattern().compareTo(y.getPattern());
    if (comparison != 0) {
      return comparison;
    }
    return x.getOptions().compareTo(y.getOptions());
  }

  private BsonValueComparator() {}
}
