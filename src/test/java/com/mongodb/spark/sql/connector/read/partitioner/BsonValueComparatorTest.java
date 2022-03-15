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

import static com.mongodb.spark.sql.connector.read.partitioner.BsonValueComparator.BSON_VALUE_COMPARATOR;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDbPointer;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

public class BsonValueComparatorTest {

  private static final ObjectId OBJECT_ID = new ObjectId("000000000000000000000000");
  private static final BsonDocument ALL_BSON_TYPES_BSON_DOCUMENT = new BsonDocument();

  static {
    ALL_BSON_TYPES_BSON_DOCUMENT.put("nullValue", new BsonNull());
    ALL_BSON_TYPES_BSON_DOCUMENT.put("int32", new BsonInt32(42));
    ALL_BSON_TYPES_BSON_DOCUMENT.put("int64", new BsonInt64(52L));
    ALL_BSON_TYPES_BSON_DOCUMENT.put(
        "decimal", new BsonDecimal128(new Decimal128(new BigDecimal(10))));
    ALL_BSON_TYPES_BSON_DOCUMENT.put("boolean", new BsonBoolean(true));
    ALL_BSON_TYPES_BSON_DOCUMENT.put("date", new BsonDateTime(1463497097));
    ALL_BSON_TYPES_BSON_DOCUMENT.put("double", new BsonDouble(62.0));
    ALL_BSON_TYPES_BSON_DOCUMENT.put("string", new BsonString("spark connector"));
    ALL_BSON_TYPES_BSON_DOCUMENT.put("minKey", new BsonMinKey());
    ALL_BSON_TYPES_BSON_DOCUMENT.put("maxKey", new BsonMaxKey());
    ALL_BSON_TYPES_BSON_DOCUMENT.put("objectId", new BsonObjectId(OBJECT_ID));
    ALL_BSON_TYPES_BSON_DOCUMENT.put("code", new BsonJavaScript("int i = 0;"));
    ALL_BSON_TYPES_BSON_DOCUMENT.put(
        "codeWithScope",
        new BsonJavaScriptWithScope("int x = y", new BsonDocument("y", new BsonInt32(1))));
    ALL_BSON_TYPES_BSON_DOCUMENT.put("regex", new BsonRegularExpression("^test.*regex.*xyz$", "i"));
    ALL_BSON_TYPES_BSON_DOCUMENT.put("symbol", new BsonSymbol("ruby stuff"));
    ALL_BSON_TYPES_BSON_DOCUMENT.put("timestamp", new BsonTimestamp(0x12345678, 5));
    ALL_BSON_TYPES_BSON_DOCUMENT.put("undefined", new BsonUndefined());
    ALL_BSON_TYPES_BSON_DOCUMENT.put("binary", new BsonBinary(new byte[] {5, 4, 3, 2, 1}));
    ALL_BSON_TYPES_BSON_DOCUMENT.put(
        "oldBinary", new BsonBinary(BsonBinarySubType.OLD_BINARY, new byte[] {1, 1, 1, 1, 1}));
    ALL_BSON_TYPES_BSON_DOCUMENT.put(
        "arrayInt", new BsonArray(asList(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3))));
    ALL_BSON_TYPES_BSON_DOCUMENT.put("document", new BsonDocument("a", new BsonInt32(1)));
    ALL_BSON_TYPES_BSON_DOCUMENT.put("dbPointer", new BsonDbPointer("db.coll", OBJECT_ID));
  }

  private static final List<String> ORDERED_KEYS =
      asList(
          "minKey",
          "nullValue",
          "decimal",
          "int32",
          "int64",
          "double",
          "symbol",
          "string",
          "document",
          "arrayInt",
          "binary",
          "oldBinary",
          "objectId",
          "boolean",
          "date",
          "timestamp",
          "regex",
          "maxKey");

  private static final List<String> UNDEFINED_ORDERED_KEYS =
      asList("dbPointer", "undefined", "code", "codeWithScope");

  @Test
  void shouldOrderBsonTypesCorrectly() {
    List<BsonValue> expected =
        ORDERED_KEYS.stream().map(ALL_BSON_TYPES_BSON_DOCUMENT::get).collect(toList());

    testComparator("Bson types", expected);
  }

  @Test
  void testBoolean() {
    List<BsonValue> expected = asList(BsonBoolean.FALSE, BsonBoolean.TRUE);
    testComparator("BsonBoolean", expected);
  }

  @Test
  void testNumerics() {
    List<BsonValue> expected =
        asList(
            new BsonInt32(1),
            new BsonInt64(2),
            new BsonInt64(2),
            new BsonDecimal128(Decimal128.parse("2.5")),
            new BsonDecimal128(Decimal128.parse("2.5")),
            new BsonDouble(3.0),
            new BsonDouble(3.1),
            new BsonInt64(5),
            new BsonInt32(6));

    testComparator("Bson numbers", expected);

    List<BsonValue> expectedWithInfinity =
        asList(
            new BsonDouble(Double.NEGATIVE_INFINITY),
            new BsonDouble(Long.MIN_VALUE),
            new BsonInt64(Long.MIN_VALUE + 1),
            new BsonDouble(Double.MIN_VALUE),
            new BsonInt64(1L),
            new BsonInt64(Long.MAX_VALUE),
            new BsonDouble(Long.MAX_VALUE),
            new BsonDouble(Double.POSITIVE_INFINITY),
            new BsonDouble(Double.NaN));

    testComparator("Bson numbers", expectedWithInfinity);
  }

  @Test
  void testStringsAndSymbols() {
    List<BsonValue> expected =
        asList(
            new BsonString("1"),
            new BsonString("12345"),
            new BsonSymbol("a"),
            new BsonSymbol("b"),
            new BsonSymbol("c 1"),
            new BsonString("c 2"),
            new BsonSymbol("d"),
            new BsonString("e"));

    testComparator("Bson Strings / Symbols", expected);
  }

  @Test
  void testTimestamps() {
    List<BsonValue> expected =
        asList(
            new BsonTimestamp(1, 5),
            new BsonTimestamp(1, 6),
            new BsonTimestamp(2, 1),
            new BsonTimestamp(2, 10),
            new BsonTimestamp(3, 0),
            new BsonTimestamp(3, 1),
            new BsonTimestamp(10010101, 0));

    testComparator("BsonTimestamp", expected);
  }

  @Test
  void testDateTime() {
    List<BsonValue> expected =
        asList(
            new BsonDateTime(Long.MIN_VALUE),
            new BsonDateTime(1),
            new BsonDateTime(2),
            new BsonDateTime(2),
            new BsonDateTime(3),
            new BsonDateTime(Long.MAX_VALUE));

    testComparator("BsonDateTime", expected);
  }

  @Test
  void testBinaries() {
    List<BsonValue> expected =
        asList(
            new BsonBinary(BsonBinarySubType.BINARY, new byte[] {1, 1, 1, 0, 1}),
            new BsonBinary(BsonBinarySubType.BINARY, new byte[] {1, 1, 1, 2, 1}),
            new BsonBinary(BsonBinarySubType.BINARY, new byte[] {1, 1, 1, 2, 1}),
            new BsonBinary(BsonBinarySubType.OLD_BINARY, new byte[] {2, 2, 2, 2, 2, 2}),
            new BsonBinary(BsonBinarySubType.OLD_BINARY, new byte[] {2, 2, 2, 2, 2, 2, 2}),
            new BsonBinary(BsonBinarySubType.OLD_BINARY, new byte[] {3, 2, 2, 2, 2, 2, 3}));

    testComparator("BsonBinaries", expected);
  }

  @Test
  void testArrays() {
    List<BsonValue> expected =
        asList(
            new BsonArray(singletonList(new BsonInt32(1))),
            new BsonArray(asList(new BsonInt32(1), new BsonInt32(2))),
            new BsonArray(asList(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3))),
            new BsonArray(singletonList(new BsonString("1"))));

    testComparator("BsonArrays", expected);
  }

  @Test
  void testRegularExpressions() {
    List<BsonValue> expected =
        asList(
            new BsonRegularExpression(".*"),
            new BsonRegularExpression(".*", "i"),
            new BsonRegularExpression("[0-9a-z]+"),
            new BsonRegularExpression("[a-z]+"));

    testComparator("BsonRegularExpressions", expected);
  }

  @Test
  void testCompareDocuments() {
    List<BsonValue> expected =
        asList(
            BsonDocument.parse("{}"),
            BsonDocument.parse("{a: -1}"),
            BsonDocument.parse("{a: 1}"),
            BsonDocument.parse("{a: 1, b: 1}"),
            BsonDocument.parse("{a: 1, b: 2}"),
            BsonDocument.parse("{a: 1, b: 2, c: -1}"),
            BsonDocument.parse("{b: 2, c: -1}"),
            BsonDocument.parse("{a: '1', b: 0}"),
            BsonDocument.parse("{b: '1'}"),
            BsonDocument.parse("{c: '2'}"),
            BsonDocument.parse("{c: '2', d: 0}"),
            BsonDocument.parse("{c: '3'}"),
            BsonDocument.parse("{a: [1, 2], b: 0}"));

    testComparator("BsonDocuments", expected);
  }

  @Test
  void testObjectIds() {
    List<BsonValue> expected =
        asList(
            new BsonObjectId(new ObjectId("000000000000000000000000")),
            new BsonObjectId(new ObjectId("000000000000000000000001")),
            new BsonObjectId(new ObjectId("000000000000000000000002")),
            new BsonObjectId(new ObjectId("000000000000000000000003")),
            new BsonObjectId(new ObjectId("000000000000000000000004")),
            new BsonObjectId(new ObjectId()));
    testComparator("BsonObjectId", expected);
  }

  @Test
  void testBsonValueTypesWithNoDefinedOrder() {
    List<BsonValue> expected =
        UNDEFINED_ORDERED_KEYS.stream().map(ALL_BSON_TYPES_BSON_DOCUMENT::get).collect(toList());

    assertAll(
        "No defined ordering",
        IntStream.range(0, 10)
            .mapToObj(
                x ->
                    () -> {
                      List<BsonValue> data = new ArrayList<>(expected);
                      Collections.shuffle(data);

                      for (int i = 0; i < expected.size(); i++) {
                        assertEquals(
                            0, BSON_VALUE_COMPARATOR.compare(expected.get(i), data.get(i)));
                      }
                    }));
  }

  private void testComparator(final String heading, final List<BsonValue> expected) {
    assertAll(
        heading,
        IntStream.range(0, 10)
            .mapToObj(
                i ->
                    () -> {
                      List<BsonValue> data = new ArrayList<>(expected);
                      Collections.shuffle(data);
                      data.sort(BSON_VALUE_COMPARATOR);
                      assertIterableEquals(expected, data);
                    }));
  }
}
