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

package com.mongodb.spark.sql.connector.schema;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.mongodb.spark.sql.connector.exceptions.DataException;
import com.mongodb.spark.sql.connector.interop.JavaScala;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class BsonDocumentToRowConverterTest extends SchemaTest {

  private static final BsonDocumentToRowConverter CONVERTER =
      new BsonDocumentToRowConverter(new StructType(), false);

  private static final BiFunction<DataType, BsonValue, Object> CONVERT =
      (dataType, bsonValue) -> CONVERTER.convertBsonValue("", dataType, bsonValue);

  private static final BsonDocumentToRowConverter EXTENDED_CONVERTER =
      new BsonDocumentToRowConverter(new StructType(), true);

  private static final BiFunction<DataType, BsonValue, Object> EXTENDED_CONVERT =
      (dataType, bsonValue) -> EXTENDED_CONVERTER.convertBsonValue("", dataType, bsonValue);

  private static final DataType DECIMAL_TYPE = DataTypes.createDecimalType();

  @Test
  @DisplayName("test string support relaxed")
  void testStringSupport() {
    Map<String, String> expected = new HashMap<String, String>() {
      {
        put("_id", "5f15aab12435743f9bd126a4");
        put("myString", "some foo bla text");
        put("myInt", "42");
        put("myDouble", "20.21");
        put(
            "mySubDoc",
            "{\"A\": \"S2Fma2Egcm9ja3Mh\", "
                + "\"B\": \"2020-01-01T07:27:07Z\", "
                + "\"C\": {\"D\": \"12345.6789\"}}");
        put("myArray", "[1, 2, 3]");
        put("myBytes", "S2Fma2Egcm9ja3Mh");
        put("myDate", "1970-01-15T06:56:07.89Z");
        put("myDecimal", "12345.6789");
        put("null", null);
      }
    };

    BSON_DOCUMENT.forEach(
        (k, v) -> assertEquals(expected.get(k), CONVERT.apply(DataTypes.StringType, v)));
  }

  @Test
  @DisplayName("test extended Json string support")
  void testExtendedJsonStringSupport() {
    BSON_DOCUMENT_ALL_TYPES.forEach((k, v) -> assertEquals(
        ALL_TYPES_EXTENDED_JSON_ROW.get(ALL_TYPES_EXTENDED_JSON_ROW.fieldIndex(k)),
        EXTENDED_CONVERT.apply(DataTypes.StringType, v)));
  }

  @Test
  @DisplayName("test number support")
  void testNumberSupport() {
    BsonInt32 bsonInt32 = new BsonInt32(42);
    BsonInt64 bsonInt64 = new BsonInt64(2020L);
    BsonDouble bsonDouble = new BsonDouble(20.20);
    BsonDateTime bsonDateTime = new BsonDateTime(1234567890000L);
    BsonTimestamp bsonTimestamp = new BsonTimestamp(1234567890, 1);
    BsonDecimal128 bsonDecimal128 = new BsonDecimal128(Decimal128.parse("1234567890.1234"));
    BsonString bsonString = new BsonString("987654321.54321");

    assertAll(
        "Testing int8 support",
        () -> assertEquals((byte) 42, CONVERT.apply(DataTypes.ByteType, bsonInt32)),
        () -> assertEquals((byte) 2020, CONVERT.apply(DataTypes.ByteType, bsonInt64)),
        () -> assertEquals((byte) 20.20, CONVERT.apply(DataTypes.ByteType, bsonDouble)),
        () -> assertEquals((byte) 1234567890000L, CONVERT.apply(DataTypes.ByteType, bsonDateTime)),
        () -> assertEquals((byte) 1234567890000L, CONVERT.apply(DataTypes.ByteType, bsonTimestamp)),
        () ->
            assertEquals((byte) 1234567890.1234, CONVERT.apply(DataTypes.ByteType, bsonDecimal128)),
        () -> assertEquals((byte) 987654321.54321, CONVERT.apply(DataTypes.ByteType, bsonString)),
        () -> assertNull(CONVERT.apply(DataTypes.ByteType, BsonNull.VALUE)));

    assertAll(
        "Testing int16 support",
        () -> assertEquals((short) 42, CONVERT.apply(DataTypes.ShortType, bsonInt32)),
        () -> assertEquals((short) 2020, CONVERT.apply(DataTypes.ShortType, bsonInt64)),
        () -> assertEquals((short) 20.20, CONVERT.apply(DataTypes.ShortType, bsonDouble)),
        () ->
            assertEquals((short) 1234567890000L, CONVERT.apply(DataTypes.ShortType, bsonDateTime)),
        () ->
            assertEquals((short) 1234567890000L, CONVERT.apply(DataTypes.ShortType, bsonTimestamp)),
        () -> assertEquals(
            (short) 1234567890.1234, CONVERT.apply(DataTypes.ShortType, bsonDecimal128)),
        () -> assertEquals((short) 987654321.54321, CONVERT.apply(DataTypes.ShortType, bsonString)),
        () -> assertNull(CONVERT.apply(DataTypes.ShortType, BsonNull.VALUE)));

    assertAll(
        "Testing int32 support",
        () -> assertEquals(42, CONVERT.apply(DataTypes.IntegerType, bsonInt32)),
        () -> assertEquals(2020, CONVERT.apply(DataTypes.IntegerType, bsonInt64)),
        () -> assertEquals((int) 20.20, CONVERT.apply(DataTypes.IntegerType, bsonDouble)),
        () ->
            assertEquals((int) 1234567890000L, CONVERT.apply(DataTypes.IntegerType, bsonDateTime)),
        () ->
            assertEquals((int) 1234567890000L, CONVERT.apply(DataTypes.IntegerType, bsonTimestamp)),
        () -> assertEquals(
            (int) 1234567890.1234, CONVERT.apply(DataTypes.IntegerType, bsonDecimal128)),
        () -> assertEquals((int) 987654321.54321, CONVERT.apply(DataTypes.IntegerType, bsonString)),
        () -> assertNull(CONVERT.apply(DataTypes.IntegerType, BsonNull.VALUE)));

    assertAll(
        "Testing int64 support",
        () -> assertEquals((long) 42, CONVERT.apply(DataTypes.LongType, bsonInt32)),
        () -> assertEquals((long) 2020, CONVERT.apply(DataTypes.LongType, bsonInt64)),
        () -> assertEquals((long) 20.20, CONVERT.apply(DataTypes.LongType, bsonDouble)),
        () -> assertEquals(1234567890000L, CONVERT.apply(DataTypes.LongType, bsonDateTime)),
        () -> assertEquals(1234567890000L, CONVERT.apply(DataTypes.LongType, bsonTimestamp)),
        () ->
            assertEquals((long) 1234567890.1234, CONVERT.apply(DataTypes.LongType, bsonDecimal128)),
        () -> assertEquals((long) 987654321.54321, CONVERT.apply(DataTypes.LongType, bsonString)),
        () -> assertNull(CONVERT.apply(DataTypes.LongType, BsonNull.VALUE)));

    assertAll(
        "Testing float32 support",
        () -> assertEquals((float) 42, CONVERT.apply(DataTypes.FloatType, bsonInt32)),
        () -> assertEquals((float) 2020, CONVERT.apply(DataTypes.FloatType, bsonInt64)),
        () -> assertEquals((float) 20.20, CONVERT.apply(DataTypes.FloatType, bsonDouble)),
        () ->
            assertEquals((float) 1234567890000L, CONVERT.apply(DataTypes.FloatType, bsonDateTime)),
        () ->
            assertEquals((float) 1234567890000L, CONVERT.apply(DataTypes.FloatType, bsonTimestamp)),
        () -> assertEquals(
            (float) 1234567890.1234, CONVERT.apply(DataTypes.FloatType, bsonDecimal128)),
        () -> assertEquals((float) 987654321.54321, CONVERT.apply(DataTypes.FloatType, bsonString)),
        () -> assertNull(CONVERT.apply(DataTypes.FloatType, BsonNull.VALUE)));

    assertAll(
        "Testing float64 support",
        () -> assertEquals((double) 42, CONVERT.apply(DataTypes.DoubleType, bsonInt32)),
        () -> assertEquals((double) 2020, CONVERT.apply(DataTypes.DoubleType, bsonInt64)),
        () -> assertEquals(20.20, CONVERT.apply(DataTypes.DoubleType, bsonDouble)),
        () -> assertEquals(
            (double) 1234567890000L, CONVERT.apply(DataTypes.DoubleType, bsonDateTime)),
        () -> assertEquals(
            (double) 1234567890000L, CONVERT.apply(DataTypes.DoubleType, bsonTimestamp)),
        () -> assertEquals(1234567890.1234, CONVERT.apply(DataTypes.DoubleType, bsonDecimal128)),
        () -> assertEquals(987654321.54321, CONVERT.apply(DataTypes.DoubleType, bsonString)),
        () -> assertNull(CONVERT.apply(DataTypes.DoubleType, BsonNull.VALUE)));

    assertAll(
        "Testing decimal support",
        () -> assertEquals(BigDecimal.valueOf(42), CONVERT.apply(DECIMAL_TYPE, bsonInt32)),
        () -> assertEquals(BigDecimal.valueOf(2020), CONVERT.apply(DECIMAL_TYPE, bsonInt64)),
        () -> assertEquals(BigDecimal.valueOf(20.20), CONVERT.apply(DECIMAL_TYPE, bsonDouble)),
        () -> assertEquals(
            BigDecimal.valueOf(1234567890000L), CONVERT.apply(DECIMAL_TYPE, bsonDateTime)),
        () -> assertEquals(
            BigDecimal.valueOf(1234567890000L), CONVERT.apply(DECIMAL_TYPE, bsonTimestamp)),
        () -> assertEquals(
            bsonDecimal128.decimal128Value().bigDecimalValue(),
            CONVERT.apply(DECIMAL_TYPE, bsonDecimal128)),
        () -> assertEquals(
            new BigDecimal("987654321.54321"), CONVERT.apply(DECIMAL_TYPE, bsonString)),
        () -> assertNull(CONVERT.apply(DECIMAL_TYPE, BsonNull.VALUE)));

    List<String> validKeys = asList("myInt", "myDouble", "myDate", "myDecimal");
    Set<String> invalidKeys = BSON_DOCUMENT.keySet().stream()
        .filter(k -> !validKeys.contains(k))
        .collect(Collectors.toSet());

    invalidKeys.forEach(k -> assertThrows(
        DataException.class,
        () -> CONVERT.apply(DataTypes.DoubleType, BSON_DOCUMENT.get(k)),
        format("Expected %s to fail", k)));
  }

  @Test
  @DisplayName("test date and timestamp support")
  void testDateAndTimestampSupport() {
    long oneDayAnd1Hour = TimeUnit.MILLISECONDS.convert(25, TimeUnit.HOURS);

    BsonInt32 bsonInt32 = new BsonInt32((int) oneDayAnd1Hour);
    BsonInt64 bsonInt64 = new BsonInt64(oneDayAnd1Hour * 2L);
    BsonDouble bsonDouble = new BsonDouble(oneDayAnd1Hour * 3L);
    BsonDateTime bsonDateTime = new BsonDateTime(oneDayAnd1Hour * 4L);
    BsonTimestamp bsonTimestamp = new BsonTimestamp((int) (oneDayAnd1Hour * 5L / 1000), 1);
    BsonDecimal128 bsonDecimal128 = new BsonDecimal128(Decimal128.parse(oneDayAnd1Hour * 7 + ""));

    assertAll(
        "Testing date support",
        () -> assertEquals(new Date(oneDayAnd1Hour), CONVERT.apply(DataTypes.DateType, bsonInt32)),
        () -> assertEquals(
            new Date(oneDayAnd1Hour * 2L), CONVERT.apply(DataTypes.DateType, bsonInt64)),
        () -> assertEquals(
            new Date(oneDayAnd1Hour * 3L), CONVERT.apply(DataTypes.DateType, bsonDouble)),
        () -> assertEquals(
            new Date(oneDayAnd1Hour * 4L), CONVERT.apply(DataTypes.DateType, bsonDateTime)),
        () -> assertEquals(
            new Date(oneDayAnd1Hour * 5L), CONVERT.apply(DataTypes.DateType, bsonTimestamp)),
        () -> assertEquals(
            new Date(oneDayAnd1Hour * 7L), CONVERT.apply(DataTypes.DateType, bsonDecimal128)),
        () -> assertNull(CONVERT.apply(DataTypes.DateType, BsonNull.VALUE)));

    assertAll(
        "Testing datetime support",
        () -> assertEquals(
            new Timestamp(oneDayAnd1Hour), CONVERT.apply(DataTypes.TimestampType, bsonInt32)),
        () -> assertEquals(
            new Timestamp(oneDayAnd1Hour * 2L), CONVERT.apply(DataTypes.TimestampType, bsonInt64)),
        () -> assertEquals(
            new Timestamp(oneDayAnd1Hour * 3L), CONVERT.apply(DataTypes.TimestampType, bsonDouble)),
        () -> assertEquals(
            new Timestamp(oneDayAnd1Hour * 4L),
            CONVERT.apply(DataTypes.TimestampType, bsonDateTime)),
        () -> assertEquals(
            new Timestamp(oneDayAnd1Hour * 5L),
            CONVERT.apply(DataTypes.TimestampType, bsonTimestamp)),
        () -> assertEquals(
            new Timestamp(oneDayAnd1Hour * 7L),
            CONVERT.apply(DataTypes.TimestampType, bsonDecimal128)),
        () -> assertNull(CONVERT.apply(DataTypes.TimestampType, BsonNull.VALUE)));

    List<String> validKeys = asList("myInt", "myDouble", "myDate", "myDecimal");
    Set<String> invalidKeys = BSON_DOCUMENT.keySet().stream()
        .filter(k -> !validKeys.contains(k))
        .collect(Collectors.toSet());

    invalidKeys.forEach(k -> assertThrows(
        DataException.class,
        () -> CONVERT.apply(DataTypes.DateType, BSON_DOCUMENT.get(k)),
        format("Expected %s to fail", k)));

    invalidKeys.forEach(k -> assertThrows(
        DataException.class,
        () -> CONVERT.apply(DataTypes.TimestampType, BSON_DOCUMENT.get(k)),
        format("Expected %s to fail", k)));
  }

  @Test
  @DisplayName("test boolean support")
  void testBooleanSupport() {
    assertAll(
        "Testing boolean support",
        () -> assertEquals(true, CONVERT.apply(DataTypes.BooleanType, BsonBoolean.TRUE)),
        () -> assertEquals(false, CONVERT.apply(DataTypes.BooleanType, BsonBoolean.FALSE)),
        () -> assertNull(CONVERT.apply(DataTypes.BooleanType, BsonNull.VALUE)));

    Set<String> invalidKeys = BSON_DOCUMENT.keySet().stream()
        .filter(k -> !k.equals("myBoolean"))
        .collect(Collectors.toSet());

    invalidKeys.forEach(k -> assertThrows(
        DataException.class,
        () -> CONVERT.apply(DataTypes.BooleanType, BSON_DOCUMENT.get(k)),
        format("Expected %s to fail", k)));
  }

  @Test
  @DisplayName("test bytes support")
  void testBytesSupport() {

    assertAll(
        "Testing bytes support",
        () -> assertArrayEquals(
            BSON_DOCUMENT.get("myString").asString().getValue().getBytes(StandardCharsets.UTF_8),
            (byte[]) CONVERT.apply(DataTypes.BinaryType, BSON_DOCUMENT.get("myString"))),
        () -> assertArrayEquals(BSON_DOCUMENT.getBinary("myBytes").getData(), (byte[])
            CONVERT.apply(DataTypes.BinaryType, BSON_DOCUMENT.getBinary("myBytes"))),
        () -> assertArrayEquals(
            BsonDocumentToRowConverter.documentToByteArray(BSON_DOCUMENT),
            (byte[]) CONVERT.apply(DataTypes.BinaryType, BSON_DOCUMENT)),
        () -> assertArrayEquals(
            BsonDocumentToRowConverter.documentToByteArray(BSON_DOCUMENT),
            (byte[]) CONVERT.apply(DataTypes.BinaryType, BsonDocument.parse(BSON_DOCUMENT_JSON))),
        () -> assertNull(CONVERT.apply(DataTypes.BinaryType, BsonNull.VALUE)));

    List<String> validKeys = asList("myString", "myBytes", "mySubDoc");
    Set<String> invalidKeys = BSON_DOCUMENT.keySet().stream()
        .filter(k -> !validKeys.contains(k))
        .collect(Collectors.toSet());

    invalidKeys.forEach(k -> assertThrows(
        DataException.class,
        () -> CONVERT.apply(DataTypes.BinaryType, BSON_DOCUMENT.get(k)),
        format("Expected %s to fail", k)));
  }

  @Test
  @DisplayName("test null support")
  void testNullSupport() {
    assertNull(CONVERT.apply(DataTypes.NullType, BSON_DOCUMENT));
    BSON_DOCUMENT
        .keySet()
        .forEach(k -> assertNull(CONVERT.apply(DataTypes.NullType, BSON_DOCUMENT.get(k))));
  }

  @Test
  @DisplayName("test array support")
  void testArraySupport() {
    BsonValue arrayContainingNull = BsonDocument.parse("{myArray: [null]}").get("myArray");
    DataType dataType = DataTypes.createArrayType(DataTypes.IntegerType);
    assertArrayEquals(asList(1, 2, 3).toArray(), (Object[])
        CONVERT.apply(dataType, BSON_DOCUMENT.get("myArray")));
    assertNull(CONVERT.apply(dataType, BsonNull.VALUE));
    assertArrayEquals(
        singletonList(null).toArray(), (Object[]) CONVERT.apply(dataType, arrayContainingNull));

    Set<String> invalidKeys = BSON_DOCUMENT.keySet().stream()
        .filter(k -> !k.equals("myArray"))
        .collect(Collectors.toSet());

    invalidKeys.forEach(k -> assertThrows(
        DataException.class,
        () -> CONVERT.apply(dataType, BSON_DOCUMENT.get(k)),
        format("Expected %s to fail", k)));
  }

  @Test
  @DisplayName("test Map support")
  void testMapSupport() {
    DataType dataType = DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType);

    assertEquals(
        JavaScala.asScala(new HashMap<String, String>() {
          {
            put("A", "S2Fma2Egcm9ja3Mh");
            put("B", "2020-01-01T07:27:07Z");
            put("C", "{\"D\": \"12345.6789\"}");
          }
        }),
        CONVERT.apply(dataType, BSON_DOCUMENT.get("mySubDoc")));

    assertNull(CONVERT.apply(dataType, BsonNull.VALUE));
    assertEquals(
        JavaScala.asScala(new HashMap<String, String>() {
          {
            put("A", null);
          }
        }),
        CONVERT.apply(dataType, BsonDocument.parse("{mySubDoc: {A: null}}").get("mySubDoc")));

    Set<String> invalidKeys = BSON_DOCUMENT.keySet().stream()
        .filter(k -> !k.equals("mySubDoc"))
        .collect(Collectors.toSet());

    invalidKeys.forEach(k -> assertThrows(
        DataException.class,
        () -> CONVERT.apply(dataType, BSON_DOCUMENT.get(k)),
        format("Expected %s to fail", k)));

    assertThrows(
        DataException.class,
        () -> CONVERT.apply(
            DataTypes.createMapType(DataTypes.IntegerType, DataTypes.StringType),
            BSON_DOCUMENT.get("mySubDoc")));
    assertThrows(
        DataException.class,
        () -> CONVERT.apply(
            DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType),
            BSON_DOCUMENT.get("mySubDoc")));
  }

  @Test
  @DisplayName("test struct support")
  void testStructSupport() {

    StructType subDocumentStructType = DataTypes.createStructType(
        singletonList(DataTypes.createStructField("D", DataTypes.StringType, true)));
    StructType structType = DataTypes.createStructType(asList(
        DataTypes.createStructField("A", DataTypes.StringType, false),
        DataTypes.createStructField("B", DataTypes.StringType, true),
        DataTypes.createStructField("C", subDocumentStructType, true)));

    GenericRowWithSchema genericRowWithSchema = new GenericRowWithSchema(
        asList(
                "S2Fma2Egcm9ja3Mh",
                "2020-01-01T07:27:07Z",
                new GenericRowWithSchema(
                    singletonList("12345.6789").toArray(), subDocumentStructType))
            .toArray(),
        structType);

    assertEquals(
        genericRowWithSchema, CONVERT.apply(structType, BSON_DOCUMENT.getDocument("mySubDoc")));

    GenericRowWithSchema genericRowWithSchemaWithNull = new GenericRowWithSchema(
        asList("S2Fma2Egcm9ja3Mh", "2020-01-01T07:27:07Z", null).toArray(), structType);

    BsonDocument mySubDoc = BsonDocument.parse(SUB_BSON_DOCUMENT_JSON);
    mySubDoc.remove("C");
    assertEquals(genericRowWithSchemaWithNull, CONVERT.apply(structType, mySubDoc));

    Set<String> invalidKeys = BSON_DOCUMENT.keySet().stream()
        .filter(k -> !k.equals("mySubDoc"))
        .collect(Collectors.toSet());

    invalidKeys.forEach(k -> assertThrows(
        DataException.class,
        () -> CONVERT.apply(structType, BSON_DOCUMENT.get(k)),
        format("Expected %s to fail", k)));

    BsonDocument invalidDoc = BsonDocument.parse(SUB_BSON_DOCUMENT_JSON);
    invalidDoc.remove("A");
    assertThrows(DataException.class, () -> CONVERT.apply(structType, invalidDoc));
  }

  @Test
  @DisplayName("test fromBsonDocument")
  void testFromBsonDocument() {
    BsonDocumentToRowConverter bsonDocumentToRowConverter =
        new BsonDocumentToRowConverter(ALL_TYPES_ROW.schema(), false);
    GenericRowWithSchema actual = bsonDocumentToRowConverter.toRow(BSON_DOCUMENT_ALL_TYPES);

    assertRows(ALL_TYPES_ROW, actual);
  }

  @Test
  @DisplayName("test unsupported types")
  void testUnsupportedTypes() {
    assertThrows(
        DataException.class,
        () -> CONVERT.apply(DataTypes.CalendarIntervalType, new BsonInt32(123)));
    assertThrows(
        DataException.class, () -> CONVERT.apply(DataTypes.BinaryType, new BsonInt32(123)));
  }

  private void assertRows(final GenericRowWithSchema expected, final GenericRowWithSchema actual) {
    assertEquals(expected.schema(), actual.schema());
    for (String fieldName : expected.schema().fieldNames()) {
      Object expectedValue = expected.values()[expected.schema().fieldIndex(fieldName)];
      Object actualValue = actual.values()[actual.schema().fieldIndex(fieldName)];
      assertRowValues(expectedValue, actualValue);
    }
  }

  private void assertRowValues(final Object expectedValue, final Object actualValue) {
    if (expectedValue == null || actualValue == null) {
      assertEquals(expectedValue, actualValue);
    } else if (expectedValue instanceof GenericRowWithSchema
        && actualValue instanceof GenericRowWithSchema) {
      assertRows((GenericRowWithSchema) expectedValue, (GenericRowWithSchema) actualValue);
    } else if (expectedValue.getClass() == byte[].class) {
      assertArrayEquals((byte[]) expectedValue, (byte[]) actualValue);
    } else if (expectedValue.getClass() == Object[].class) {
      Object[] expectedValues = (Object[]) expectedValue;
      Object[] actualValues = (Object[]) actualValue;
      assertEquals(
          expectedValues.length, actualValues.length, "Different lengths for array of Objects");
      for (int i = 0; i < expectedValues.length; i++) {
        assertRowValues(expectedValues[i], actualValues[i]);
      }
    } else {
      assertEquals(expectedValue, actualValue);
    }
  }
}
