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
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.types.Decimal128;

import com.mongodb.spark.sql.connector.exceptions.DataException;

public class BsonDocumentToRowConverterTest {

  private static final String SUB_DOCUMENT_JSON =
      "{\"A\": {\"$binary\": {\"base64\": \"S2Fma2Egcm9ja3Mh\", \"subType\": \"00\"}},"
          + " \"B\": {\"$date\": {\"$numberLong\": \"1577863627000\"}},"
          + " \"C\": {\"D\": \"12345.6789\"}}";
  private static final String DOCUMENT_JSON =
      "{\"_id\": {\"$oid\": \"5f15aab12435743f9bd126a4\"},"
          + " \"myString\": \"some foo bla text\","
          + " \"myInt\": {\"$numberInt\": \"42\"},"
          + " \"myDouble\": {\"$numberDouble\": \"20.21\"},"
          + " \"mySubDoc\": "
          + SUB_DOCUMENT_JSON
          + ","
          + " \"myArray\": [{\"$numberInt\": \"1\"}, {\"$numberInt\": \"2\"}, {\"$numberInt\": \"3\"}],"
          + " \"myBytes\": {\"$binary\": {\"base64\": \"S2Fma2Egcm9ja3Mh\", \"subType\": \"00\"}},"
          + " \"myDate\": {\"$date\": {\"$numberLong\": \"1234567890\"}},"
          + " \"myDecimal\": {\"$numberDecimal\": \"12345.6789\"}"
          + "}";

  private static final String BSON_DOCUMENT_ALL_TYPES_JSON =
      "{"
          + "\"arrayEmpty\": [], "
          + "\"arraySimple\": [{\"$numberInt\": \"1\"}, {\"$numberInt\": \"2\"}, {\"$numberInt\": \"3\"}], "
          + "\"arrayComplex\": [{\"a\": {\"$numberInt\": \"1\"}}, {\"a\": {\"$numberInt\": \"2\"}}], "
          + "\"arrayMixedTypes\": [{\"$numberInt\": \"1\"}, {\"$numberInt\": \"2\"}, true,"
          + " [{\"$numberInt\": \"1\"}, {\"$numberInt\": \"2\"}, {\"$numberInt\": \"3\"}],"
          + " {\"a\": {\"$numberInt\": \"2\"}}], "
          + "\"arrayComplexMixedTypes\": [{\"a\": {\"$numberInt\": \"1\"}}, {\"a\": \"a\"}], "
          + "\"binary\": {\"$binary\": {\"base64\": \"S2Fma2Egcm9ja3Mh\", \"subType\": \"00\"}}, "
          + "\"boolean\": true, "
          + "\"code\": {\"$code\": \"int i = 0;\"}, "
          + "\"codeWithScope\": {\"$code\": \"int x = y\", \"$scope\": {\"y\": {\"$numberInt\": \"1\"}}}, "
          + "\"dateTime\": {\"$date\": {\"$numberLong\": \"1577836801000\"}}, "
          + "\"decimal128\": {\"$numberDecimal\": \"1.0\"}, "
          + "\"document\": {\"a\": {\"$numberInt\": \"1\"}}, "
          + "\"double\": {\"$numberDouble\": \"62.0\"}, "
          + "\"int32\": {\"$numberInt\": \"42\"}, "
          + "\"int64\": {\"$numberLong\": \"52\"}, "
          + "\"maxKey\": {\"$maxKey\": 1}, "
          + "\"minKey\": {\"$minKey\": 1}, "
          + "\"null\": null, "
          + "\"objectId\": {\"$oid\": \"5f3d1bbde0ca4d2829c91e1d\"}, "
          + "\"regex\": {\"$regularExpression\": {\"pattern\": \"^test.*regex.*xyz$\", \"options\": \"i\"}}, "
          + "\"string\": \"the fox ...\", "
          + "\"symbol\": {\"$symbol\": \"ruby stuff\"}, "
          + "\"timestamp\": {\"$timestamp\": {\"t\": 305419896, \"i\": 5}}, "
          + "\"undefined\": {\"$undefined\": true}"
          + "}";

  private static final BsonDocument BSON_DOCUMENT_ALL_TYPES =
      RawBsonDocument.parse(BSON_DOCUMENT_ALL_TYPES_JSON);
  private static final BsonDocument BSON_DOCUMENT = RawBsonDocument.parse(DOCUMENT_JSON);
  private static final BsonDocumentToRowConverter CONVERTER = new BsonDocumentToRowConverter();

  private static final BiFunction<DataType, BsonValue, Object> CONVERT =
      (dataType, bsonValue) ->
          CONVERTER.convertBsonValue(
              "", dataType, bsonValue, ConverterHelper.DEFAULT_JSON_WRITER_SETTINGS);
  private static final DataType DECIMAL_TYPE = DataTypes.createDecimalType();

  @Test
  @DisplayName("test string support")
  void testStringSupport() {
    Map<String, String> expected =
        new HashMap<String, String>() {
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
          }
        };

    BSON_DOCUMENT.forEach(
        (k, v) -> assertEquals(expected.get(k), CONVERT.apply(DataTypes.StringType, v)));
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
        () -> assertEquals((byte) 987654321.54321, CONVERT.apply(DataTypes.ByteType, bsonString)));

    assertAll(
        "Testing int16 support",
        () -> assertEquals((short) 42, CONVERT.apply(DataTypes.ShortType, bsonInt32)),
        () -> assertEquals((short) 2020, CONVERT.apply(DataTypes.ShortType, bsonInt64)),
        () -> assertEquals((short) 20.20, CONVERT.apply(DataTypes.ShortType, bsonDouble)),
        () ->
            assertEquals((short) 1234567890000L, CONVERT.apply(DataTypes.ShortType, bsonDateTime)),
        () ->
            assertEquals((short) 1234567890000L, CONVERT.apply(DataTypes.ShortType, bsonTimestamp)),
        () ->
            assertEquals(
                (short) 1234567890.1234, CONVERT.apply(DataTypes.ShortType, bsonDecimal128)),
        () ->
            assertEquals((short) 987654321.54321, CONVERT.apply(DataTypes.ShortType, bsonString)));

    assertAll(
        "Testing int32 support",
        () -> assertEquals(42, CONVERT.apply(DataTypes.IntegerType, bsonInt32)),
        () -> assertEquals(2020, CONVERT.apply(DataTypes.IntegerType, bsonInt64)),
        () -> assertEquals((int) 20.20, CONVERT.apply(DataTypes.IntegerType, bsonDouble)),
        () ->
            assertEquals((int) 1234567890000L, CONVERT.apply(DataTypes.IntegerType, bsonDateTime)),
        () ->
            assertEquals((int) 1234567890000L, CONVERT.apply(DataTypes.IntegerType, bsonTimestamp)),
        () ->
            assertEquals(
                (int) 1234567890.1234, CONVERT.apply(DataTypes.IntegerType, bsonDecimal128)),
        () ->
            assertEquals((int) 987654321.54321, CONVERT.apply(DataTypes.IntegerType, bsonString)));

    assertAll(
        "Testing int64 support",
        () -> assertEquals((long) 42, CONVERT.apply(DataTypes.LongType, bsonInt32)),
        () -> assertEquals((long) 2020, CONVERT.apply(DataTypes.LongType, bsonInt64)),
        () -> assertEquals((long) 20.20, CONVERT.apply(DataTypes.LongType, bsonDouble)),
        () -> assertEquals(1234567890000L, CONVERT.apply(DataTypes.LongType, bsonDateTime)),
        () -> assertEquals(1234567890000L, CONVERT.apply(DataTypes.LongType, bsonTimestamp)),
        () ->
            assertEquals((long) 1234567890.1234, CONVERT.apply(DataTypes.LongType, bsonDecimal128)),
        () -> assertEquals((long) 987654321.54321, CONVERT.apply(DataTypes.LongType, bsonString)));

    assertAll(
        "Testing float32 support",
        () -> assertEquals((float) 42, CONVERT.apply(DataTypes.FloatType, bsonInt32)),
        () -> assertEquals((float) 2020, CONVERT.apply(DataTypes.FloatType, bsonInt64)),
        () -> assertEquals((float) 20.20, CONVERT.apply(DataTypes.FloatType, bsonDouble)),
        () ->
            assertEquals((float) 1234567890000L, CONVERT.apply(DataTypes.FloatType, bsonDateTime)),
        () ->
            assertEquals((float) 1234567890000L, CONVERT.apply(DataTypes.FloatType, bsonTimestamp)),
        () ->
            assertEquals(
                (float) 1234567890.1234, CONVERT.apply(DataTypes.FloatType, bsonDecimal128)),
        () ->
            assertEquals((float) 987654321.54321, CONVERT.apply(DataTypes.FloatType, bsonString)));

    assertAll(
        "Testing float64 support",
        () -> assertEquals((double) 42, CONVERT.apply(DataTypes.DoubleType, bsonInt32)),
        () -> assertEquals((double) 2020, CONVERT.apply(DataTypes.DoubleType, bsonInt64)),
        () -> assertEquals(20.20, CONVERT.apply(DataTypes.DoubleType, bsonDouble)),
        () ->
            assertEquals(
                (double) 1234567890000L, CONVERT.apply(DataTypes.DoubleType, bsonDateTime)),
        () ->
            assertEquals(
                (double) 1234567890000L, CONVERT.apply(DataTypes.DoubleType, bsonTimestamp)),
        () -> assertEquals(1234567890.1234, CONVERT.apply(DataTypes.DoubleType, bsonDecimal128)),
        () -> assertEquals(987654321.54321, CONVERT.apply(DataTypes.DoubleType, bsonString)));

    assertAll(
        "Testing decimal support",
        () -> assertEquals(BigDecimal.valueOf(42), CONVERT.apply(DECIMAL_TYPE, bsonInt32)),
        () -> assertEquals(BigDecimal.valueOf(2020), CONVERT.apply(DECIMAL_TYPE, bsonInt64)),
        () -> assertEquals(BigDecimal.valueOf(20.20), CONVERT.apply(DECIMAL_TYPE, bsonDouble)),
        () ->
            assertEquals(
                BigDecimal.valueOf(1234567890000L), CONVERT.apply(DECIMAL_TYPE, bsonDateTime)),
        () ->
            assertEquals(
                BigDecimal.valueOf(1234567890000L), CONVERT.apply(DECIMAL_TYPE, bsonTimestamp)),
        () ->
            assertEquals(
                bsonDecimal128.decimal128Value().bigDecimalValue(),
                CONVERT.apply(DECIMAL_TYPE, bsonDecimal128)),
        () ->
            assertEquals(
                new BigDecimal("987654321.54321"), CONVERT.apply(DECIMAL_TYPE, bsonString)));

    List<String> validKeys = asList("myInt", "myDouble", "myDate", "myDecimal");
    Set<String> invalidKeys =
        BSON_DOCUMENT.keySet().stream()
            .filter(k -> !validKeys.contains(k))
            .collect(Collectors.toSet());

    invalidKeys.forEach(
        k ->
            assertThrows(
                DataException.class,
                () -> CONVERT.apply(DataTypes.DoubleType, BSON_DOCUMENT.get(k)),
                format("Expected %s to fail", k)));
  }

  @Test
  @DisplayName("test date and timestamp support")
  void testDateAndTimestampSupport() {
    int oneDay = 86400000;
    int oneDayAnd1Hour = oneDay + 3600000;

    BsonInt32 bsonInt32 = new BsonInt32(oneDayAnd1Hour);
    BsonInt64 bsonInt64 = new BsonInt64(oneDayAnd1Hour * 2L);
    BsonDouble bsonDouble = new BsonDouble(oneDayAnd1Hour * 3L);
    BsonDateTime bsonDateTime = new BsonDateTime(oneDayAnd1Hour * 4L);
    BsonTimestamp bsonTimestamp = new BsonTimestamp((int) (oneDayAnd1Hour * 5L / 1000), 1);
    BsonDecimal128 bsonDecimal128 = new BsonDecimal128(Decimal128.parse(oneDayAnd1Hour * 7 + ""));

    assertAll(
        "Testing date support",
        () -> assertEquals(new Date(oneDay), CONVERT.apply(DataTypes.DateType, bsonInt32)),
        () -> assertEquals(new Date(oneDay * 2L), CONVERT.apply(DataTypes.DateType, bsonInt64)),
        () -> assertEquals(new Date(oneDay * 3L), CONVERT.apply(DataTypes.DateType, bsonDouble)),
        () -> assertEquals(new Date(oneDay * 4L), CONVERT.apply(DataTypes.DateType, bsonDateTime)),
        () -> assertEquals(new Date(oneDay * 5L), CONVERT.apply(DataTypes.DateType, bsonTimestamp)),
        () ->
            assertEquals(new Date(oneDay * 7L), CONVERT.apply(DataTypes.DateType, bsonDecimal128)));

    assertAll(
        "Testing datetime support",
        () ->
            assertEquals(
                new Date(oneDayAnd1Hour), CONVERT.apply(DataTypes.TimestampType, bsonInt32)),
        () ->
            assertEquals(
                new Date(oneDayAnd1Hour * 2L), CONVERT.apply(DataTypes.TimestampType, bsonInt64)),
        () ->
            assertEquals(
                new Date(oneDayAnd1Hour * 3L), CONVERT.apply(DataTypes.TimestampType, bsonDouble)),
        () ->
            assertEquals(
                new Date(oneDayAnd1Hour * 4L),
                CONVERT.apply(DataTypes.TimestampType, bsonDateTime)),
        () ->
            assertEquals(
                new Date(oneDayAnd1Hour * 5L),
                CONVERT.apply(DataTypes.TimestampType, bsonTimestamp)),
        () ->
            assertEquals(
                new Date(oneDayAnd1Hour * 7L),
                CONVERT.apply(DataTypes.TimestampType, bsonDecimal128)));

    List<String> validKeys = asList("myInt", "myDouble", "myDate", "myDecimal");
    Set<String> invalidKeys =
        BSON_DOCUMENT.keySet().stream()
            .filter(k -> !validKeys.contains(k))
            .collect(Collectors.toSet());

    invalidKeys.forEach(
        k ->
            assertThrows(
                DataException.class,
                () -> CONVERT.apply(DataTypes.DateType, BSON_DOCUMENT.get(k)),
                format("Expected %s to fail", k)));

    invalidKeys.forEach(
        k ->
            assertThrows(
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
        () -> assertEquals(false, CONVERT.apply(DataTypes.BooleanType, BsonBoolean.FALSE)));

    Set<String> invalidKeys =
        BSON_DOCUMENT.keySet().stream()
            .filter(k -> !k.equals("myBoolean"))
            .collect(Collectors.toSet());

    invalidKeys.forEach(
        k ->
            assertThrows(
                DataException.class,
                () -> CONVERT.apply(DataTypes.BooleanType, BSON_DOCUMENT.get(k)),
                format("Expected %s to fail", k)));
  }

  @Test
  @DisplayName("test bytes support")
  void testBytesSupport() {

    assertAll(
        "Testing bytes support",
        () ->
            assertArrayEquals(
                BSON_DOCUMENT
                    .get("myString")
                    .asString()
                    .getValue()
                    .getBytes(StandardCharsets.UTF_8),
                (byte[]) CONVERT.apply(DataTypes.BinaryType, BSON_DOCUMENT.get("myString"))),
        () ->
            assertArrayEquals(
                BSON_DOCUMENT.getBinary("myBytes").getData(),
                (byte[]) CONVERT.apply(DataTypes.BinaryType, BSON_DOCUMENT.getBinary("myBytes"))),
        () ->
            assertArrayEquals(
                BsonDocumentToRowConverter.documentToByteArray(BSON_DOCUMENT),
                (byte[]) CONVERT.apply(DataTypes.BinaryType, BSON_DOCUMENT)),
        () ->
            assertArrayEquals(
                BsonDocumentToRowConverter.documentToByteArray(BSON_DOCUMENT),
                (byte[]) CONVERT.apply(DataTypes.BinaryType, BsonDocument.parse(DOCUMENT_JSON))));

    List<String> validKeys = asList("myString", "myBytes", "mySubDoc");
    Set<String> invalidKeys =
        BSON_DOCUMENT.keySet().stream()
            .filter(k -> !validKeys.contains(k))
            .collect(Collectors.toSet());

    invalidKeys.forEach(
        k ->
            assertThrows(
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
    DataType dataType = DataTypes.createArrayType(DataTypes.IntegerType);
    assertIterableEquals(
        asList(1, 2, 3), (Iterable<?>) CONVERT.apply(dataType, BSON_DOCUMENT.get("myArray")));

    Set<String> invalidKeys =
        BSON_DOCUMENT.keySet().stream()
            .filter(k -> !k.equals("myArray"))
            .collect(Collectors.toSet());

    invalidKeys.forEach(
        k ->
            assertThrows(
                DataException.class,
                () -> CONVERT.apply(dataType, BSON_DOCUMENT.get(k)),
                format("Expected %s to fail", k)));
  }

  @Test
  @DisplayName("test Map support")
  void testMapSupport() {
    DataType dataType = DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType);

    assertEquals(
        new LinkedHashMap<String, String>() {
          {
            put("A", "S2Fma2Egcm9ja3Mh");
            put("B", "2020-01-01T07:27:07Z");
            put("C", "{\"D\": \"12345.6789\"}");
          }
        },
        CONVERT.apply(dataType, BSON_DOCUMENT.get("mySubDoc")));

    Set<String> invalidKeys =
        BSON_DOCUMENT.keySet().stream()
            .filter(k -> !k.equals("mySubDoc"))
            .collect(Collectors.toSet());

    invalidKeys.forEach(
        k ->
            assertThrows(
                DataException.class,
                () -> CONVERT.apply(dataType, BSON_DOCUMENT.get(k)),
                format("Expected %s to fail", k)));

    assertThrows(
        DataException.class,
        () ->
            CONVERT.apply(
                DataTypes.createMapType(DataTypes.IntegerType, DataTypes.StringType),
                BSON_DOCUMENT.get("mySubDoc")));
    assertThrows(
        DataException.class,
        () ->
            CONVERT.apply(
                DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType),
                BSON_DOCUMENT.get("mySubDoc")));
  }

  @Test
  @DisplayName("test struct support")
  void testStructSupport() {

    StructType subDocumentStructType =
        DataTypes.createStructType(
            singletonList(DataTypes.createStructField("D", DataTypes.StringType, true)));
    StructType structType =
        DataTypes.createStructType(
            asList(
                DataTypes.createStructField("A", DataTypes.StringType, false),
                DataTypes.createStructField("B", DataTypes.StringType, true),
                DataTypes.createStructField("C", subDocumentStructType, true)));

    GenericRowWithSchema genericRowWithSchema =
        new GenericRowWithSchema(
            asList(
                    "S2Fma2Egcm9ja3Mh",
                    "2020-01-01T07:27:07Z",
                    new GenericRowWithSchema(
                        singletonList("12345.6789").toArray(), subDocumentStructType))
                .toArray(),
            structType);

    assertEquals(
        genericRowWithSchema, CONVERT.apply(structType, BSON_DOCUMENT.getDocument("mySubDoc")));

    GenericRowWithSchema genericRowWithSchemaWithNull =
        new GenericRowWithSchema(
            asList("S2Fma2Egcm9ja3Mh", "2020-01-01T07:27:07Z", null).toArray(), structType);

    BsonDocument mySubDoc = BsonDocument.parse(SUB_DOCUMENT_JSON);
    mySubDoc.remove("C");
    assertEquals(genericRowWithSchemaWithNull, CONVERT.apply(structType, mySubDoc));

    Set<String> invalidKeys =
        BSON_DOCUMENT.keySet().stream()
            .filter(k -> !k.equals("mySubDoc"))
            .collect(Collectors.toSet());

    invalidKeys.forEach(
        k ->
            assertThrows(
                DataException.class,
                () -> CONVERT.apply(structType, BSON_DOCUMENT.get(k)),
                format("Expected %s to fail", k)));

    BsonDocument invalidDoc = BsonDocument.parse(SUB_DOCUMENT_JSON);
    invalidDoc.remove("A");
    assertThrows(DataException.class, () -> CONVERT.apply(structType, invalidDoc));
  }

  @Test
  @DisplayName("test getDataType")
  void testGetDataType() {
    assertAll(
        () ->
            assertEquals(
                DataTypes.createArrayType(DataTypes.StringType, true),
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("arrayEmpty")),
                "arrayEmpty failed"),
        () ->
            assertEquals(
                DataTypes.createArrayType(DataTypes.IntegerType, true),
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("arraySimple")),
                "arraySimple failed"),
        () ->
            assertEquals(
                DataTypes.createArrayType(
                    DataTypes.createStructType(
                        singletonList(
                            DataTypes.createStructField("a", DataTypes.IntegerType, true)))),
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("arrayComplex")),
                "arraySimple failed"),
        () ->
            assertEquals(
                DataTypes.createArrayType(DataTypes.StringType, true),
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("arrayMixedTypes")),
                "arrayMixedTypes failed"),
        () ->
            assertEquals(
                DataTypes.createArrayType(
                    DataTypes.createStructType(
                        singletonList(
                            DataTypes.createStructField("a", DataTypes.StringType, true)))),
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("arrayComplexMixedTypes")),
                "arrayComplexMixedTypes failed"),
        () ->
            assertEquals(
                DataTypes.BinaryType,
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("binary")),
                "binary failed"),
        () ->
            assertEquals(
                DataTypes.BooleanType,
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("boolean")),
                "boolean failed"),
        () ->
            assertEquals(
                DataTypes.StringType,
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("code")),
                "code failed"),
        () ->
            assertEquals(
                DataTypes.StringType,
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("codeWithScope")),
                "codeWithScope failed"),
        () ->
            assertEquals(
                DataTypes.TimestampType,
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("dateTime")),
                "dateTime failed"),
        () ->
            assertEquals(
                DataTypes.createDecimalType(2, 1),
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("decimal128")),
                "decimal128 failed"),
        () ->
            assertEquals(
                DataTypes.createStructType(
                    singletonList(DataTypes.createStructField("a", DataTypes.IntegerType, true))),
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("document")),
                "document failed"),
        () ->
            assertEquals(
                DataTypes.DoubleType,
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("double")),
                "double failed"),
        () ->
            assertEquals(
                DataTypes.IntegerType,
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("int32")),
                "int32 failed"),
        () ->
            assertEquals(
                DataTypes.LongType,
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("int64")),
                "int64 failed"),
        () ->
            assertEquals(
                DataTypes.StringType,
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("maxKey")),
                "maxKey failed"),
        () ->
            assertEquals(
                DataTypes.StringType,
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("minKey")),
                "minKey failed"),
        () ->
            assertEquals(
                DataTypes.NullType,
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("null")),
                "null failed"),
        () ->
            assertEquals(
                DataTypes.StringType,
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("objectId")),
                "objectId failed"),
        () ->
            assertEquals(
                DataTypes.StringType,
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("regex")),
                "regex failed"),
        () ->
            assertEquals(
                DataTypes.StringType,
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("string")),
                "string failed"),
        () ->
            assertEquals(
                DataTypes.StringType,
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("symbol")),
                "symbol failed"),
        () ->
            assertEquals(
                DataTypes.TimestampType,
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("timestamp")),
                "timestamp failed"),
        () ->
            assertEquals(
                DataTypes.StringType,
                CONVERTER.getDataType(BSON_DOCUMENT_ALL_TYPES.get("undefined")),
                "undefined failed"));
  }

  @Test
  @DisplayName("test fromBsonDocument")
  void testFromBsonDocument() {
    GenericRowWithSchema subSubDocRow =
        new GenericRowWithSchema(
            singletonList("12345.6789").toArray(), new StructType().add("D", DataTypes.StringType));
    GenericRowWithSchema subDocRow =
        new GenericRowWithSchema(
            asList(
                    BSON_DOCUMENT.getDocument("mySubDoc").getBinary("A").getData(),
                    new Date(1577863627000L),
                    subSubDocRow)
                .toArray(),
            new StructType()
                .add("A", DataTypes.BinaryType, true)
                .add("B", DataTypes.TimestampType)
                .add("C", subSubDocRow.schema()));

    StructType schema =
        new StructType()
            .add("_id", DataTypes.StringType, true)
            .add("myString", DataTypes.StringType, true)
            .add("myInt", DataTypes.IntegerType, true)
            .add("myDouble", DataTypes.DoubleType, true)
            .add("mySubDoc", subDocRow.schema(), true)
            .add("myArray", DataTypes.createArrayType(DataTypes.IntegerType, true), true)
            .add("myBytes", DataTypes.BinaryType, true)
            .add("myDate", DataTypes.TimestampType, true)
            .add("myDecimal", DataTypes.createDecimalType(), true);

    BsonDocumentToRowConverter bsonDocumentToRowConverter = new BsonDocumentToRowConverter(schema);
    List<Object> values =
        asList(
            "5f15aab12435743f9bd126a4",
            "some foo bla text",
            42,
            20.21,
            subDocRow,
            asList(1, 2, 3),
            BSON_DOCUMENT.getBinary("myBytes").getData(),
            new Date(BSON_DOCUMENT.getDateTime("myDate").getValue()),
            BSON_DOCUMENT.getDecimal128("myDecimal").decimal128Value().bigDecimalValue());

    assertEquals(
        new GenericRowWithSchema(values.toArray(), schema),
        bsonDocumentToRowConverter.toRow(BSON_DOCUMENT));
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
}
