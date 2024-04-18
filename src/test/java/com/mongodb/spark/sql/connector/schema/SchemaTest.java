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

import static com.mongodb.spark.sql.connector.schema.InferSchema.INFERRED_METADATA;
import static com.mongodb.spark.sql.connector.schema.InferSchema.PLACE_HOLDER_ARRAY_TYPE;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;

abstract class SchemaTest {

  static final BsonDocument SIMPLE_BSON_DOCUMENT = BsonDocument.parse("{"
      + "'binaryType': {'$binary': {'base64': 'YWJj', 'subType': '00'}}, "
      + "'booleanType': true, "
      + "'byteType': 1, "
      + "'dateType': {'$date': '1970-01-01T01:00:00Z'}, "
      + "'doubleType': 2.0, "
      + "'floatType': 3.0, "
      + "'integerType': 5, "
      + "'longType': {'$numberLong': '6'}, "
      + "'nullType': null,"
      + "'shortType': 7, "
      + "'stringType': 'string', "
      + "'timestampType': {'$date': '1970-01-01T05:00:00Z'}"
      + "}");

  static final Row SIMPLE_ROW = new GenericRowWithSchema(
      asList(
              "abc".getBytes(StandardCharsets.UTF_8),
              true,
              (byte) 1,
              new Timestamp(3600000L),
              2.0,
              3.0f,
              5,
              6L,
              null,
              (short) 7,
              "string",
              new Date(18000000L))
          .toArray(),
      new StructType()
          .add("binaryType", DataTypes.BinaryType, true)
          .add("booleanType", DataTypes.BooleanType, true)
          .add("byteType", DataTypes.ByteType, true)
          .add("dateType", DataTypes.DateType, true)
          .add("doubleType", DataTypes.DoubleType, true)
          .add("floatType", DataTypes.FloatType, true)
          .add("integerType", DataTypes.IntegerType, true)
          .add("longType", DataTypes.LongType, true)
          .add("nullType", DataTypes.NullType, false)
          .add("shortType", DataTypes.ShortType, true)
          .add("stringType", DataTypes.StringType, true)
          .add("timestampType", DataTypes.TimestampType, true));

  static final Row SIMPLE_NULL_ROW = new GenericRowWithSchema(
      asList(null, null, null, null, null, null, null, null, null, null, null, null)
          .toArray(),
      new StructType()
          .add("binaryType", DataTypes.BinaryType, true)
          .add("booleanType", DataTypes.BooleanType, true)
          .add("byteType", DataTypes.ByteType, true)
          .add("dateType", DataTypes.DateType, true)
          .add("doubleType", DataTypes.DoubleType, true)
          .add("floatType", DataTypes.FloatType, true)
          .add("integerType", DataTypes.IntegerType, true)
          .add("longType", DataTypes.LongType, true)
          .add("nullType", DataTypes.NullType, false)
          .add("shortType", DataTypes.ShortType, true)
          .add("stringType", DataTypes.StringType, true)
          .add("timestampType", DataTypes.TimestampType, true));

  static final BsonDocument SIMPLE_NULL_BSON_DOCUMENT = BsonDocument.parse("{"
      + "'binaryType': null, "
      + "'booleanType': null, "
      + "'byteType': null, "
      + "'dateType': null, "
      + "'doubleType': null, "
      + "'floatType': null, "
      + "'integerType': null, "
      + "'longType': null, "
      + "'nullType': null,"
      + "'shortType': null, "
      + "'stringType': null, "
      + "'timestampType': null"
      + "}");

  static final String SUB_BSON_DOCUMENT_JSON =
      "{\"A\": {\"$binary\": {\"base64\": \"S2Fma2Egcm9ja3Mh\", \"subType\": \"00\"}},"
          + " \"B\": {\"$date\": {\"$numberLong\": \"1577863627000\"}},"
          + " \"C\": {\"D\": \"12345.6789\"}}";
  static final String BSON_DOCUMENT_JSON = "{\"_id\": {\"$oid\": \"5f15aab12435743f9bd126a4\"},"
      + " \"myString\": \"some foo bla text\","
      + " \"myInt\": {\"$numberInt\": \"42\"},"
      + " \"myDouble\": {\"$numberDouble\": \"20.21\"},"
      + " \"mySubDoc\": "
      + SUB_BSON_DOCUMENT_JSON
      + ","
      + " \"myArray\": [{\"$numberInt\": \"1\"}, {\"$numberInt\": \"2\"}, {\"$numberInt\": \"3\"}],"
      + " \"myBytes\": {\"$binary\": {\"base64\": \"S2Fma2Egcm9ja3Mh\", \"subType\": \"00\"}},"
      + " \"myDate\": {\"$date\": {\"$numberLong\": \"1234567890\"}},"
      + " \"myDecimal\": {\"$numberDecimal\": \"12345.6789\"}"
      + "}";
  static final BsonDocument BSON_DOCUMENT = RawBsonDocument.parse(BSON_DOCUMENT_JSON);

  static final String BSON_DOCUMENT_ALL_TYPES_JSON = "{"
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
      + "\"codeWithScope\": {\"$code\": \"int x = y;\", \"$scope\": {\"y\": {\"$numberInt\": \"1\"}}}, "
      + "\"dateTime\": {\"$date\": {\"$numberLong\": \"1577836801000\"}}, "
      + "\"decimal128\": {\"$numberDecimal\": \"1.0\"}, "
      + "\"documentEmpty\": {},"
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

  static final BsonDocument BSON_DOCUMENT_ALL_TYPES =
      RawBsonDocument.parse(BSON_DOCUMENT_ALL_TYPES_JSON);

  static final StructType BSON_DOCUMENT_ALL_TYPES_SCHEMA = DataTypes.createStructType(asList(
      createStructField("arrayEmpty", DataTypes.createArrayType(DataTypes.StringType, true)),
      createStructField("arraySimple", DataTypes.createArrayType(DataTypes.IntegerType, true)),
      createStructField(
          "arrayComplex",
          DataTypes.createArrayType(DataTypes.createStructType(
              singletonList(createStructField("a", DataTypes.IntegerType))))),
      createStructField("arrayMixedTypes", DataTypes.createArrayType(DataTypes.StringType, true)),
      createStructField(
          "arrayComplexMixedTypes",
          DataTypes.createArrayType(DataTypes.createStructType(
              singletonList(createStructField("a", DataTypes.StringType))))),
      createStructField("binary", DataTypes.BinaryType),
      createStructField("boolean", DataTypes.BooleanType),
      createStructField("code", DataTypes.StringType),
      createStructField("codeWithScope", DataTypes.StringType),
      createStructField("dateTime", DataTypes.TimestampType),
      createStructField("decimal128", DataTypes.createDecimalType(2, 1)),
      createStructField("documentEmpty", DataTypes.createStructType(emptyList())),
      createStructField(
          "document",
          DataTypes.createStructType(singletonList(createStructField("a", DataTypes.IntegerType)))),
      createStructField("double", DataTypes.DoubleType),
      createStructField("int32", DataTypes.IntegerType),
      createStructField("int64", DataTypes.LongType),
      createStructField("maxKey", DataTypes.StringType),
      createStructField("minKey", DataTypes.StringType),
      createStructField("null", DataTypes.NullType),
      createStructField("objectId", DataTypes.StringType),
      createStructField("regex", DataTypes.StringType),
      createStructField("string", DataTypes.StringType),
      createStructField("symbol", DataTypes.StringType),
      createStructField("timestamp", DataTypes.TimestampType),
      createStructField("undefined", DataTypes.StringType)));

  static final GenericRowWithSchema ALL_TYPES_ROW = new GenericRowWithSchema(
      asList(
              Collections.<String>emptyList().toArray(),
              asList(1, 2, 3).toArray(),
              asList(
                      new GenericRowWithSchema(
                          singletonList(1).toArray(),
                          DataTypes.createStructType(
                              singletonList(createStructField("a", DataTypes.IntegerType)))),
                      new GenericRowWithSchema(
                          singletonList(2).toArray(),
                          DataTypes.createStructType(
                              singletonList(createStructField("a", DataTypes.IntegerType)))))
                  .toArray(),
              asList("1", "2", "true", "[1, 2, 3]", "{\"a\": 2}").toArray(),
              asList(
                      new GenericRowWithSchema(
                          singletonList("1").toArray(),
                          DataTypes.createStructType(
                              singletonList(createStructField("a", DataTypes.StringType)))),
                      new GenericRowWithSchema(
                          singletonList("a").toArray(),
                          DataTypes.createStructType(
                              singletonList(createStructField("a", DataTypes.StringType)))))
                  .toArray(),
              new byte[] {75, 97, 102, 107, 97, 32, 114, 111, 99, 107, 115, 33},
              true,
              "{\"$code\": \"int i = 0;\"}",
              "{\"$code\": \"int x = y;\", \"$scope\": {\"y\": 1}}",
              new Timestamp(1577836801000L),
              new BigDecimal("1.0"),
              new GenericRowWithSchema(
                  emptyList().toArray(), DataTypes.createStructType(emptyList())),
              new GenericRowWithSchema(
                  singletonList(1).toArray(),
                  DataTypes.createStructType(
                      singletonList(createStructField("a", DataTypes.IntegerType)))),
              62.0,
              42,
              52L,
              "{\"$maxKey\": 1}",
              "{\"$minKey\": 1}",
              null,
              "5f3d1bbde0ca4d2829c91e1d",
              "{\"$regularExpression\": {\"pattern\": \"^test.*regex.*xyz$\", \"options\": \"i\"}}",
              "the fox ...",
              "ruby stuff",
              new Timestamp(305419896000L),
              "{\"$undefined\": true}")
          .toArray(),
      BSON_DOCUMENT_ALL_TYPES_SCHEMA);

  static final StructType BSON_DOCUMENT_STRING_SCHEMA =
      DataTypes.createStructType(Arrays.stream(BSON_DOCUMENT_ALL_TYPES_SCHEMA.fields())
          .map(f -> DataTypes.createStructField(f.name(), DataTypes.StringType, f.nullable()))
          .collect(Collectors.toList()));

  // Relaxed JSON Schema
  static final BsonDocument BSON_DOCUMENT_RELAXED = RawBsonDocument.parse("{"
      + " \"arrayEmpty\": [],"
      + " \"arraySimple\": [ 1, 2, 3 ],"
      + " \"arrayComplex\": [ {\"a\": 1 }, {\"a\": 2 } ],"
      + " \"arrayMixedTypes\": [ 1, 2, true, [ 1, 2, 3 ], {\"a\": 2 } ],"
      + " \"arrayComplexMixedTypes\": [ {\"a\": 1 }, {\"a\": \"a\" } ],"
      + " \"binary\": \"S2Fma2Egcm9ja3Mh\","
      + " \"boolean\": true,"
      + " \"code\": { \"$code\": \"int i = 0;\"},"
      + " \"codeWithScope\": { \"$code\": \"int x = y;\", \"$scope\": {\"y\": 1 }},"
      + " \"dateTime\": \"2020-01-01T00:00:01Z\","
      + " \"decimal128\": 1.0,"
      + " \"documentEmpty\": {},"
      + " \"document\": {\"a\": 1},"
      + " \"double\": 62.0,"
      + " \"int32\": 42,"
      + " \"int64\": 52,"
      + " \"maxKey\": {\"$maxKey\": 1},"
      + " \"minKey\": {\"$minKey\": 1},"
      + " \"null\": null,"
      + " \"objectId\": \"5f3d1bbde0ca4d2829c91e1d\","
      + " \"regex\": {\"$regularExpression\": {\"pattern\": \"^test.*regex.*xyz$\", \"options\": \"i\" }},"
      + " \"string\": \"the fox ...\","
      + " \"symbol\": \"ruby stuff\","
      + " \"timestamp\": {\"$timestamp\": {\"t\": 305419896, \"i\": 5 }},"
      + " \"undefined\": {\"$undefined\": true}"
      + "}");
  static final GenericRowWithSchema ALL_TYPES_RELAXED_JSON_ROW = new GenericRowWithSchema(
      asList(
              "[]",
              "[1, 2, 3]",
              "[{\"a\": 1}, {\"a\": 2}]",
              "[1, 2, true, [1, 2, 3], {\"a\": 2}]",
              "[{\"a\": 1}, {\"a\": \"a\"}]",
              "S2Fma2Egcm9ja3Mh",
              "true",
              "{\"$code\": \"int i = 0;\"}",
              "{\"$code\": \"int x = y;\", \"$scope\": {\"y\": 1}}",
              "2020-01-01T00:00:01Z",
              "1.0",
              "{}",
              "{\"a\": 1}",
              "62.0",
              "42",
              "52",
              "{\"$maxKey\": 1}",
              "{\"$minKey\": 1}",
              "null",
              "5f3d1bbde0ca4d2829c91e1d",
              "{\"$regularExpression\": {\"pattern\": \"^test.*regex.*xyz$\", \"options\": \"i\"}}",
              "the fox ...",
              "ruby stuff",
              "{\"$timestamp\": {\"t\": 305419896, \"i\": 5}}",
              "{\"$undefined\": true}")
          .toArray(),
      BSON_DOCUMENT_STRING_SCHEMA);

  // Extended JSON
  static final GenericRowWithSchema ALL_TYPES_EXTENDED_JSON_ROW = new GenericRowWithSchema(
      asList(
              "[]",
              "[{\"$numberInt\": \"1\"}, {\"$numberInt\": \"2\"}, {\"$numberInt\": \"3\"}]",
              "[{\"a\": {\"$numberInt\": \"1\"}}, {\"a\": {\"$numberInt\": \"2\"}}]",
              "[{\"$numberInt\": \"1\"}, {\"$numberInt\": \"2\"}, true, [{\"$numberInt\": \"1\"}, {\"$numberInt\": "
                  + "\"2\"}, {\"$numberInt\": \"3\"}], {\"a\": {\"$numberInt\": \"2\"}}]",
              "[{\"a\": {\"$numberInt\": \"1\"}}, {\"a\": \"a\"}]",
              "{\"$binary\": {\"base64\": \"S2Fma2Egcm9ja3Mh\", \"subType\": \"00\"}}",
              "true",
              "{\"$code\": \"int i = 0;\"}",
              "{\"$code\": \"int x = y;\", \"$scope\": {\"y\": {\"$numberInt\": \"1\"}}}",
              "{\"$date\": {\"$numberLong\": \"1577836801000\"}}",
              "{\"$numberDecimal\": \"1.0\"}",
              "{}",
              "{\"a\": {\"$numberInt\": \"1\"}}",
              "{\"$numberDouble\": \"62.0\"}",
              "{\"$numberInt\": \"42\"}",
              "{\"$numberLong\": \"52\"}",
              "{\"$maxKey\": 1}",
              "{\"$minKey\": 1}",
              null,
              "{\"$oid\": \"5f3d1bbde0ca4d2829c91e1d\"}",
              "{\"$regularExpression\": {\"pattern\": \"^test.*regex.*xyz$\", \"options\": \"i\"}}",
              "the fox ...",
              "{\"$symbol\": \"ruby stuff\"}",
              "{\"$timestamp\": {\"t\": 305419896, \"i\": 5}}",
              "{\"$undefined\": true}")
          .toArray(),
      BSON_DOCUMENT_STRING_SCHEMA);

  // JSON Conversion types handling
  private static final StructType CONVERT_JSON_SCHEMA = DataTypes.createStructType(asList(
      DataTypes.createStructField("emptyString", DataTypes.StringType, true),
      DataTypes.createStructField("booleanString", DataTypes.StringType, true),
      DataTypes.createStructField("numericString", DataTypes.StringType, true),
      DataTypes.createStructField("document", DataTypes.StringType, true),
      DataTypes.createStructField("array", DataTypes.StringType, true)));

  static final GenericRowWithSchema CONVERT_JSON_ROW = new GenericRowWithSchema(
      asList(
              "",
              "true",
              "012345",
              "{\"a\": \"foo\", \"b\": \"012345\", \"c\": \"true\"}",
              "[\"012345\", \"true\", {\"a\": \"foo\", \"b\": \"012345\", \"c\": \"true\"}]")
          .toArray(),
      CONVERT_JSON_SCHEMA);

  static final BsonDocument CONVERT_JSON_DOCUMENT = BsonDocument.parse("{"
      + " \"emptyString\": \"\","
      + " \"booleanString\": true,"
      + " \"numericString\": 012345,"
      + " \"document\": {\"a\": \"foo\", \"b\": \"012345\", \"c\": \"true\"},"
      + " \"array\": [\"012345\", \"true\", {\"a\": \"foo\", \"b\": \"012345\", \"c\": \"true\"}]"
      + "}");

  static final BsonDocument CONVERT_JSON_OBJECT_OR_ARRAY_ONLY_DOCUMENT = BsonDocument.parse("{"
      + " \"emptyString\": \"\","
      + " \"booleanString\": \"true\","
      + " \"numericString\": \"012345\","
      + " \"document\": {\"a\": \"foo\", \"b\": \"012345\", \"c\": \"true\"},"
      + " \"array\": [\"012345\", \"true\", {\"a\": \"foo\", \"b\": \"012345\", \"c\": \"true\"}],"
      + "}");

  // Infer Schema
  static final StructType BSON_DOCUMENT_ALL_TYPES_SCHEMA_WITH_PLACEHOLDER =
      DataTypes.createStructType(Arrays.stream(BSON_DOCUMENT_ALL_TYPES_SCHEMA.fields())
          .map(f -> {
            if (f.name().equals("arrayEmpty")) {
              return DataTypes.createStructField(
                  f.name(), PLACE_HOLDER_ARRAY_TYPE, f.nullable(), INFERRED_METADATA);
            }
            return f;
          })
          .collect(Collectors.toList()));

  private static StructField createStructField(final String name, final DataType dataType) {
    return DataTypes.createStructField(name, dataType, true, INFERRED_METADATA);
  }
}
