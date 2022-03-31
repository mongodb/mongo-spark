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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.stream.Stream;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;

import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;

public class InferSchemaTest {

  private static final String BSON_DOCUMENT_JSON =
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
          + "\"documentEmpty\": {}, "
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

  private static final BsonDocument BSON_DOCUMENT = RawBsonDocument.parse(BSON_DOCUMENT_JSON);
  private static final StructType BSON_DOCUMENT_SCHEMA =
      new StructType()
          .add("arrayEmpty", DataTypes.createArrayType(DataTypes.StringType, true))
          .add("arraySimple", DataTypes.createArrayType(DataTypes.IntegerType, true))
          .add(
              "arrayComplex",
              DataTypes.createArrayType(
                  DataTypes.createStructType(
                      singletonList(
                          DataTypes.createStructField("a", DataTypes.IntegerType, true)))))
          .add("arrayMixedTypes", DataTypes.createArrayType(DataTypes.StringType, true))
          .add(
              "arrayComplexMixedTypes",
              DataTypes.createArrayType(
                  DataTypes.createStructType(
                      singletonList(DataTypes.createStructField("a", DataTypes.StringType, true)))))
          .add("binary", DataTypes.BinaryType)
          .add("boolean", DataTypes.BooleanType)
          .add("code", DataTypes.StringType)
          .add("codeWithScope", DataTypes.StringType)
          .add("dateTime", DataTypes.TimestampType)
          .add("decimal128", DataTypes.createDecimalType(2, 1))
          .add("documentEmpty", DataTypes.createStructType(emptyList()))
          .add(
              "document",
              DataTypes.createStructType(
                  singletonList(DataTypes.createStructField("a", DataTypes.IntegerType, true))))
          .add("double", DataTypes.DoubleType)
          .add("int32", DataTypes.IntegerType)
          .add("int64", DataTypes.LongType)
          .add("maxKey", DataTypes.StringType)
          .add("minKey", DataTypes.StringType)
          .add("null", DataTypes.NullType)
          .add("objectId", DataTypes.StringType)
          .add("regex", DataTypes.StringType)
          .add("string", DataTypes.StringType)
          .add("symbol", DataTypes.StringType)
          .add("timestamp", DataTypes.TimestampType)
          .add("undefined", DataTypes.StringType);

  private static final ReadConfig READ_CONFIG = MongoConfig.readConfig(emptyMap());

  @ParameterizedTest
  @MethodSource("documentFieldNames")
  void testIndividualFieldSchema(final String fieldName) {
    assertEquals(
        getDataType(fieldName),
        InferSchema.getDataType(BSON_DOCUMENT.get(fieldName), READ_CONFIG),
        fieldName + " failed");
  }

  @ParameterizedTest
  @MethodSource("documentFieldNames")
  void testStructSchema(final String fieldName) {
    assertEquals(
        DataTypes.createStructType(
            singletonList(DataTypes.createStructField("field", getDataType(fieldName), true))),
        InferSchema.getDataType(
            new BsonDocument("field", BSON_DOCUMENT.get(fieldName)), READ_CONFIG),
        fieldName + " failed");
  }

  @ParameterizedTest
  @MethodSource("documentFieldNames")
  void testArraySchema(final String fieldName) {
    assertEquals(
        DataTypes.createArrayType(getDataType(fieldName), true),
        InferSchema.getDataType(
            new BsonArray(singletonList(BSON_DOCUMENT.get(fieldName))), READ_CONFIG),
        fieldName + " failed");
  }

  @Test
  void testSingleFullDocument() {
    assertEquals(BSON_DOCUMENT_SCHEMA, InferSchema.getDataType(BSON_DOCUMENT, READ_CONFIG));
  }

  @Test
  @DisplayName("It should upscale number types based on numeric precedence")
  void testUpscalingNumberTypesBasedOnNumericPrecedence() {
    assertAll(
        "numeric precedence",
        () ->
            assertEquals(
                DataTypes.createStructType(
                    singletonList(DataTypes.createStructField("a", DataTypes.LongType, true))),
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse("{a: -1}"),
                        BsonDocument.parse("{a: 1}"),
                        BsonDocument.parse("{a: {'$numberLong': '123'}}")),
                    READ_CONFIG)),
        () ->
            assertEquals(
                DataTypes.createStructType(
                    singletonList(DataTypes.createStructField("a", DataTypes.DoubleType, true))),
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse("{a: -1}"),
                        BsonDocument.parse("{a: 1.1}"),
                        BsonDocument.parse("{a: {'$numberLong': '123'}}")),
                    READ_CONFIG)),
        () ->
            assertEquals(
                DataTypes.createStructType(
                    singletonList(
                        DataTypes.createStructField(
                            "a", DataTypes.createDecimalType(10, 0), true))),
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse("{a: 1}}"),
                        BsonDocument.parse("{a: 2}"),
                        BsonDocument.parse("{a: {'$numberDecimal' : '1'}}")),
                    READ_CONFIG)),
        () ->
            assertEquals(
                DataTypes.createStructType(
                    singletonList(
                        DataTypes.createStructField(
                            "a", DataTypes.createDecimalType(10, 5), true))),
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse("{a: {'$numberDecimal' : '1'}}"),
                        BsonDocument.parse("{a: {'$numberDecimal' : '10.01'}}"),
                        BsonDocument.parse("{a: {'$numberDecimal' : '10000.00001'}}")),
                    READ_CONFIG)),
        () ->
            assertEquals(
                DataTypes.createStructType(
                    singletonList(
                        DataTypes.createStructField(
                            "a", DataTypes.createDecimalType(30, 15), true))),
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse("{a: 1.0}"),
                        BsonDocument.parse("{a: {'$numberDecimal' : '10.01'}}")),
                    READ_CONFIG)),
        () ->
            assertEquals(
                DataTypes.createStructType(
                    singletonList(DataTypes.createStructField("a", DataTypes.DoubleType, true))),
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse(
                            "{a: {'$numberDecimal' : '0.1234567890123456789012345678901234'}}"),
                        BsonDocument.parse(
                            "{a: {'$numberDecimal' : '1234567890123456789012345678901231'}}")),
                    READ_CONFIG)),
        () ->
            assertEquals(
                DataTypes.createStructType(
                    singletonList(
                        DataTypes.createStructField(
                            "a", DataTypes.createDecimalType(20, 0), true))),
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse("{a: 1}}"),
                        BsonDocument.parse("{a:  {'$numberLong': '123'}}"),
                        BsonDocument.parse("{a: {'$numberDecimal' : '1'}}")),
                    READ_CONFIG)),
        () ->
            assertEquals(
                DataTypes.createStructType(
                    singletonList(
                        DataTypes.createStructField(
                            "a", DataTypes.createDecimalType(30, 15), true))),
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse("{a: {'$numberLong': '123'}}"),
                        BsonDocument.parse("{a: -1.1}"),
                        BsonDocument.parse("{a: {'$numberDecimal' : '1'}}")),
                    READ_CONFIG)),
        () ->
            assertEquals(
                DataTypes.createStructType(
                    singletonList(
                        DataTypes.createStructField(
                            "a", DataTypes.createDecimalType(30, 15), true))),
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse("{a: {'$numberLong': '123'}}"),
                        BsonDocument.parse("{a: -1.1}"),
                        BsonDocument.parse("{a: {'$numberDecimal' : '-1.00E-8'}}")),
                    READ_CONFIG)));

    assertAll(
        "nested numeric precedence",
        () ->
            assertEquals(
                DataTypes.createStructType(
                    singletonList(
                        DataTypes.createStructField(
                            "a", DataTypes.createArrayType(DataTypes.LongType, true), true))),
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse("{a: [-1]}"),
                        BsonDocument.parse("{a: [1]}"),
                        BsonDocument.parse("{a: [{'$numberLong': '123'}]}")),
                    READ_CONFIG)),
        () ->
            assertEquals(
                DataTypes.createStructType(
                    singletonList(
                        DataTypes.createStructField(
                            "a",
                            DataTypes.createArrayType(DataTypes.createDecimalType(30, 15), true),
                            true))),
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse("{a: [{'$numberLong': '123'}]}"),
                        BsonDocument.parse("{a: [-1.1]}"),
                        BsonDocument.parse("{a: [{'$numberDecimal' : '1'}]}")),
                    READ_CONFIG)));
  }

  @Test
  @DisplayName("It should be able to infer the schema from arrays")
  void testInferArraysComplex() {
    StructType elementType =
        DataTypes.createStructType(
            asList(
                DataTypes.createStructField("a", DataTypes.IntegerType, true),
                DataTypes.createStructField("b", DataTypes.IntegerType, true),
                DataTypes.createStructField("c", DataTypes.IntegerType, true),
                DataTypes.createStructField("d", DataTypes.IntegerType, true),
                DataTypes.createStructField("e", DataTypes.IntegerType, true)));

    StructType expectedStructType =
        DataTypes.createStructType(
            singletonList(
                DataTypes.createStructField(
                    "arrayField", DataTypes.createArrayType(elementType, true), true)));

    assertAll(
        "arrays containing structs",
        () ->
            assertEquals(
                expectedStructType,
                InferSchema.inferSchema(
                    singletonList(
                        BsonDocument.parse("{arrayField: [{a: 1, b: 2, c: 3, d: 4, e: 5}]}")),
                    READ_CONFIG)),
        () ->
            assertEquals(
                expectedStructType,
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse("{arrayField: [{a: 1, e: 2}]}"),
                        BsonDocument.parse("{arrayField: [{d: 3, c: 4}]}"),
                        BsonDocument.parse("{arrayField: [{b: 5}]}")),
                    READ_CONFIG)),
        () ->
            assertEquals(
                expectedStructType,
                InferSchema.inferSchema(
                    singletonList(
                        BsonDocument.parse(
                            "{arrayField: [{a: 1, b: 2}, {}, {c: 3}, {d: 4, e: 5}]}")),
                    READ_CONFIG)));

    StructType expectedNestedStructType =
        DataTypes.createStructType(
            singletonList(
                DataTypes.createStructField(
                    "arrayField",
                    DataTypes.createArrayType(DataTypes.createArrayType(elementType, true), true),
                    true)));
    assertAll(
        "nested arrays containing structs",
        () ->
            assertEquals(
                expectedNestedStructType,
                InferSchema.inferSchema(
                    singletonList(
                        BsonDocument.parse("{arrayField: [[{a: 1, b: 2, c: 3, d: 4, e: 5}]]}")),
                    READ_CONFIG)),
        () ->
            assertEquals(
                expectedNestedStructType,
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse("{arrayField: [[{a: 1, e: 2}]]}"),
                        BsonDocument.parse("{arrayField: [[{d: 3, c: 4}]]}"),
                        BsonDocument.parse("{arrayField: [[{b: 5}]]}")),
                    READ_CONFIG)),
        () ->
            assertEquals(
                expectedNestedStructType,
                InferSchema.inferSchema(
                    singletonList(
                        BsonDocument.parse(
                            "{arrayField: [[{a: 1, b: 2}, {}, {c: 3}, {d: 4, e: 5}]]}")),
                    READ_CONFIG)));

    StructType stringArrayStructType =
        DataTypes.createStructType(
            singletonList(
                DataTypes.createStructField(
                    "arrayField", DataTypes.createArrayType(DataTypes.StringType, true), true)));

    assertAll(
        "arrays containing mixed incompatible types",
        () ->
            assertEquals(
                stringArrayStructType,
                InferSchema.inferSchema(
                    singletonList(
                        BsonDocument.parse("{arrayField: [{a: 1, b: 2, c: 3, d: 4, e: 5}, 1]}")),
                    READ_CONFIG)),
        () ->
            assertEquals(
                stringArrayStructType,
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse("{arrayField: [{a: 1, e: 2}]}"),
                        BsonDocument.parse("{arrayField: [{d: 3, c: 4}]}"),
                        BsonDocument.parse("{arrayField: [{b: 5}, 1]}")),
                    READ_CONFIG)),
        () ->
            assertEquals(
                stringArrayStructType,
                InferSchema.inferSchema(
                    singletonList(
                        BsonDocument.parse(
                            "{arrayField: [{a: 1, b: 2}, 1, {c: 3}, {d: 4, e: 5}]}")),
                    READ_CONFIG)));
  }

  @Test
  @DisplayName("It should be able to infer map types")
  void testMapTypes() {
    StructType simpleMapFieldStruct =
        DataTypes.createStructType(
            singletonList(
                DataTypes.createStructField(
                    "mapField",
                    DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType),
                    true)));

    StructType abcdStruct =
        DataTypes.createStructType(
            asList(
                DataTypes.createStructField("a", DataTypes.IntegerType, true),
                DataTypes.createStructField("b", DataTypes.IntegerType, true),
                DataTypes.createStructField("c", DataTypes.IntegerType, true),
                DataTypes.createStructField("d", DataTypes.IntegerType, true)));

    ReadConfig readConfig =
        READ_CONFIG.withOptions(
            singletonMap(ReadConfig.INFER_SCHEMA_MAP_TYPE_MINIMUM_KEY_SIZE_CONFIG, "5"));

    assertAll(
        "simple map fields",
        () ->
            assertEquals(
                DataTypes.createStructType(
                    singletonList(DataTypes.createStructField("mapField", abcdStruct, true))),
                InferSchema.inferSchema(
                    singletonList(BsonDocument.parse("{mapField: {a: 1, b: 2, c: 3, d: 4}}")),
                    readConfig)),
        () ->
            assertEquals(
                simpleMapFieldStruct,
                InferSchema.inferSchema(
                    singletonList(BsonDocument.parse("{mapField: {a: 1, b: 2, c: 3, d: 4, e: 5}}")),
                    readConfig)),
        () ->
            assertEquals(
                simpleMapFieldStruct,
                InferSchema.inferSchema(
                    singletonList(
                        BsonDocument.parse("{mapField: {a: 1, b: 2, c: 3, d: 4, e: 5, f: 6}}")),
                    readConfig)),
        () ->
            assertEquals(
                simpleMapFieldStruct,
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse("{mapField: {a: 1}}"),
                        BsonDocument.parse("{mapField: {b: 2, c: 3}}"),
                        BsonDocument.parse("{mapField: {d: 4, e: 5}}"),
                        BsonDocument.parse("{mapField: {f: 6}}")),
                    readConfig)));

    StructType nestedMapFieldStruct =
        DataTypes.createStructType(
            singletonList(
                DataTypes.createStructField(
                    "mapField",
                    DataTypes.createStructType(
                        singletonList(
                            DataTypes.createStructField(
                                "nested",
                                DataTypes.createMapType(
                                    DataTypes.StringType, DataTypes.IntegerType),
                                true))),
                    true)));

    assertAll(
        "nested struct map fields",
        () ->
            assertEquals(
                DataTypes.createStructType(
                    singletonList(
                        DataTypes.createStructField(
                            "mapField",
                            DataTypes.createStructType(
                                singletonList(
                                    DataTypes.createStructField("nested", abcdStruct, true))),
                            true))),
                InferSchema.inferSchema(
                    singletonList(
                        BsonDocument.parse("{mapField: {nested: {a: 1, b: 2, c: 3, d: 4}}}")),
                    readConfig)),
        () ->
            assertEquals(
                nestedMapFieldStruct,
                InferSchema.inferSchema(
                    singletonList(
                        BsonDocument.parse("{mapField: {nested: {a: 1, b: 2, c: 3, d: 4, e: 5}}}")),
                    readConfig)),
        () ->
            assertEquals(
                nestedMapFieldStruct,
                InferSchema.inferSchema(
                    singletonList(
                        BsonDocument.parse(
                            "{mapField: {nested: {a: 1, b: 2, c: 3, d: 4, e: 5, f: 6}}}")),
                    readConfig)),
        () ->
            assertEquals(
                nestedMapFieldStruct,
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse("{mapField: {nested: {a: 1}}}"),
                        BsonDocument.parse("{mapField: {nested: {b: 2, c: 3}}}"),
                        BsonDocument.parse("{mapField: {nested: {d: 4, e: 5}}}"),
                        BsonDocument.parse("{mapField: {nested: {f: 6}}}")),
                    readConfig)));

    StructType arrayMapFieldStruct =
        DataTypes.createStructType(
            singletonList(
                DataTypes.createStructField(
                    "arrayMapField",
                    DataTypes.createArrayType(
                        DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType), true),
                    true)));
    assertAll(
        "nested array map fields",
        () ->
            assertEquals(
                DataTypes.createStructType(
                    singletonList(
                        DataTypes.createStructField(
                            "arrayMapField", DataTypes.createArrayType(abcdStruct, true), true))),
                InferSchema.inferSchema(
                    singletonList(
                        BsonDocument.parse("{arrayMapField: [{a: 1, b: 2, c: 3, d: 4}]}")),
                    readConfig)),
        () ->
            assertEquals(
                arrayMapFieldStruct,
                InferSchema.inferSchema(
                    singletonList(
                        BsonDocument.parse("{arrayMapField: [{a: 1, b: 2, c: 3, d: 4, e: 5}]}")),
                    readConfig)),
        () ->
            assertEquals(
                arrayMapFieldStruct,
                InferSchema.inferSchema(
                    singletonList(
                        BsonDocument.parse(
                            "{arrayMapField: [{a: 1, b: 2, c: 3, d: 4, e: 5, f: 6}]}")),
                    readConfig)),
        () ->
            assertEquals(
                arrayMapFieldStruct,
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse("{arrayMapField: [{a: 1}]}"),
                        BsonDocument.parse("{arrayMapField: [{b: 2, c: 3}]}"),
                        BsonDocument.parse("{arrayMapField: [{d: 4, e: 5}]}"),
                        BsonDocument.parse("{arrayMapField: [{f: 6}]}")),
                    readConfig)),
        () ->
            assertEquals(
                arrayMapFieldStruct,
                InferSchema.inferSchema(
                    singletonList(
                        BsonDocument.parse(
                            "{arrayMapField: [{a: 1, b: 2}, {c: 3, d: 4}, {e: 5, f: 6}]}")),
                    readConfig)));

    ReadConfig disabledInferSchemaReadConfig =
        readConfig.withOptions(
            new HashMap<String, String>() {
              {
                put(ReadConfig.INFER_SCHEMA_MAP_TYPE_ENABLED_CONFIG, "false");
                put(ReadConfig.INFER_SCHEMA_MAP_TYPE_MINIMUM_KEY_SIZE_CONFIG, "1");
              }
            });

    assertEquals(
        DataTypes.createStructType(
            singletonList(DataTypes.createStructField("mapField", abcdStruct, true))),
        InferSchema.inferSchema(
            singletonList(BsonDocument.parse("{mapField: {a: 1, b: 2, c: 3, d: 4}}")),
            disabledInferSchemaReadConfig));
  }

  static Stream<String> documentFieldNames() {
    return BSON_DOCUMENT.keySet().stream();
  }

  private DataType getDataType(final String fieldName) {
    return BSON_DOCUMENT_SCHEMA.fields()[BSON_DOCUMENT_SCHEMA.fieldIndex(fieldName)].dataType();
  }
}
