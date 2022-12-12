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

import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;

public class InferSchemaTest extends SchemaTest {

  private static final ReadConfig READ_CONFIG = MongoConfig.readConfig(emptyMap());

  @ParameterizedTest
  @MethodSource("documentFieldNames")
  void testIndividualFieldSchema(final String fieldName) {
    assertEquals(
        getDataType(fieldName),
        InferSchema.getDataType(BSON_DOCUMENT_ALL_TYPES.get(fieldName), READ_CONFIG),
        fieldName + " failed");
  }

  @ParameterizedTest
  @MethodSource("documentFieldNames")
  void testStructSchema(final String fieldName) {
    assertEquals(
        DataTypes.createStructType(
            singletonList(DataTypes.createStructField("field", getDataType(fieldName), true))),
        InferSchema.getDataType(
            new BsonDocument("field", BSON_DOCUMENT_ALL_TYPES.get(fieldName)), READ_CONFIG),
        fieldName + " failed");
  }

  @ParameterizedTest
  @MethodSource("documentFieldNames")
  void testArraySchema(final String fieldName) {
    assertEquals(
        DataTypes.createArrayType(getDataType(fieldName), true),
        InferSchema.getDataType(
            new BsonArray(singletonList(BSON_DOCUMENT_ALL_TYPES.get(fieldName))), READ_CONFIG),
        fieldName + " failed");
  }

  @Test
  void testSingleFullDocument() {
    assertEquals(
        BSON_DOCUMENT_ALL_TYPES_SCHEMA_WITH_PLACEHOLDER,
        InferSchema.getDataType(BSON_DOCUMENT_ALL_TYPES, READ_CONFIG));
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
  void testInferArrays() {

    assertAll(
        "arrays simple",
        () ->
            assertEquals(
                DataTypes.createStructType(
                    singletonList(
                        DataTypes.createStructField(
                            "arrayField",
                            DataTypes.createArrayType(DataTypes.StringType, true),
                            true))),
                InferSchema.inferSchema(
                    singletonList(BsonDocument.parse("{arrayField: []}")), READ_CONFIG)),
        () ->
            assertEquals(
                DataTypes.createStructType(
                    singletonList(
                        DataTypes.createStructField(
                            "arrayField",
                            DataTypes.createArrayType(DataTypes.BooleanType, true),
                            true))),
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse("{arrayField: []}"),
                        BsonDocument.parse("{arrayField: [true]}")),
                    READ_CONFIG)),
        () ->
            assertEquals(
                DataTypes.createStructType(
                    singletonList(
                        DataTypes.createStructField(
                            "arrayField",
                            DataTypes.createArrayType(DataTypes.BooleanType, true),
                            true))),
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse("{arrayField: [true]}"),
                        BsonDocument.parse("{arrayField: []}"),
                        BsonDocument.parse("{arrayField: []}")),
                    READ_CONFIG)),
        () ->
            assertEquals(
                DataTypes.createStructType(
                    singletonList(
                        DataTypes.createStructField(
                            "arrayField",
                            DataTypes.createArrayType(DataTypes.BooleanType, true),
                            true))),
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse("{arrayField: []}"),
                        BsonDocument.parse("{arrayField: []}"),
                        BsonDocument.parse("{arrayField: [false]}"),
                        BsonDocument.parse("{arrayField: []}")),
                    READ_CONFIG)));

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
                    READ_CONFIG)),
        () ->
            assertEquals(
                expectedStructType,
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse("{arrayField: [{a: 1, e: 2}]}"),
                        BsonDocument.parse("{arrayField: [{d: 3, c: 4}]}"),
                        BsonDocument.parse("{arrayField: []}"),
                        BsonDocument.parse("{arrayField: [{b: 5}]}")),
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
                        BsonDocument.parse("{arrayField: []}"),
                        BsonDocument.parse("{arrayField: [[{b: 5}]]}")),
                    READ_CONFIG)),
        () ->
            assertEquals(
                expectedNestedStructType,
                InferSchema.inferSchema(
                    asList(
                        BsonDocument.parse("{arrayField: [[{a: 1, e: 2}]]}"),
                        BsonDocument.parse("{arrayField: [[{d: 3, c: 4}]]}"),
                        BsonDocument.parse("{arrayField: [[]]}"),
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
    return BSON_DOCUMENT_ALL_TYPES.keySet().stream();
  }

  private DataType getDataType(final String fieldName) {
    return BSON_DOCUMENT_ALL_TYPES_SCHEMA_WITH_PLACEHOLDER
        .fields()[BSON_DOCUMENT_ALL_TYPES_SCHEMA_WITH_PLACEHOLDER.fieldIndex(fieldName)].dataType();
  }
}
