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
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.BsonArray;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.types.Decimal128;

import com.mongodb.spark.sql.connector.exceptions.DataException;

import scala.collection.JavaConverters;
import scala.collection.Seq;

public class RowToBsonDocumentConverterTest extends SchemaTest {

  private static final RowToBsonDocumentConverter CONVERTER = new RowToBsonDocumentConverter();

  @Test
  @DisplayName("test simple types")
  void testSimpleTypes() {
    assertEquals(SIMPLE_BSON_DOCUMENT, CONVERTER.fromRow(SIMPLE_ROW));
  }

  @Test
  @DisplayName("test decimal types")
  void testDecimalTypes() {
    BigDecimal bigDecimal = BigDecimal.valueOf(123456.789);
    Row row =
        new GenericRowWithSchema(
            new Object[] {Decimal.apply(bigDecimal)},
            new StructType().add("decimalType", DataTypes.createDecimalType(), true));
    BsonDocument expected =
        new BsonDocument("decimalType", new BsonDecimal128(new Decimal128(bigDecimal)));
    assertEquals(expected, CONVERTER.fromRow(row));

    row =
        new GenericRowWithSchema(
            new Object[] {Decimal.apply(bigDecimal)},
            new StructType()
                .add(
                    "decimalType",
                    DataTypes.createDecimalType(bigDecimal.precision(), bigDecimal.scale()),
                    true));
    assertEquals(expected, CONVERTER.fromRow(row));
  }

  @Test
  @DisplayName("test list types")
  void testListTypes() {
    Row row =
        new GenericRowWithSchema(
            new Object[] {toSeq(SIMPLE_ROW)},
            new StructType()
                .add("listType", DataTypes.createArrayType(SIMPLE_ROW.schema(), true), true));
    BsonDocument expected =
        new BsonDocument("listType", new BsonArray(singletonList(SIMPLE_BSON_DOCUMENT)));
    assertEquals(expected, CONVERTER.fromRow(row));
  }

  @Test
  @DisplayName("test map types")
  void testMapTypes() {
    Row row =
        new GenericRowWithSchema(
            new Object[] {toMap("mapType", SIMPLE_ROW)},
            new StructType()
                .add(
                    "mapType",
                    DataTypes.createMapType(DataTypes.StringType, SIMPLE_ROW.schema(), true),
                    true));
    BsonDocument expected =
        new BsonDocument("mapType", new BsonDocument("mapType", SIMPLE_BSON_DOCUMENT));
    assertEquals(expected, CONVERTER.fromRow(row));
  }

  @Test
  @DisplayName("test unsupported types")
  void testUnsupportedTypes() {
    Row schemalessRow = new GenericRow(new Object[] {"a", "b"});
    assertThrows(DataException.class, () -> CONVERTER.fromRow(schemalessRow));

    Row invalidType =
        new GenericRowWithSchema(
            new Object[] {new CalendarInterval(1, 2, 3)},
            new StructType().add("calendarIntervalType", DataTypes.TimestampType, true));
    assertThrows(DataException.class, () -> CONVERTER.fromRow(invalidType));

    Row invalidMap =
        new GenericRowWithSchema(
            new Object[] {toMap(1, 2)},
            new StructType()
                .add(
                    "mapType",
                    DataTypes.createMapType(DataTypes.IntegerType, DataTypes.IntegerType),
                    true));
    assertThrows(DataException.class, () -> CONVERTER.fromRow(invalidMap));

    DataType unknownDataType =
        new DataType() {

          @Override
          public int defaultSize() {
            return 0;
          }

          @Override
          public DataType asNullable() {
            return null;
          }
        };

    Row unknownDataTypeRow =
        new GenericRowWithSchema(
            new Object[] {1}, new StructType().add("unknownDataType", unknownDataType, true));
    assertThrows(DataException.class, () -> CONVERTER.fromRow(unknownDataTypeRow));
  }

  @SafeVarargs
  private final <T> Seq<T> toSeq(final T... values) {
    return JavaConverters.collectionAsScalaIterableConverter(asList(values)).asScala().toSeq();
  }

  private <K, V> scala.collection.Map<K, V> toMap(final K key, final V value) {
    Map<K, V> map = new HashMap<>();
    map.put(key, value);
    return JavaConverters.mapAsScalaMap(map);
  }
}
