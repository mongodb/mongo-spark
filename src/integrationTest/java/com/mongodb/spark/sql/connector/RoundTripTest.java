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

package com.mongodb.spark.sql.connector;

import static com.mongodb.spark.sql.connector.mongodb.MongoSparkConnectorHelper.CATALOG;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.params.provider.Arguments.of;

import com.mongodb.spark.sql.connector.beans.BoxedBean;
import com.mongodb.spark.sql.connector.beans.ComplexBean;
import com.mongodb.spark.sql.connector.beans.DateTimeBean;
import com.mongodb.spark.sql.connector.beans.PrimitiveBean;
import com.mongodb.spark.sql.connector.config.WriteConfig;
import com.mongodb.spark.sql.connector.config.WriteConfig.TruncateMode;
import com.mongodb.spark.sql.connector.mongodb.MongoSparkConnectorTestCase;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class RoundTripTest extends MongoSparkConnectorTestCase {

  @Test
  void testPrimitiveBean() {
    // Given
    List<PrimitiveBean> dataSetOriginal =
        singletonList(new PrimitiveBean((byte) 1, (short) 2, 3, 4L, 5.0f, 6.0, true));

    // when
    SparkSession spark = getOrCreateSparkSession();
    Encoder<PrimitiveBean> encoder = Encoders.bean(PrimitiveBean.class);

    Dataset<PrimitiveBean> dataset = spark.createDataset(dataSetOriginal, encoder);
    dataset.write().format("mongodb").mode("Overwrite").save();

    // Then
    List<PrimitiveBean> dataSetMongo = spark
        .read()
        .format("mongodb")
        .schema(encoder.schema())
        .load()
        .as(encoder)
        .collectAsList();
    assertIterableEquals(dataSetOriginal, dataSetMongo);
  }

  @ParameterizedTest
  @EnumSource(TruncateMode.class)
  void testBoxedBean(final TruncateMode mode) {
    // Given
    List<BoxedBean> dataSetOriginal =
        singletonList(new BoxedBean((byte) 1, (short) 2, 3, 4L, 5.0f, 6.0, true));

    // when
    SparkSession spark = getOrCreateSparkSession();
    Encoder<BoxedBean> encoder = Encoders.bean(BoxedBean.class);

    Dataset<BoxedBean> dataset = spark.createDataset(dataSetOriginal, encoder);
    dataset
        .write()
        .format("mongodb")
        .mode("Overwrite")
        .option(WriteConfig.TRUNCATE_MODE_CONFIG, mode.name())
        .save();

    // Then
    List<BoxedBean> dataSetMongo = spark
        .read()
        .format("mongodb")
        .schema(encoder.schema())
        .load()
        .as(encoder)
        .collectAsList();
    assertIterableEquals(dataSetOriginal, dataSetMongo);
  }

  @ParameterizedTest()
  @ValueSource(strings = {"true", "false"})
  void testDateTimeBean(final String java8DateTimeAPI) {
    TimeZone original = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(UTC));

      // Given
      long oneHour = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
      long oneDay = oneHour * 24;

      Instant epoch = Instant.EPOCH;
      List<DateTimeBean> dataSetOriginal = singletonList(new DateTimeBean(
          new Date(oneDay * 365),
          new Timestamp(oneDay + oneHour),
          LocalDate.of(2000, 1, 1),
          epoch,
          LocalDateTime.ofInstant(epoch, UTC)));

      // when
      SparkSession spark = getOrCreateSparkSession(
          getSparkConf().set("spark.sql.datetime.java8API.enabled", java8DateTimeAPI));
      Encoder<DateTimeBean> encoder = Encoders.bean(DateTimeBean.class);

      Dataset<DateTimeBean> dataset = spark.createDataset(dataSetOriginal, encoder);
      dataset.write().format("mongodb").mode("Overwrite").save();

      // Then
      List<DateTimeBean> dataSetMongo = spark
          .read()
          .format("mongodb")
          .schema(encoder.schema())
          .load()
          .as(encoder)
          .collectAsList();
      assertIterableEquals(dataSetOriginal, dataSetMongo);
    } finally {
      TimeZone.setDefault(original);
    }
  }

  @Test
  void testComplexBean() {
    // Given
    BoxedBean boxedBean = new BoxedBean((byte) 1, (short) 2, 3, 4L, 5.0f, 6.0, true);
    List<String> stringList = asList("a", "b", "c");
    Map<String, String> stringMap = new HashMap<>();
    stringMap.put("a", "a");
    stringMap.put("b", "b");

    List<Map<String, String>> complexList = singletonList(stringMap);
    Map<String, List<String>> complexMap = new HashMap<>();
    complexMap.put("a", stringList);

    List<ComplexBean> dataSetOriginal =
        singletonList(new ComplexBean(boxedBean, stringList, stringMap, complexList, complexMap));

    // when
    SparkSession spark = getOrCreateSparkSession();
    Encoder<ComplexBean> encoder = Encoders.bean(ComplexBean.class);

    Dataset<ComplexBean> dataset = spark.createDataset(dataSetOriginal, encoder);
    dataset.write().format("mongodb").mode("Overwrite").save();

    // Then
    List<ComplexBean> dataSetMongo = spark
        .read()
        .format("mongodb")
        .schema(encoder.schema())
        .load()
        .as(encoder)
        .collectAsList();
    assertIterableEquals(dataSetOriginal, dataSetMongo);
  }

  private static Stream<Arguments> testCatalogAccessAndDelete() {
    // Spark's LikeSimplification optimizer skips patterns containing escape characters (backslash).
    // These stay as LIKE expressions which cannot be translated to V2 Predicate API (which only
    // supports
    // StringContains/StartsWith/EndsWith). DELETE requires all filters pushable, so these fail.
    // For example: LIKE '%\\%' is not supported, but LIKE '%cherry%' is.
    return Stream.of(
        of("LIKE '.*'", asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)),
        of("LIKE '%cherry%'", asList(0, 1, 3, 4, 5, 6, 7, 8, 9)),
        of("LIKE '%.*%'", asList(0, 1, 2, 3, 8, 9)),
        of("LIKE '%.%'", asList(0, 1, 2, 8, 9)),
        of("LIKE '%[ae]%'", asList(0, 1, 2, 3, 4, 5, 6, 7, 8)),
        of("LIKE 'pattern.*%'", asList(0, 1, 2, 3, 5, 6, 7, 8, 9)),
        of("LIKE '%.*pattern'", asList(0, 1, 2, 3, 5, 6, 7, 8, 9)));
  }

  @ParameterizedTest
  @MethodSource
  void testCatalogAccessAndDelete(
      final String predicateValue, final List<Integer> expectedIntFields) {
    List<BoxedBean> dataSetOriginal = asList(
        new BoxedBean((byte) 1, (short) 2, 0, 4L, 5.0f, 6.0, true, "apple"),
        new BoxedBean((byte) 1, (short) 2, 1, 4L, 5.0f, 6.0, true, "banana"),
        new BoxedBean((byte) 1, (short) 2, 2, 4L, 5.0f, 6.0, true, "cherry"),
        new BoxedBean((byte) 1, (short) 2, 3, 4L, 5.0f, 6.0, true, "dot_._dot"),
        new BoxedBean((byte) 1, (short) 2, 9, 4L, 5.0f, 6.0, false, "b_[ae]_b"),
        new BoxedBean((byte) 1, (short) 2, 4, 4L, 5.0f, 6.0, false, "pattern.*pattern"),
        new BoxedBean((byte) 1, (short) 2, 5, 4L, 5.0f, 6.0, false, "meta_\\E.*_meta"),
        new BoxedBean((byte) 1, (short) 2, 6, 4L, 5.0f, 6.0, false, "meta_.*\\Q_meta"),
        new BoxedBean((byte) 1, (short) 2, 7, 4L, 5.0f, 6.0, false, "meta_\\E.*\\Q_meta"),
        new BoxedBean((byte) 1, (short) 2, 8, 4L, 5.0f, 6.0, false, "test\\nest"));

    SparkSession spark = getOrCreateSparkSession();
    Encoder<BoxedBean> encoder = Encoders.bean(BoxedBean.class);
    spark
        .createDataset(dataSetOriginal, encoder)
        .write()
        .format("mongodb")
        .mode("Overwrite")
        .save();

    String tableName = CATALOG + "." + HELPER.getDatabaseName() + "." + HELPER.getCollectionName();
    List<Row> rows = spark.sql("select * from " + tableName).collectAsList();
    assertEquals(10, rows.size());

    spark.sql(
        "delete from " + tableName + " where stringField " + predicateValue + " and intField > 1");
    rows = spark.sql("select * from " + tableName).collectAsList();

    List<Integer> actualIntFields = rows.stream()
        .map(r -> r.getInt(r.fieldIndex("intField")))
        .sorted()
        .collect(Collectors.toList());

    assertIterableEquals(
        expectedIntFields,
        actualIntFields,
        "Expected " + expectedIntFields + " but found " + actualIntFields);
  }
}
