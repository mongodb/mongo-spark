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
package com.mongodb.spark.sql.connector.write;

import static java.util.Arrays.asList;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import org.bson.BsonDocument;

import com.mongodb.client.model.Projections;

import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.mongodb.MongoSparkConnectorTestCase;

class MongoSparkConnectorWriteTest extends MongoSparkConnectorTestCase {
  private static final String WRITE_RESOURCES_JSON_PATH =
      "src/integrationTest/resources/data/write/*.json";

  private static final String WRITE_RESOURCES_CSV_PATH =
      "src/integrationTest/resources/data/write/*.csv";

  @Test
  void testSupportedWriteModes() {
    SparkSession spark = getOrCreateSparkSession();

    Dataset<Row> df = spark.read().json(WRITE_RESOURCES_JSON_PATH);
    DataFrameWriter<Row> dfw = df.write().format("mongodb");

    dfw.mode("Overwrite").save();
    assertEquals(10, getCollection().countDocuments());

    dfw.mode("Append").save();
    assertEquals(20, getCollection().countDocuments());

    dfw.mode("Overwrite").save();
    assertEquals(10, getCollection().countDocuments());

    assertCollection();
  }

  @Test
  void testSupportedStreamingWriteAppend() throws TimeoutException {
    SparkSession spark = getOrCreateSparkSession();

    StructType schema =
        createStructType(
            asList(
                createStructField("age", DataTypes.LongType, true),
                createStructField("name", DataTypes.StringType, true)));

    Dataset<Row> df = spark.readStream().schema(schema).json(WRITE_RESOURCES_JSON_PATH);

    StreamingQuery query = df.writeStream().outputMode("Append").format("mongodb").start();
    query.processAllAvailable();
    query.stop();

    assertEquals(10, getCollection().countDocuments());

    assertCollection();
  }

  /** By using a window function, this test implicitly tests committing empty (no-op) commits. */
  @Test
  void testSupportedStreamingWriteWithWindow() throws TimeoutException {
    SparkSession spark = getOrCreateSparkSession();

    StructType schema =
        createStructType(
            asList(
                createStructField("Type", DataTypes.StringType, true),
                createStructField("Date", DataTypes.TimestampType, true),
                createStructField("Price", DataTypes.DoubleType, true)));

    Dataset<Row> ds =
        spark
            .readStream()
            .format("csv")
            .option("header", "true")
            .schema(schema)
            .load(WRITE_RESOURCES_CSV_PATH);

    Dataset<Row> slidingWindows =
        ds.withWatermark("Date", "1 minute")
            .groupBy(ds.col("Type"), functions.window(ds.col("Date"), "7 day"))
            .avg()
            .orderBy(ds.col("Type"));

    StreamingQuery query =
        slidingWindows
            .writeStream()
            .outputMode("complete")
            .format("mongodb")
            .queryName("7DaySlidingWindow")
            .start();
    query.processAllAvailable();
    query.stop();

    assertEquals(52, getCollection().countDocuments());
  }

  @Test
  void testSupportedWriteModesWithOptions() {
    SparkSession spark = getOrCreateSparkSession();
    Dataset<Row> df = spark.read().json(WRITE_RESOURCES_JSON_PATH);

    DataFrameWriter<Row> dfw =
        df.write()
            .format("mongodb")
            .mode("Overwrite")
            .option(MongoConfig.COLLECTION_NAME_CONFIG, "coll2");

    dfw.save();
    assertEquals(0, getCollection().countDocuments());
    assertEquals(10, getCollection("coll2").countDocuments());

    dfw.option(MongoConfig.COLLECTION_NAME_CONFIG, "coll3").save();
    assertEquals(10, getCollection("coll3").countDocuments());
    assertCollection("coll3");
  }

  @Test
  void testUnsupportedDatasourceV2WriteModes() {
    SparkSession spark = getOrCreateSparkSession();
    DataFrameWriter<Row> dfw =
        spark.read().json(WRITE_RESOURCES_JSON_PATH).write().format("mongodb");

    assertThrows(AnalysisException.class, dfw::save); // Error if exists is the default
    assertThrows(AnalysisException.class, () -> dfw.mode("ErrorIfExists").save());
    assertThrows(AnalysisException.class, () -> dfw.mode("Ignore").save());
  }

  private void assertCollection() {
    assertCollection(getCollectionName());
  }

  private void assertCollection(final String collectionName) {
    List<BsonDocument> expected =
        getOrCreateSparkSession()
            .read()
            .json(WRITE_RESOURCES_JSON_PATH)
            .toJSON()
            .collectAsList()
            .stream()
            .map(BsonDocument::parse)
            .collect(Collectors.toList());

    ArrayList<BsonDocument> actual =
        getCollection(collectionName)
            .find()
            .projection(Projections.excludeId())
            .map(
                d ->
                    BsonDocument.parse(
                        d.toJson())) // Parse as simple json for simplified numeric values
            .into(new ArrayList<>());
    assertIterableEquals(expected, actual);
  }
}
