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

import static com.mongodb.spark.sql.connector.config.MongoConfig.COMMENT_CONFIG;
import static com.mongodb.spark.sql.connector.interop.JavaScala.asJava;
import static java.util.Arrays.asList;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.TimeSeriesGranularity;
import com.mongodb.client.model.TimeSeriesOptions;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.WriteConfig;
import com.mongodb.spark.sql.connector.mongodb.MongoSparkConnectorTestCase;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.junit.jupiter.api.Test;

class MongoSparkConnectorWriteTest extends MongoSparkConnectorTestCase {

  private static final String TIMESERIES_RESOURCES_JSON_PATH =
      "src/integrationTest/resources/data/timeseries/*.json";

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
  void testIgnoreNullValues() {
    List<String> dataWithNulls = asList(
        "{'_id': 1, 'string': 'mystring', 'array': [1, 2], 'subDoc': {'k':  'v'}}",
        "{'_id': 2, 'string': null, 'array': null, 'subDoc':  null}",
        "{'_id': 3, 'string': 'mystring', 'array': [null], 'subDoc': {'k':  'v'}}",
        "{'_id': 4, 'string': 'mystring', 'array': [1, 2], 'subDoc': {'k':  null}}");

    List<String> dataWithoutNulls = asList(
        "{'_id': 1, 'string': 'mystring', 'array': [1, 2], 'subDoc': {'k':  'v'}}",
        "{'_id': 2}",
        "{'_id': 3, 'string': 'mystring', 'array': [], 'subDoc': {'k':  'v'}}",
        "{'_id': 4, 'string': 'mystring', 'array': [1, 2], 'subDoc': {}}");

    SparkSession spark = getOrCreateSparkSession();
    Dataset<Row> df = spark.read().json(spark.createDataset(dataWithNulls, Encoders.STRING()));

    df.write().format("mongodb").mode("Overwrite").save();
    assertCollection(dataWithNulls.stream().map(BsonDocument::parse).collect(Collectors.toList()));

    df.write()
        .format("mongodb")
        .option("ignoreNullValues", "true")
        .mode("Overwrite")
        .save();
    assertCollection(
        dataWithoutNulls.stream().map(BsonDocument::parse).collect(Collectors.toList()));
  }

  @Test
  void testTimeseriesSupport() {
    assumeTrue(isAtLeastFiveDotZero());
    SparkSession spark = getOrCreateSparkSession();

    getDatabase()
        .createCollection(
            getCollectionName(),
            new CreateCollectionOptions()
                .timeSeriesOptions(new TimeSeriesOptions("timestamp")
                    .metaField("metadata")
                    .granularity(TimeSeriesGranularity.HOURS)));

    StructType schema = createStructType(asList(
        createStructField(
            "metadata",
            DataTypes.createStructType(asList(
                createStructField("sensorId", DataTypes.IntegerType, false),
                createStructField("type", DataTypes.StringType, false))),
            false),
        createStructField("timestamp", DataTypes.DateType, false),
        createStructField("temp", DataTypes.IntegerType, false)));

    Dataset<Row> df = spark.read().schema(schema).json(TIMESERIES_RESOURCES_JSON_PATH);
    df.write()
        .mode("Append")
        .format("mongodb")
        .option(WriteConfig.UPSERT_DOCUMENT_CONFIG, "false")
        .save();

    assertEquals(
        1,
        getDatabase()
            .listCollections()
            .filter(BsonDocument.parse(
                "{ \"name\": \"" + getCollectionName() + "\",  \"type\": \"timeseries\"}"))
            .into(new ArrayList<>())
            .size());
    assertEquals(12, getCollection().countDocuments());
  }

  @Test
  void testSupportedStreamingWriteAppend() throws TimeoutException {
    SparkSession spark = getOrCreateSparkSession();

    StructType schema = createStructType(asList(
        createStructField("age", DataTypes.LongType, true),
        createStructField("name", DataTypes.StringType, true)));

    Dataset<Row> df = spark.readStream().schema(schema).json(WRITE_RESOURCES_JSON_PATH);

    StreamingQuery query =
        df.writeStream().outputMode("Append").format("mongodb").start();
    query.processAllAvailable();
    query.stop();

    assertEquals(10, getCollection().countDocuments());

    assertCollection();
  }

  /** By using a window function, this test implicitly tests committing empty (no-op) commits. */
  @Test
  void testSupportedStreamingWriteWithWindow() throws TimeoutException {
    SparkSession spark = getOrCreateSparkSession();

    StructType schema = createStructType(asList(
        createStructField("Type", DataTypes.StringType, true),
        createStructField("Date", DataTypes.TimestampType, true),
        createStructField("Price", DataTypes.DoubleType, true)));

    Dataset<Row> ds = spark
        .readStream()
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load(WRITE_RESOURCES_CSV_PATH);

    Dataset<Row> slidingWindows = ds.withWatermark("Date", "1 minute")
        .groupBy(ds.col("Type"), functions.window(ds.col("Date"), "7 day"))
        .avg()
        .orderBy(ds.col("Type"));

    StreamingQuery query = slidingWindows
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

    DataFrameWriter<Row> dfw = df.write()
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

  @Test
  void testLogCommentsInProfilerLogs() {
    SparkSession spark = getOrCreateSparkSession();

    Dataset<Row> df = spark.read().json(WRITE_RESOURCES_JSON_PATH);
    WriteConfig writeConfig = MongoConfig.writeConfig(asJava(spark.initialSessionOptions()))
        .withOption(COMMENT_CONFIG, TEST_COMMENT);

    assertCommentsInProfile(
        () -> {
          df.write()
              .option(COMMENT_CONFIG, TEST_COMMENT)
              .format("mongodb")
              .mode("Overwrite")
              .save();
          assertEquals(
              10,
              getCollection()
                  .countDocuments(new BsonDocument(), new CountOptions().comment(IGNORE_COMMENT)));
          assertCollection();
        },
        writeConfig);
  }

  @Test
  void testLogCommentsInProfilerLogsStreamingWrites() {
    SparkSession spark = getOrCreateSparkSession();

    WriteConfig writeConfig = MongoConfig.writeConfig(asJava(spark.initialSessionOptions()))
        .withOption(COMMENT_CONFIG, TEST_COMMENT);

    assertCommentsInProfile(
        () -> {
          try {
            StructType schema = createStructType(asList(
                createStructField("age", DataTypes.LongType, true),
                createStructField("name", DataTypes.StringType, true)));

            Dataset<Row> df = spark.readStream().schema(schema).json(WRITE_RESOURCES_JSON_PATH);

            StreamingQuery query = df.writeStream()
                .outputMode("Append")
                .format("mongodb")
                .option(COMMENT_CONFIG, TEST_COMMENT)
                .start();
            query.processAllAvailable();
            query.stop();
          } catch (TimeoutException e) {
            fail("TimeoutException", e);
          }

          assertEquals(
              10,
              getCollection()
                  .countDocuments(new BsonDocument(), new CountOptions().comment(IGNORE_COMMENT)));
          assertCollection();
        },
        writeConfig);
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
            .peek(d -> d.put("age", d.getOrDefault("age", BsonNull.VALUE)))
            .collect(Collectors.toList());

    List<BsonDocument> actual = getCollectionData(collectionName).stream()
        .peek(d -> d.remove("_id"))
        .collect(Collectors.toList());
    assertIterableEquals(expected, actual);
  }

  private void assertCollection(final List<BsonDocument> expected) {
    assertCollection(getCollectionName(), expected);
  }

  private void assertCollection(final String collectionName, final List<BsonDocument> expected) {
    assertIterableEquals(expected, getCollectionData(collectionName));
  }

  private List<BsonDocument> getCollectionData(final String collectionName) {
    return getCollection(collectionName)
        .find()
        .comment(IGNORE_COMMENT)
        .map(d ->
            BsonDocument.parse(d.toJson())) // Parse as simple json for simplified numeric values
        .into(new ArrayList<>());
  }
}
