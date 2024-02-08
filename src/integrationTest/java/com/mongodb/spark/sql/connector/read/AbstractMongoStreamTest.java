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
package com.mongodb.spark.sql.connector.read;

import static com.mongodb.spark.sql.connector.config.MongoConfig.COMMENT_CONFIG;
import static com.mongodb.spark.sql.connector.config.MongoConfig.PREFIX;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.lang.Nullable;
import com.mongodb.spark.sql.connector.config.CollectionsConfig;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.config.WriteConfig;
import com.mongodb.spark.sql.connector.exceptions.ConfigException;
import com.mongodb.spark.sql.connector.mongodb.MongoSparkConnectorTestCase;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opentest4j.AssertionFailedError;

/**
 * The abstract streaming test class.
 *
 * <p>When testing streaming reads, the memory sink is preferred due to speed. Note: When using the
 * memory sink `ds.collectAsList().size()` will return the current size. `ds.count()` does not
 * return immediately perhaps waiting for the stream to complete.
 */
abstract class AbstractMongoStreamTest extends MongoSparkConnectorTestCase {
  /*
   *
   */
  private static final String MEMORY = "memory";

  /*
   * The MongoDB sink is only used to test streaming writes.
   */
  private static final String MONGODB = "mongodb";

  abstract String collectionPrefix();

  abstract Trigger getTrigger();

  private String testIdentifier;

  @ParameterizedTest
  @ValueSource(strings = {"SINGLE", "MULTIPLE", "ALL"})
  void testStream(final String collectionsConfigModeStr) {
    assumeTrue(supportsChangeStreams());
    CollectionsConfig.Type collectionsConfigType =
        CollectionsConfig.Type.valueOf(collectionsConfigModeStr);
    testIdentifier = computeTestIdentifier("Simple", collectionsConfigType);
    testStreamingQuery(
        createMongoConfig(collectionsConfigType),
        withSource("inserting 0-25", (msg, coll) -> coll.insertMany(createDocuments(0, 25))),
        withMemorySink(
            "Expected to see 25 documents",
            (msg, ds) -> assertEquals(25, ds.collectAsList().size(), msg)),
        withSource("inserting 100-125", (msg, coll) -> coll.insertMany(createDocuments(100, 125))),
        withMemorySink(
            "Expecting to see 50 documents",
            (msg, ds) -> assertEquals(50, ds.collectAsList().size(), msg)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"MULTIPLE", "ALL"})
  void testStreamFromActuallyMultipleCollections(final String collectionsConfigModeStr) {
    assumeTrue(supportsChangeStreams());
    CollectionsConfig.Type collectionsConfigType =
        CollectionsConfig.Type.valueOf(collectionsConfigModeStr);
    testIdentifier = computeTestIdentifier("ActuallyMultiple", collectionsConfigType);
    String collectionName1 = collectionName() + "_1";
    String collectionName2 = collectionName() + "_2";
    String collectionName3 = collectionName() + "_3";
    int expectedNumberOfDocuments;
    Set<String> expectedCollectionNames;
    String collectionNameOptionValue;
    if (collectionsConfigType == CollectionsConfig.Type.ALL) {
      expectedNumberOfDocuments = 3;
      expectedCollectionNames =
          Stream.of(collectionName1, collectionName2, collectionName3).collect(Collectors.toSet());
      collectionNameOptionValue = "*";
    } else {
      expectedNumberOfDocuments = 2;
      expectedCollectionNames =
          Stream.of(collectionName1, collectionName3).collect(Collectors.toSet());
      collectionNameOptionValue = join(",", expectedCollectionNames);
    }
    testStreamingQuery(
        createMongoConfig(collectionsConfigType)
            .withOption(
                ReadConfig.READ_PREFIX + ReadConfig.COLLECTION_NAME_CONFIG,
                collectionNameOptionValue),
        DEFAULT_SCHEMA.add(
            "ns", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
        withSourceDb("Inserting 0-1 in " + collectionName1, (msg, db) -> db.getCollection(
                collectionName1, BsonDocument.class)
            .insertMany(createDocuments(0, 1))),
        withSourceDb("Inserting 2-3 in " + collectionName2, (msg, db) -> db.getCollection(
                collectionName2, BsonDocument.class)
            .insertMany(createDocuments(2, 3))),
        withSourceDb("Inserting 3-4 in " + collectionName3, (msg, db) -> db.getCollection(
                collectionName3, BsonDocument.class)
            .insertMany(createDocuments(3, 4))),
        withMemorySink(
            format(
                "Expected to see %d documents from %s",
                expectedNumberOfDocuments, expectedCollectionNames),
            (msg, ds) -> {
              List<Row> rows = ds.collectAsList();
              assertEquals(expectedNumberOfDocuments, rows.size(), msg);
              Set<String> actualCollectionNames = rows.stream()
                  .map(row -> row.<String, String>getMap(row.fieldIndex("ns"))
                      .get("coll")
                      .get())
                  .collect(Collectors.toSet());
              assertEquals(expectedCollectionNames, actualCollectionNames, msg);
            }));
  }

  @ParameterizedTest
  @ValueSource(strings = {"SINGLE", "MULTIPLE", "ALL"})
  void testStreamHandlesCollectionDrop(final String collectionsConfigModeStr) {
    assumeTrue(supportsChangeStreams());
    assumeTrue(isAtLeastFourDotFour());

    CollectionsConfig.Type collectionsConfigType =
        CollectionsConfig.Type.valueOf(collectionsConfigModeStr);
    testIdentifier = computeTestIdentifier("WithDrop", collectionsConfigType);
    int expectedInvalidateDocumentsCount =
        collectionsConfigType == CollectionsConfig.Type.SINGLE ? 1 : 0;
    testStreamingQuery(
        createMongoConfig(collectionsConfigType),
        withSource("inserting 0-25", (msg, coll) -> coll.insertMany(createDocuments(0, 25))),
        withMemorySink(
            "Expected to see 25 documents",
            (msg, ds) -> assertEquals(25, ds.collectAsList().size(), msg)),
        withSource("Dropping Collection", (msg, coll) -> coll.drop()),
        withMemorySink(
            "Expected to see 1 drop document",
            (msg, ds) -> assertEquals(
                1,
                ds.collectAsList().stream()
                    .filter(c -> c.get(c.fieldIndex("operationType")).equals("drop"))
                    .count(),
                msg)),
        withMemorySink(
            "Expected to see " + expectedInvalidateDocumentsCount + " invalidate document",
            (msg, ds) -> assertEquals(
                expectedInvalidateDocumentsCount,
                ds.collectAsList().stream()
                    .filter(c -> c.get(c.fieldIndex("operationType")).equals("invalidate"))
                    .count(),
                msg)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"SINGLE", "MULTIPLE", "ALL"})
  void testStreamWithFilter(final String collectionsConfigModeStr) {
    assumeTrue(supportsChangeStreams());
    assumeTrue(isAtLeastFourDotFour());

    CollectionsConfig.Type collectionsConfigType =
        CollectionsConfig.Type.valueOf(collectionsConfigModeStr);
    testIdentifier = computeTestIdentifier("WithFilter", collectionsConfigType);
    Column filterColumn = new Column("operationType").equalTo("insert");
    testStreamingQuery(
        createMongoConfig(collectionsConfigType)
            .withOption(
                ReadConfig.READ_PREFIX + ReadConfig.STREAM_LOOKUP_FULL_DOCUMENT_CONFIG,
                "updateLookup"),
        filterColumn,
        withSource("inserting 0-50", (msg, coll) -> coll.insertMany(createDocuments(0, 50))),
        withMemorySink(
            "Expected to see 50 documents",
            (msg, ds) -> assertEquals(50, ds.collectAsList().size(), msg)),
        withSource(
            "Deleting documents",
            (msg, coll) -> coll.deleteMany(Filters.in(
                "_id",
                IntStream.range(0, 50)
                    .filter(i -> i % 2 == 0)
                    .mapToObj(idFieldMapper())
                    .collect(Collectors.toList())))),
        withSource("Inserting 100-125", (msg, coll) -> coll.insertMany(createDocuments(100, 125))),
        withMemorySink(
            "Expected to see 75 documents",
            (msg, ds) -> assertEquals(75, ds.collectAsList().size(), msg)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"SINGLE", "MULTIPLE", "ALL"})
  void testStreamWithPublishFullDocumentOnly(final String collectionsConfigModeStr) {
    assumeTrue(supportsChangeStreams());
    assumeTrue(isAtLeastFourDotFour());

    CollectionsConfig.Type collectionsConfigType =
        CollectionsConfig.Type.valueOf(collectionsConfigModeStr);
    testIdentifier = computeTestIdentifier("FullDocOnly", collectionsConfigType);

    testStreamingQuery(
        createMongoConfig(collectionsConfigType)
            .withOption(
                ReadConfig.READ_PREFIX + ReadConfig.STREAM_PUBLISH_FULL_DOCUMENT_ONLY_CONFIG,
                "true")
            .withOption(
                ReadConfig.READ_PREFIX + ReadConfig.STREAM_LOOKUP_FULL_DOCUMENT_CONFIG,
                "overwritten / ignored")
            .withOption(WriteConfig.WRITE_PREFIX + WriteConfig.OPERATION_TYPE_CONFIG, "Update"),
        createStructType(asList(
            createStructField("_id", DataTypes.StringType, false),
            createStructField("a", DataTypes.StringType, false))),
        withSource(
            "Inserting 0-50",
            (msg, coll) -> coll.insertMany(createDocuments(
                0, 50, i -> new BsonDocument("_id", new BsonString(testIdentifier + "-" + i))
                    .append("a", new BsonString("a"))))),
        withMemorySink(
            "Expected to see 50 documents",
            (msg, ds) -> assertEquals(50, ds.collectAsList().size(), msg)),
        withSource(
            "Updating evens",
            (msg, coll) -> coll.updateMany(
                Filters.in(
                    "_id",
                    IntStream.range(0, 50)
                        .filter(i -> i % 2 == 0)
                        .mapToObj(idFieldMapper())
                        .collect(Collectors.toList())),
                Updates.set("a", new BsonString("b")))),
        withMemorySink(
            "Expected to see 25 documents",
            (msg, ds) -> assertEquals(
                25,
                ds.collectAsList().stream()
                    .filter(c -> c.get(c.fieldIndex("a")).equals("b"))
                    .count(),
                msg)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"SINGLE", "MULTIPLE", "ALL"})
  void testStreamPublishFullDocumentOnlyHandlesCollectionDrop(
      final String collectionsConfigModeStr) {
    assumeTrue(supportsChangeStreams());
    assumeTrue(isAtLeastFourDotFour());

    CollectionsConfig.Type collectionsConfigType =
        CollectionsConfig.Type.valueOf(collectionsConfigModeStr);
    testIdentifier = computeTestIdentifier("FullDocOnlyWithDrop", collectionsConfigType);
    testStreamingQuery(
        createMongoConfig(collectionsConfigType)
            .withOption(
                ReadConfig.READ_PREFIX + ReadConfig.STREAM_PUBLISH_FULL_DOCUMENT_ONLY_CONFIG,
                "true")
            .withOption(WriteConfig.WRITE_PREFIX + WriteConfig.OPERATION_TYPE_CONFIG, "Update"),
        createStructType(singletonList(createStructField("_id", DataTypes.StringType, false))),
        withSource("inserting 0-25", (msg, coll) -> coll.insertMany(createDocuments(0, 25))),
        withMemorySink(
            "Expected to see 25 documents",
            (msg, ds) -> assertEquals(25, ds.collectAsList().size(), msg)),
        withSource(
            "Dropping the collection",
            (msg, coll) -> coll.drop()), // Dropping collection shouldn't error
        withSource("Inserting 100-125", (msg, coll) -> coll.insertMany(createDocuments(100, 125))),
        withMemorySink(
            "Expected to see 25 documents",
            (msg, ds) -> assertEquals(25, ds.collectAsList().size(), msg)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"SINGLE", "MULTIPLE", "ALL"})
  void testStreamResumable(final String collectionsConfigModeStr) {
    assumeTrue(supportsChangeStreams());
    assumeTrue(isAtLeastFourDotFour());
    CollectionsConfig.Type collectionsConfigType =
        CollectionsConfig.Type.valueOf(collectionsConfigModeStr);
    testIdentifier = computeTestIdentifier("Resumable", collectionsConfigType);

    MongoConfig mongoConfig = createMongoConfig(collectionsConfigType);

    testStreamingQuery(
        "mongodb",
        mongoConfig,
        withSource("inserting 0-25", (msg, coll) -> coll.insertMany(createDocuments(0, 25))),
        withSink(
            "Expected to see 25 documents",
            (msg, ds) -> assertEquals(25, ds.countDocuments(), msg)));

    // Insert 50 documents - when there is no stream running
    getCollection(collectionName()).insertMany(createDocuments(100, 200));

    // Start the stream again - confirm it resumes at last point and sees the new documents
    testStreamingQuery(
        "mongodb",
        mongoConfig,
        withSource("Setup", (msg, coll) -> {} /* NOOP */),
        withSink(
            "Expecting to see 125 documents",
            (msg, ds) -> assertEquals(125, ds.countDocuments(), msg)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"SINGLE", "MULTIPLE", "ALL"})
  void testStreamStartAtOperationTime(final String collectionsConfigModeStr) {
    assumeTrue(supportsChangeStreams());
    assumeTrue(isAtLeastFourDotFour());
    CollectionsConfig.Type collectionsConfigType =
        CollectionsConfig.Type.valueOf(collectionsConfigModeStr);
    testIdentifier = computeTestIdentifier("startAtOperationTime", collectionsConfigType);

    ReadConfig readConfig = createMongoConfig(collectionsConfigType).toReadConfig();
    MongoCollection<BsonDocument> collection = getCollection(collectionName());

    // Add some documents prior to the start time
    collection.insertMany(createDocuments(0, 25));

    HELPER.sleep(1000);
    BsonTimestamp currentTimestamp = new BsonTimestamp((int) Instant.now().getEpochSecond(), 0);

    // Add some documents post start time

    collection.insertMany(createDocuments(100, 120));
    testStreamingQuery(
        readConfig
            .withOption(ReadConfig.STREAMING_STARTUP_MODE_CONFIG, "timestamp")
            .withOption(
                ReadConfig.STREAMING_STARTUP_MODE_TIMESTAMP_START_AT_OPERATION_TIME_CONFIG,
                format("{\"$timestamp\": {\"t\": %d, \"i\": 0}}", currentTimestamp.getTime())),
        withSource("Setup", (msg, coll) -> {} /* NOOP */),
        withMemorySink(
            "Expected to see 20 documents",
            (msg, ds) -> assertEquals(20, ds.collectAsList().size(), msg)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"SINGLE", "MULTIPLE", "ALL"})
  void testStreamCustomMongoClientFactory(final String collectionsConfigModeStr) {
    assumeTrue(supportsChangeStreams());
    CollectionsConfig.Type collectionsConfigType =
        CollectionsConfig.Type.valueOf(collectionsConfigModeStr);
    testIdentifier = computeTestIdentifier("CustomClientFactory", collectionsConfigType);
    testStreamingQuery(
        createMongoConfig(collectionsConfigType)
            .withOption(
                ReadConfig.PREFIX + ReadConfig.CLIENT_FACTORY_CONFIG,
                "com.mongodb.spark.sql.connector.read.CustomMongoClientFactory"),
        withSource("inserting 0-25", (msg, coll) -> coll.insertMany(createDocuments(0, 25))),
        withMemorySink(
            "Expected to see 25 documents",
            (msg, ds) -> assertEquals(25, ds.collectAsList().size(), msg)));

    assertTrue(CustomMongoClientFactory.CALLED.get());
  }

  @Test
  void testStreamInferSchemaNoData() {
    testIdentifier = "inferSchemaNoData";
    SparkSession spark = getOrCreateSparkSession();
    Throwable cause = assertThrows(Exception.class, () -> spark
        .readStream()
        .format(MONGODB)
        .load()
        .writeStream()
        .trigger(getTrigger())
        .format(MONGODB)
        .queryName("test")
        .outputMode("append")
        .start()
        .processAllAvailable());

    if (cause instanceof StreamingQueryException) {
      cause = ((StreamingQueryException) cause).cause();
    }
    assertTrue(cause instanceof ConfigException, format("Expected ConfigException: %s", cause));
    assertTrue(cause.getMessage().contains("streams require a schema to be explicitly defined"));
  }

  @Test
  void testStreamInferSchemaWithData() {
    testIdentifier = "inferSchemaWithData";
    SparkSession spark = getOrCreateSparkSession();

    getCollection().insertMany(createDocuments(0, 25));
    Throwable cause = assertThrows(Exception.class, () -> spark
        .readStream()
        .format(MONGODB)
        .load()
        .writeStream()
        .option("checkpointLocation", HELPER.getTempDirectory(true))
        .trigger(getTrigger())
        .format(MEMORY)
        .queryName("test")
        .outputMode("append")
        .start()
        .processAllAvailable());

    if (cause instanceof StreamingQueryException) {
      cause = ((StreamingQueryException) cause).cause();
    }
    assertTrue(cause instanceof ConfigException, format("Expected ConfigException: %s", cause));
    assertTrue(cause.getMessage().contains("streams require a schema to be explicitly defined"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"SINGLE", "MULTIPLE", "ALL"})
  void testStreamInferSchemaWithDataPublishFullOnly(final String collectionsConfigModeStr) {
    assumeTrue(supportsChangeStreams());
    CollectionsConfig.Type collectionsConfigType =
        CollectionsConfig.Type.valueOf(collectionsConfigModeStr);
    testIdentifier =
        computeTestIdentifier("inferSchemaWithDataPublishFullOnly", collectionsConfigType);

    ReadConfig readConfig = createMongoConfig(collectionsConfigType)
        .toReadConfig()
        .withOption(ReadConfig.STREAM_PUBLISH_FULL_DOCUMENT_ONLY_CONFIG, "true");

    withSource("inserting 0-1", (msg, coll) -> coll.insertMany(createDocuments(0, 1)))
        .accept(readConfig);
    HELPER.sleep(1000);

    testStreamingQuery(
        readConfig,
        IGNORE_SCHEMA,
        withSource("inserting 100-125", (msg, coll) -> coll.insertMany(createDocuments(100, 125))),
        withMemorySink(
            "Expected to see 25 documents",
            (msg, ds) -> assertEquals(25, ds.collectAsList().size(), msg)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"SINGLE", "MULTIPLE", "ALL"})
  void testStreamWriteStream(final String collectionsConfigModeStr) {
    assumeTrue(supportsChangeStreams());
    assumeTrue(isAtLeastFourDotFour());

    CollectionsConfig.Type collectionsConfigType =
        CollectionsConfig.Type.valueOf(collectionsConfigModeStr);
    testIdentifier = computeTestIdentifier("RoundTrip", collectionsConfigType);
    testStreamingQuery(
        "mongodb",
        createMongoConfig(collectionsConfigType),
        withSource("inserting 0-25", (msg, coll) -> coll.insertMany(createDocuments(0, 25))),
        withSink(
            "Expected to see 25 documents",
            (msg, coll) -> assertEquals(25, coll.countDocuments(), msg)),
        withSource("Inserting 100-125", (msg, coll) -> coll.insertMany(createDocuments(100, 125))),
        withSink(
            "Expected to see 50 documents",
            (msg, coll) -> assertEquals(50, coll.countDocuments(), msg)));
  }

  @Test
  void testLogCommentsInProfilerLogs() {
    assumeTrue(supportsChangeStreams());
    CollectionsConfig.Type collectionsConfigType = CollectionsConfig.Type.SINGLE;
    testIdentifier = computeTestIdentifier("logsComments", collectionsConfigType);
    MongoConfig mongoConfig =
        createMongoConfig(collectionsConfigType).withOption(PREFIX + COMMENT_CONFIG, TEST_COMMENT);

    assertCommentsInProfile(
        () -> testStreamingQuery(
            mongoConfig,
            withSource(
                "inserting 0-25",
                (msg, coll) -> coll.insertMany(
                    createDocuments(0, 25), new InsertManyOptions().comment(IGNORE_COMMENT))),
            withMemorySink(
                "Expected to see 25 documents",
                (msg, ds) -> assertEquals(25, ds.collectAsList().size(), msg))),
        mongoConfig.toReadConfig());
  }

  @ParameterizedTest
  @ValueSource(strings = {"SINGLE", "MULTIPLE", "ALL"})
  void testStreamHandlesDbDrop(final String collectionsConfigModeStr) {
    assumeTrue(supportsChangeStreams());
    CollectionsConfig.Type collectionsConfigType =
        CollectionsConfig.Type.valueOf(collectionsConfigModeStr);
    testIdentifier = computeTestIdentifier("WithDbDrop", collectionsConfigType);
    MongoConfig config = createMongoConfig(collectionsConfigType);
    ReadConfig readConfig = config.toReadConfig();
    Set<String> expectedEventTypes =
        Stream.of("insert", "drop", "invalidate").collect(Collectors.toSet());
    testStreamingQuery(
        config,
        withSource("inserting 0-3", (msg, coll) -> coll.insertMany(createDocuments(0, 3))),
        withMemorySink(
            "Expected to see 3 documents",
            (msg, ds) -> assertEquals(3, ds.collectAsList().size(), msg)),
        withSource(
            format("Dropping the database `%s`", readConfig.getDatabaseName()),
            (msg, coll) -> readConfig.doWithClient(
                client -> client.getDatabase(readConfig.getDatabaseName()).drop())),
        withMemorySink(
            "Expected to see 1 drop document",
            (msg, ds) -> assertEquals(
                1,
                ds.collectAsList().stream()
                    .filter(c -> c.get(c.fieldIndex("operationType")).equals("drop"))
                    .count(),
                msg)),
        withMemorySink(
            "Expected to see 1 invalidate document",
            (msg, ds) -> assertEquals(
                1,
                ds.collectAsList().stream()
                    .filter(c -> c.get(c.fieldIndex("operationType")).equals("invalidate"))
                    .count(),
                msg)),
        withMemorySink(
            "Expected to see 0 documents of operation types other than " + expectedEventTypes,
            (msg, ds) -> assertEquals(
                0,
                ds.collectAsList().stream()
                    .filter(c ->
                        !expectedEventTypes.contains(c.getString(c.fieldIndex("operationType"))))
                    .count(),
                msg)));
  }

  private static final StructType IGNORE_SCHEMA = createStructType(emptyList());

  private static final StructType DEFAULT_SCHEMA = createStructType(asList(
      createStructField("operationType", DataTypes.StringType, false),
      createStructField("clusterTime", DataTypes.StringType, false),
      createStructField("fullDocument", DataTypes.StringType, true)));

  @SafeVarargs
  private final void testStreamingQuery(
      final MongoConfig mongoConfig,
      final Consumer<MongoConfig> setup,
      final Consumer<MongoConfig>... consumers) {
    testStreamingQuery(MEMORY, mongoConfig, setup, consumers);
  }

  @SafeVarargs
  private final void testStreamingQuery(
      final String writeFormat,
      final MongoConfig mongoConfig,
      final Consumer<MongoConfig> setup,
      final Consumer<MongoConfig>... consumers) {
    testStreamingQuery(writeFormat, mongoConfig, DEFAULT_SCHEMA, null, setup, consumers);
  }

  @SafeVarargs
  private final void testStreamingQuery(
      final MongoConfig mongoConfig,
      final StructType schema,
      final Consumer<MongoConfig> setup,
      final Consumer<MongoConfig>... consumers) {
    testStreamingQuery(MEMORY, mongoConfig, schema, null, setup, consumers);
  }

  @SafeVarargs
  private final void testStreamingQuery(
      final MongoConfig mongoConfig,
      final Column condition,
      final Consumer<MongoConfig> setup,
      final Consumer<MongoConfig>... consumers) {
    testStreamingQuery(MEMORY, mongoConfig, DEFAULT_SCHEMA, condition, setup, consumers);
  }

  @SafeVarargs
  private final void testStreamingQuery(
      final String writeFormat,
      final MongoConfig mongoConfig,
      final StructType schema,
      final Column condition,
      final Consumer<MongoConfig> setup,
      final Consumer<MongoConfig>... consumers) {
    // see https://jira.mongodb.org/browse/SPARK-417 for the reason behind this call to `sleep`
    HELPER.sleep(2000);
    StreamingQuery streamingQuery =
        createStreamingQuery(writeFormat, mongoConfig, schema, condition);
    try {
      retryAssertion(() -> assertFalse(
          streamingQuery.status().message().contains("Initializing"), "Stream is not initialized"));

      // Give some time for the stream to be fully initialized and to be running.
      HELPER.sleep(2000);

      try {
        setup.accept(mongoConfig);
        LOGGER.info("Setup completed");
      } catch (Exception e) {
        throw new AssertionFailedError("Setup failed: " + e.getMessage(), e);
      }

      for (Consumer<MongoConfig> consumer : consumers) {

        retryAssertion(() -> consumer.accept(mongoConfig), () -> {
          withSource(
                  null,
                  (msg, coll) -> LOGGER.info(
                      "Source Collection Status: {}.",
                      coll.find()
                          .comment(IGNORE_COMMENT)
                          .map(BsonDocument::toJson)
                          .into(new ArrayList<>())))
              .accept(mongoConfig);

          if (writeFormat.equals(MONGODB)) {
            withSink(
                    null,
                    (msg, coll) -> LOGGER.info(
                        "Sink Collection Status: {}.",
                        coll.find()
                            .comment(IGNORE_COMMENT)
                            .map(BsonDocument::toJson)
                            .into(new ArrayList<>())))
                .accept(mongoConfig);
          } else {
            LOGGER.info(
                "Sink Memory Status: {}.",
                getOrCreateSparkSession()
                    .sql("select * from " + testIdentifier)
                    .collectAsList()
                    .stream()
                    .map(r -> Arrays.stream(r.schema().fields())
                        .map(f -> f.name() + ": " + r.get(r.fieldIndex(f.name())))
                        .collect(joining(", ", "{", "}")))
                    .collect(joining(", ", "[", "]")));
          }
        });
      }
    } catch (RuntimeException e) {
      fail("Assertions caused an exception", e);
    } finally {
      try {
        streamingQuery.stop();
        LOGGER.info("Stream stopped");
      } catch (TimeoutException e) {
        fail("Stopping the stream failed: ", e);
      }
    }
  }

  private StreamingQuery createStreamingQuery(
      final String writeFormat,
      final MongoConfig mongoConfig,
      final StructType schema,
      final Column condition) {

    DataStreamReader dfr = getOrCreateSparkSession(getSparkConf().set("numPartitions", "1"))
        .readStream()
        .format(MONGODB)
        .options(mongoConfig.toReadConfig().getOptions());

    if (schema != IGNORE_SCHEMA) {
      dfr = dfr.schema(schema);
    }

    Dataset<Row> ds = dfr.load();

    if (condition != null) {
      ds = ds.filter(condition);
    }

    try {
      return ds.writeStream()
          .format(writeFormat)
          .options(mongoConfig.toWriteConfig().getOptions())
          .queryName(testIdentifier)
          .trigger(getTrigger())
          .start();
    } catch (TimeoutException e) {
      return fail(e);
    }
  }

  private Consumer<MongoConfig> withSource(
      @Nullable final String msg,
      final BiConsumer<String, MongoCollection<BsonDocument>> biConsumer) {
    return mongoConfig -> {
      if (msg != null) {
        LOGGER.info("-> With source: {}", msg);
      }
      ReadConfig readConfig = mongoConfig.toReadConfig();
      MongoCollection<BsonDocument> collection = readConfig.withClient(client -> client
          .getDatabase(readConfig.getDatabaseName())
          .getCollection(collectionName(), BsonDocument.class));
      biConsumer.accept(msg, collection);
    };
  }

  private Consumer<MongoConfig> withSourceDb(
      @Nullable final String msg, final BiConsumer<String, MongoDatabase> biConsumer) {
    return mongoConfig -> {
      if (msg != null) {
        LOGGER.info("-> With source: {}", msg);
      }
      ReadConfig readConfig = mongoConfig.toReadConfig();
      MongoDatabase db =
          readConfig.withClient(client -> client.getDatabase(readConfig.getDatabaseName()));
      biConsumer.accept(msg, db);
    };
  }

  private Consumer<MongoConfig> withSink(
      @Nullable final String msg,
      final BiConsumer<String, MongoCollection<BsonDocument>> biConsumer) {
    return mongoConfig -> {
      if (msg != null) {
        LOGGER.info("-> With sink: " + msg);
      }
      WriteConfig writeConfig = mongoConfig.toWriteConfig();
      MongoCollection<BsonDocument> collection = writeConfig.withClient(client -> client
          .getDatabase(writeConfig.getDatabaseName())
          .getCollection(collectionName(), BsonDocument.class));
      biConsumer.accept(msg, collection);
    };
  }

  private Consumer<MongoConfig> withMemorySink(
      final String msg, final BiConsumer<String, Dataset<Row>> biConsumer) {
    return mongoConfig -> {
      LOGGER.info("<- With memory sink: " + msg);
      biConsumer.accept(msg, getOrCreateSparkSession().sql("SELECT * FROM " + testIdentifier));
    };
  }

  private MongoConfig createMongoConfig(final CollectionsConfig.Type collectionsConfigType) {
    Map<String, String> options = new HashMap<>();
    Arrays.stream(getSparkConf().getAllWithPrefix(MongoConfig.PREFIX))
        .forEach(t -> options.put(MongoConfig.PREFIX + t._1(), t._2()));
    String collectionsConfigOptionValue;
    switch (collectionsConfigType) {
      case SINGLE:
        collectionsConfigOptionValue = collectionName();
        break;
      case MULTIPLE:
        collectionsConfigOptionValue =
            join(",", "collectionNameThatDoesNotExist", collectionName());
        break;
      case ALL:
        collectionsConfigOptionValue = "*";
        break;
      default:
        throw com.mongodb.assertions.Assertions.fail();
    }
    options.put(
        ReadConfig.READ_PREFIX + ReadConfig.COLLECTION_NAME_CONFIG, collectionsConfigOptionValue);
    return MongoConfig.createConfig(options);
  }

  private static String computeTestIdentifier(
      final String testIdentifierStart, final CollectionsConfig.Type collectionsConfigType) {
    return testIdentifierStart + "_" + collectionsConfigType;
  }

  private String collectionName() {
    return collectionPrefix() + "Source" + testIdentifier;
  }

  private List<BsonDocument> createDocuments(final int startInclusive, final int endExclusive) {
    return createDocuments(
        startInclusive,
        endExclusive,
        i -> new BsonDocument("_id", idFieldMapper().apply(i)));
  }

  private List<BsonDocument> createDocuments(
      final int startInclusive,
      final int endExclusive,
      final IntFunction<BsonDocument> documentMapper) {
    return IntStream.range(startInclusive, endExclusive)
        .mapToObj(documentMapper)
        .collect(Collectors.toList());
  }

  private IntFunction<BsonString> idFieldMapper() {
    return i -> new BsonString(testIdentifier + "-" + i);
  }
}
