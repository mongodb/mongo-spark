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

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.config.WriteConfig;
import com.mongodb.spark.sql.connector.exceptions.ConfigException;
import com.mongodb.spark.sql.connector.mongodb.MongoSparkConnectorTestCase;

abstract class AbstractMongoStreamTest extends MongoSparkConnectorTestCase {

  abstract String collectionPrefix();

  abstract Trigger getTrigger();

  @Test
  void testStream() {
    assumeTrue(supportsChangeStreams());
    testStreamingQuery(
        createMongoConfig("Simple"),
        withSource(
            coll ->
                coll.insertMany(
                    IntStream.range(0, 25)
                        .mapToObj(i -> new BsonDocument("_id", new BsonInt32(i)))
                        .collect(Collectors.toList()))),
        withSink(coll -> assertEquals(25, coll.countDocuments(), "Expected to see 25 documents")),
        withSource(
            coll ->
                coll.insertMany(
                    IntStream.range(100, 125)
                        .mapToObj(i -> new BsonDocument("_id", new BsonInt32(i)))
                        .collect(Collectors.toList()))),
        withSink(coll -> assertEquals(50, coll.countDocuments(), "Expected to see 50 documents")));
  }

  @Test
  void testStreamHandlesCollectionDrop() {
    assumeTrue(supportsChangeStreams());
    assumeTrue(isAtLeastFourDotFour());

    testStreamingQuery(
        createMongoConfig("WithDrop"),
        withSource(
            coll ->
                coll.insertMany(
                    IntStream.range(0, 25)
                        .mapToObj(i -> new BsonDocument("_id", new BsonInt32(i)))
                        .collect(Collectors.toList()))),
        withSink(coll -> assertEquals(25, coll.countDocuments(), "Expected to see 25 documents")),
        withSource(MongoCollection::drop),
        withSink(
            coll ->
                assertEquals(
                    1,
                    coll.countDocuments(Filters.eq("operationType", "drop")),
                    "Expected to see 1 drop document")),
        withSink(
            coll ->
                assertEquals(
                    1,
                    coll.countDocuments(Filters.eq("operationType", "invalidate")),
                    "Expected to see 1 invalidate document")));
  }

  @Test
  void testStreamWithFilter() {
    assumeTrue(supportsChangeStreams());
    assumeTrue(isAtLeastFourDotFour());

    Column filterColumn = new Column("operationType").equalTo("insert");
    testStreamingQuery(
        createMongoConfig("WithFilter")
            .withOption(
                ReadConfig.READ_PREFIX + ReadConfig.STREAM_LOOKUP_FULL_DOCUMENT_CONFIG,
                "updateLookup"),
        DEFAULT_SCHEMA,
        filterColumn,
        withSource(
            coll -> {
              coll.insertMany(
                  IntStream.range(0, 50)
                      .mapToObj(i -> new BsonDocument("_id", new BsonInt32(i)))
                      .collect(Collectors.toList()));
              coll.deleteMany(
                  Filters.in(
                      "_id",
                      IntStream.range(0, 50)
                          .filter(i -> i % 2 == 0)
                          .boxed()
                          .collect(Collectors.toList())));
            }),
        withSink(coll -> assertEquals(50, coll.countDocuments(), "Seen 50 documents")));
  }

  @Test
  void testStreamWithPublishFullDocumentOnly() {
    assumeTrue(supportsChangeStreams());
    assumeTrue(isAtLeastFourDotFour());

    testStreamingQuery(
        createMongoConfig("FullDocOnly")
            .withOption(
                ReadConfig.READ_PREFIX + ReadConfig.STREAM_PUBLISH_FULL_DOCUMENT_ONLY_CONFIG,
                "true")
            .withOption(
                ReadConfig.READ_PREFIX + ReadConfig.STREAM_LOOKUP_FULL_DOCUMENT_CONFIG,
                "overwritten / ignored")
            .withOption(WriteConfig.WRITE_PREFIX + WriteConfig.OPERATION_TYPE_CONFIG, "Update"),
        createStructType(
            asList(
                createStructField("_id", DataTypes.IntegerType, false),
                createStructField("a", DataTypes.StringType, false))),
        withSource(
            coll -> {
              coll.insertMany(
                  IntStream.range(0, 50)
                      .mapToObj(
                          i ->
                              new BsonDocument("_id", new BsonInt32(i))
                                  .append("a", new BsonString("a")))
                      .collect(Collectors.toList()));
              coll.updateMany(
                  Filters.in(
                      "_id",
                      IntStream.range(0, 50)
                          .filter(i -> i % 2 == 0)
                          .boxed()
                          .collect(Collectors.toList())),
                  Updates.set("a", new BsonString("b")));
            }),
        withSink(
            coll -> {
              assertEquals(50, (Long) coll.countDocuments(), "Expected to see 50 documents");
              assertEquals(
                  25,
                  (Long) coll.countDocuments(Filters.eq("a", "b")),
                  "Expected to see 25 documents");
            }));
  }

  @Test
  void testStreamPublishFullDocumentOnlyHandlesCollectionDrop() {
    assumeTrue(supportsChangeStreams());
    assumeTrue(isAtLeastFourDotFour());

    testStreamingQuery(
        createMongoConfig("FullDocOnlyWithDrop")
            .withOption(
                ReadConfig.READ_PREFIX + ReadConfig.STREAM_PUBLISH_FULL_DOCUMENT_ONLY_CONFIG,
                "true")
            .withOption(WriteConfig.WRITE_PREFIX + WriteConfig.OPERATION_TYPE_CONFIG, "Update"),
        createStructType(
            asList(
                createStructField("_id", DataTypes.IntegerType, false),
                createStructField("a", DataTypes.StringType, false))),
        withSource(
            coll ->
                coll.insertMany(
                    IntStream.range(0, 25)
                        .mapToObj(
                            i ->
                                new BsonDocument("_id", new BsonInt32(i))
                                    .append("a", new BsonString("a")))
                        .collect(Collectors.toList()))),
        withSink(coll -> assertEquals(25, coll.countDocuments(), "Expected to see 25 documents")),
        withSource(MongoCollection::drop), // Dropping collection shouldn't error
        withSink(coll -> assertEquals(25, coll.countDocuments(), "Expected to see 25 documents")));
  }

  @Test
  void testStreamCustomMongoClientFactory() {
    assumeTrue(supportsChangeStreams());
    testStreamingQuery(
        createMongoConfig(
            "CustomClientFactory",
            ReadConfig.PREFIX + ReadConfig.CLIENT_FACTORY_CONFIG,
            "com.mongodb.spark.sql.connector.read.CustomMongoClientFactory"),
        withSource(
            coll ->
                coll.insertMany(
                    IntStream.range(0, 25)
                        .mapToObj(i -> new BsonDocument("_id", new BsonInt32(i)))
                        .collect(Collectors.toList()))),
        withSink(coll -> assertEquals(25, coll.countDocuments(), "Expected to see 25 documents")),
        withSource(
            coll ->
                coll.insertMany(
                    IntStream.range(100, 125)
                        .mapToObj(i -> new BsonDocument("_id", new BsonInt32(i)))
                        .collect(Collectors.toList()))),
        withSink(coll -> assertEquals(50, coll.countDocuments(), "Expected to see 50 documents")));

    assertTrue(CustomMongoClientFactory.CALLED.get());
  }

  @Test
  void testStreamNoSchema() {
    SparkSession spark = getOrCreateSparkSession();
    Throwable cause =
        assertThrows(
            Exception.class,
            () ->
                spark
                    .readStream()
                    .format("mongodb")
                    .load()
                    .writeStream()
                    .trigger(getTrigger())
                    .format("memory")
                    .queryName("test")
                    .outputMode("append")
                    .start()
                    .processAllAvailable());

    if (cause instanceof StreamingQueryException) {
      cause = ((StreamingQueryException) cause).cause();
    }
    assertTrue(cause instanceof ConfigException, format("Expected ConfigException: %s", cause));
  }

  private static final StructType DEFAULT_SCHEMA =
      createStructType(
          asList(
              createStructField("operationType", DataTypes.StringType, false),
              createStructField("fullDocument", DataTypes.StringType, true)));

  @SafeVarargs
  private final void testStreamingQuery(
      final MongoConfig mongoConfig,
      final Consumer<MongoConfig> setup,
      final Consumer<MongoConfig>... consumers) {
    testStreamingQuery(mongoConfig, DEFAULT_SCHEMA, null, setup, consumers);
  }

  @SafeVarargs
  private final void testStreamingQuery(
      final MongoConfig mongoConfig,
      final StructType schema,
      final Consumer<MongoConfig> setup,
      final Consumer<MongoConfig>... consumers) {
    testStreamingQuery(mongoConfig, schema, null, setup, consumers);
  }

  @SafeVarargs
  private final void testStreamingQuery(
      final MongoConfig mongoConfig,
      final StructType schema,
      final Column condition,
      final Consumer<MongoConfig> setup,
      final Consumer<MongoConfig>... consumers) {

    StreamingQuery streamingQuery = createStreamingQuery(mongoConfig, schema, condition);
    try {
      retryAssertion(
          () ->
              assertFalse(
                  streamingQuery.status().message().contains("Initializing"),
                  "Stream is not initialized"));
      try {
        setup.accept(mongoConfig);
      } catch (Exception e) {
        throw new AssertionFailedError("Setup failed: " + e.getMessage());
      }

      for (Consumer<MongoConfig> consumer : consumers) {
        retryAssertion(() -> consumer.accept(mongoConfig));
      }
    } catch (RuntimeException e) {
      fail(e);
    } finally {
      try {
        streamingQuery.stop();
      } catch (TimeoutException e) {
        fail(e);
      }
    }
  }

  private StreamingQuery createStreamingQuery(
      final MongoConfig mongoConfig, final StructType schema, final Column condition) {
    Dataset<Row> ds =
        getOrCreateSparkSession(getSparkConf().set("numPartitions", "1"))
            .readStream()
            .format("mongodb")
            .options(mongoConfig.toReadConfig().getOptions())
            .schema(schema)
            .load();

    if (condition != null) {
      ds = ds.filter(condition);
    }

    try {
      return ds.writeStream()
          .format("mongodb")
          .options(mongoConfig.toWriteConfig().getOptions())
          .trigger(getTrigger())
          .outputMode("append")
          .start();
    } catch (TimeoutException e) {
      return fail(e);
    }
  }

  private Consumer<MongoConfig> withSource(final Consumer<MongoCollection<BsonDocument>> consumer) {
    return mongoConfig -> mongoConfig.toReadConfig().doWithCollection(consumer);
  }

  private Consumer<MongoConfig> withSink(final Consumer<MongoCollection<BsonDocument>> consumer) {
    return mongoConfig -> mongoConfig.toWriteConfig().doWithCollection(consumer);
  }

  private MongoConfig createMongoConfig(final String suffix) {
    return createMongoConfig(suffix, null, null);
  }

  private MongoConfig createMongoConfig(
      final String suffix, final String extraKey, final String extraValue) {
    Map<String, String> options = new HashMap<>();
    Arrays.stream(getSparkConf().getAllWithPrefix(MongoConfig.PREFIX))
        .forEach(t -> options.put(MongoConfig.PREFIX + t._1(), t._2()));
    options.put(
        ReadConfig.READ_PREFIX + ReadConfig.COLLECTION_NAME_CONFIG,
        collectionPrefix() + "SourceColl" + suffix);
    options.put(
        WriteConfig.WRITE_PREFIX + WriteConfig.COLLECTION_NAME_CONFIG,
        collectionPrefix() + "SinkColl" + suffix);

    if (extraKey != null && extraValue != null) {
      options.put(extraKey, extraValue);
    }
    return MongoConfig.createConfig(options);
  }
}
