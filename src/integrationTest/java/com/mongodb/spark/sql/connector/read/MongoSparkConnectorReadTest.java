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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

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

class MongoSparkConnectorReadTest extends MongoSparkConnectorTestCase {
  private static final String READ_RESOURCES_JSON_PATH =
      "src/integrationTest/resources/data/read/*.json";

  @Test
  void testReadsAreSupportedWithSchemaSupplied() {
    SparkSession spark = getOrCreateSparkSession();

    List<BsonDocument> collectionData =
        toBsonDocuments(spark.read().textFile(READ_RESOURCES_JSON_PATH));
    getCollection().insertMany(collectionData);

    StructType schema =
        createStructType(
            asList(
                createStructField("_id", DataTypes.IntegerType, false),
                createStructField("age", DataTypes.LongType, true),
                createStructField("name", DataTypes.StringType, true)));

    assertIterableEquals(
        collectionData,
        toBsonDocuments(spark.read().format("mongodb").schema(schema).load().toJSON()));
  }

  @Test
  void testReadsAreSupportedWithSchemaInferred() {
    SparkSession spark = getOrCreateSparkSession();

    String collectionName = "inferredTest";
    List<BsonDocument> collectionData =
        toBsonDocuments(spark.read().textFile(READ_RESOURCES_JSON_PATH));
    getDatabase()
        .getCollection(collectionName)
        .withDocumentClass(BsonDocument.class)
        .insertMany(collectionData);

    assertIterableEquals(
        collectionData,
        toBsonDocuments(
            spark
                .read()
                .format("mongodb")
                .option(ReadConfig.COLLECTION_NAME_CONFIG, collectionName)
                .load()
                .toJSON()));
  }

  @Test
  void testReadsAreSupportedWithFilters() {
    SparkSession spark = getOrCreateSparkSession();

    List<BsonDocument> collectionData =
        toBsonDocuments(spark.read().textFile(READ_RESOURCES_JSON_PATH));
    getCollection().insertMany(collectionData);
    getCollection().insertOne(BsonDocument.parse("{_id: 10, name: 'Bombur'}"));

    Dataset<Row> ds = spark.read().format("mongodb").load();

    // EqualNullSafe
    assertIterableEquals(
        singletonList("Gandalf"),
        ds.filter(new Column("age").eqNullSafe(1000))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // EqualTo
    assertIterableEquals(
        singletonList("Gandalf"),
        ds.filter(new Column("age").equalTo(1000))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // GreaterThan
    assertIterableEquals(
        asList("Gandalf", "Thorin"),
        ds.filter(new Column("age").gt(178))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // GreaterThanOrEqual
    assertIterableEquals(
        asList("Gandalf", "Thorin", "Balin"),
        ds.filter(new Column("age").geq(178))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // In
    assertIterableEquals(
        asList("Kíli", "Fíli"),
        ds.filter(new Column("name").isin("Kíli", "Fíli"))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // IsNull
    assertIterableEquals(
        singletonList("Bombur"),
        ds.filter(new Column("age").isNull())
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // LessThan
    assertIterableEquals(
        asList("Bilbo Baggins", "Kíli"),
        ds.filter(new Column("age").lt(82))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // LessThanOrEqual
    assertIterableEquals(
        asList("Bilbo Baggins", "Kíli", "Fíli"),
        ds.filter(new Column("age").leq(82))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // Not
    assertIterableEquals(
        asList("Gandalf", "Thorin", "Balin", "Kíli", "Dwalin", "Óin", "Glóin", "Fíli"),
        ds.filter(new Column("age").notEqual(50))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // StringContains
    assertIterableEquals(
        asList("Bilbo Baggins", "Thorin", "Balin", "Dwalin", "Óin", "Glóin"),
        ds.filter(new Column("name").contains("in"))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // StringEndsWith
    assertIterableEquals(
        asList("Kíli", "Fíli"),
        ds.filter(new Column("name").endsWith("li"))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // StringStartsWith
    assertIterableEquals(
        asList("Gandalf", "Glóin"),
        ds.filter(new Column("name").startsWith("G"))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // And
    assertIterableEquals(
        singletonList("Gandalf"),
        ds.filter(new Column("name").startsWith("G").and(new Column("age").gt(200)))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());
    // Or
    assertIterableEquals(
        asList("Bilbo Baggins", "Balin", "Kíli", "Fíli", "Bombur"),
        ds.filter(new Column("name").startsWith("B").or(new Column("age").lt(150)))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // IsNotNull - filter handled by Spark alone
    assertIterableEquals(
        asList(
            "Bilbo Baggins",
            "Gandalf",
            "Thorin",
            "Balin",
            "Kíli",
            "Dwalin",
            "Óin",
            "Glóin",
            "Fíli"),
        ds.filter(new Column("age").isNotNull())
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());
  }

  @Test
  void testContinuousStream() {
    assumeTrue(supportsChangeStreams());
    testStreamingQuery(
        createMongoConfig("continuousStream"),
        coll ->
            coll.insertMany(
                IntStream.range(0, 25)
                    .mapToObj(i -> new BsonDocument("_id", new BsonInt32(i)))
                    .collect(Collectors.toList())),
        coll -> assertEquals(25, (Long) coll.countDocuments(), "Expected to see 25 documents"));
  }

  @Test
  void testContinuousStreamHandlesCollectionDrop() {
    assumeTrue(supportsChangeStreams());
    assumeTrue(isAtLeastFourDotTwo());

    MongoConfig mongoConfig = createMongoConfig("withDrop");
    withStreamingQuery(
        () -> createStreamingQuery(mongoConfig, DEFAULT_SCHEMA, null),
        (streamingQuery) -> {
          ReadConfig readConfig = mongoConfig.toReadConfig();
          WriteConfig writeConfig = mongoConfig.toWriteConfig();

          // Insert some data
          readConfig.doWithCollection(
              coll ->
                  coll.insertMany(
                      IntStream.range(0, 25)
                          .mapToObj(i -> new BsonDocument("_id", new BsonInt32(i)))
                          .collect(Collectors.toList())));

          retryAssertion(
              () ->
                  writeConfig.doWithCollection(
                      coll ->
                          assertEquals(25, coll.countDocuments(), "Expected to see 25 documents")));

          // Dropping collection shouldn't error
          readConfig.doWithCollection(MongoCollection::drop);
          retryAssertion(
              () ->
                  writeConfig.doWithCollection(
                      coll ->
                          assertEquals(
                              1,
                              (Long) coll.countDocuments(Filters.eq("operationType", "drop")),
                              "Expected to see 1 drop document")));
          retryAssertion(
              () ->
                  writeConfig.doWithCollection(
                      coll ->
                          assertEquals(
                              1,
                              (Long) coll.countDocuments(Filters.eq("operationType", "invalidate")),
                              "Expected to see 1 invalidate document")));
        });
  }

  @Test
  void testContinuousStreamWithFilter() {
    assumeTrue(supportsChangeStreams());
    assumeTrue(isAtLeastFourDotTwo());

    MongoConfig mongoConfig =
        createMongoConfig("withFilter")
            .withOption(
                ReadConfig.READ_PREFIX + ReadConfig.STREAM_LOOKUP_FULL_DOCUMENT_CONFIG,
                "updateLookup");

    Column filterColumn = new Column("operationType").equalTo("insert");
    testStreamingQuery(
        mongoConfig,
        filterColumn,
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
        },
        coll -> assertEquals(50, (Long) coll.countDocuments(), "Seen 50 documents"));
  }

  @Test
  void testContinuousStreamWithPublishFullDocumentOnly() {
    assumeTrue(supportsChangeStreams());
    assumeTrue(isAtLeastFourDotTwo());
    MongoConfig mongoConfig =
        createMongoConfig("fullDocOnly")
            .withOption(
                ReadConfig.READ_PREFIX + ReadConfig.STREAM_PUBLISH_FULL_DOCUMENT_ONLY_CONFIG,
                "true")
            .withOption(
                ReadConfig.READ_PREFIX + ReadConfig.STREAM_LOOKUP_FULL_DOCUMENT_CONFIG,
                "overwritten / ignored")
            .withOption(WriteConfig.WRITE_PREFIX + WriteConfig.OPERATION_TYPE_CONFIG, "Update");

    StructType schema =
        createStructType(
            asList(
                createStructField("_id", DataTypes.IntegerType, false),
                createStructField("a", DataTypes.StringType, false)));

    testStreamingQuery(
        mongoConfig,
        schema,
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
        },
        coll -> {
          assertEquals(50, (Long) coll.countDocuments(), "Expected to see 50 documents");
          assertEquals(
              25, (Long) coll.countDocuments(Filters.eq("a", "b")), "Expected to see 25 documents");
        });
  }

  @Test
  void testContinuousStreamPublishFullDocumentOnlyHandlesCollectionDrop() {
    assumeTrue(supportsChangeStreams());
    assumeTrue(isAtLeastFourDotTwo());
    MongoConfig mongoConfig =
        createMongoConfig("fullDocOnlyWithDrop")
            .withOption(
                ReadConfig.READ_PREFIX + ReadConfig.STREAM_PUBLISH_FULL_DOCUMENT_ONLY_CONFIG,
                "true")
            .withOption(WriteConfig.WRITE_PREFIX + WriteConfig.OPERATION_TYPE_CONFIG, "Update");

    StructType schema =
        createStructType(
            asList(
                createStructField("_id", DataTypes.IntegerType, false),
                createStructField("a", DataTypes.StringType, false)));

    withStreamingQuery(
        () -> createStreamingQuery(mongoConfig, schema, null),
        (streamingQuery) -> {
          ReadConfig readConfig = mongoConfig.toReadConfig();
          WriteConfig writeConfig = mongoConfig.toWriteConfig();
          retryAssertion(
              () ->
                  assertFalse(
                      streamingQuery.status().message().contains("Initializing"),
                      "Stream is not initialized"));

          // Insert some data
          readConfig.doWithCollection(
              coll ->
                  coll.insertMany(
                      IntStream.range(0, 25)
                          .mapToObj(
                              i ->
                                  new BsonDocument("_id", new BsonInt32(i))
                                      .append("a", new BsonString("a")))
                          .collect(Collectors.toList())));

          retryAssertion(
              () ->
                  writeConfig.doWithCollection(
                      coll ->
                          assertEquals(25, coll.countDocuments(), "Expected to see 25 documents")));

          // Dropping collection shouldn't error
          readConfig.doWithCollection(MongoCollection::drop);
          retryAssertion(
              () ->
                  writeConfig.doWithCollection(
                      coll ->
                          assertEquals(25, coll.countDocuments(), "Expected to see 25 documents")));
        });
  }

  @Test
  void testContinuousStreamNoSchema() {
    SparkSession spark = getOrCreateSparkSession();
    assertThrows(
        ConfigException.class,
        () ->
            spark
                .readStream()
                .format("mongodb")
                .load()
                .writeStream()
                .trigger(Trigger.Continuous("1 seconds"))
                .format("memory")
                .queryName("test")
                .outputMode("append")
                .start());
  }

  @Test
  void testMicroBatchStreamingReadsAreNotSupported() {
    SparkSession spark = getOrCreateSparkSession();
    StreamingQueryException streamingQueryException =
        assertThrows(
            StreamingQueryException.class,
            () ->
                spark
                    .readStream()
                    .format("mongodb")
                    .load()
                    .writeStream()
                    .outputMode("append")
                    .format("console")
                    .trigger(Trigger.ProcessingTime("5 seconds"))
                    .start()
                    .processAllAvailable());
    assertInstanceOf(UnsupportedOperationException.class, streamingQueryException.cause());
  }

  private List<BsonDocument> toBsonDocuments(final Dataset<String> dataset) {
    return dataset.toJavaRDD().map(BsonDocument::parse).collect();
  }

  private static final StructType DEFAULT_SCHEMA =
      createStructType(
          asList(
              createStructField("operationType", DataTypes.StringType, false),
              createStructField("fullDocument", DataTypes.StringType, true)));

  @SafeVarargs
  private final void testStreamingQuery(
      final MongoConfig mongoConfig,
      final Consumer<MongoCollection<BsonDocument>> setup,
      final Consumer<MongoCollection<BsonDocument>>... assertions) {
    testStreamingQuery(mongoConfig, DEFAULT_SCHEMA, setup, assertions);
  }

  @SafeVarargs
  private final void testStreamingQuery(
      final MongoConfig mongoConfig,
      final StructType schema,
      final Consumer<MongoCollection<BsonDocument>> setup,
      final Consumer<MongoCollection<BsonDocument>>... assertions) {
    testStreamingQuery(mongoConfig, schema, null, setup, assertions);
  }

  @SafeVarargs
  private final void testStreamingQuery(
      final MongoConfig mongoConfig,
      final Column condition,
      final Consumer<MongoCollection<BsonDocument>> setup,
      final Consumer<MongoCollection<BsonDocument>>... assertions) {
    testStreamingQuery(mongoConfig, DEFAULT_SCHEMA, condition, setup, assertions);
  }

  @SafeVarargs
  private final void testStreamingQuery(
      final MongoConfig mongoConfig,
      final StructType schema,
      final Column condition,
      final Consumer<MongoCollection<BsonDocument>> setup,
      final Consumer<MongoCollection<BsonDocument>>... assertions) {

    withStreamingQuery(
        () -> createStreamingQuery(mongoConfig, schema, condition),
        (streamingQuery) -> {
          retryAssertion(
              () ->
                  assertFalse(
                      streamingQuery.status().message().contains("Initializing"),
                      "Stream is not initialized"));
          mongoConfig.toReadConfig().doWithCollection(setup);

          retryAssertion(
              () -> {
                WriteConfig writeConfig = mongoConfig.toWriteConfig();
                for (Consumer<MongoCollection<BsonDocument>> assertion : assertions) {
                  writeConfig.doWithCollection(assertion);
                }
              });
        });
  }

  @SafeVarargs
  private final void withStreamingQuery(
      final Supplier<StreamingQuery> supplier, final Consumer<StreamingQuery>... consumers) {
    StreamingQuery streamingQuery = supplier.get();
    try {
      retryAssertion(
          () ->
              assertFalse(
                  streamingQuery.status().message().contains("Initializing"),
                  "Stream is not initialized"));

      for (Consumer<StreamingQuery> consumer : consumers) {
        consumer.accept(streamingQuery);
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
        getOrCreateSparkSession()
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
          .trigger(Trigger.Continuous("1 seconds"))
          .outputMode("append")
          .start();
    } catch (TimeoutException e) {
      return fail(e);
    }
  }

  private MongoConfig createMongoConfig(final String suffix) {

    Map<String, String> overrides = new HashMap<>();
    overrides.put(
        ReadConfig.READ_PREFIX + ReadConfig.COLLECTION_NAME_CONFIG, "sourceColl" + suffix);
    overrides.put(
        WriteConfig.WRITE_PREFIX + WriteConfig.COLLECTION_NAME_CONFIG, "sinkColl" + suffix);
    return createMongoConfig(overrides);
  }

  private MongoConfig createMongoConfig(final Map<String, String> overrides) {
    Map<String, String> options = new HashMap<>();
    Arrays.stream(getSparkConf().getAllWithPrefix(MongoConfig.PREFIX))
        .forEach(t -> options.put(MongoConfig.PREFIX + t._1(), t._2()));
    options.putAll(overrides);
    return MongoConfig.createConfig(options);
  }
}
