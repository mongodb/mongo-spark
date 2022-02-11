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
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.sql.Dataset;
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

import com.mongodb.client.MongoCollection;

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

    List<BsonDocument> collectionData =
        toBsonDocuments(spark.read().textFile(READ_RESOURCES_JSON_PATH));
    getCollection().insertMany(collectionData);

    assertIterableEquals(
        collectionData, toBsonDocuments(spark.read().format("mongodb").load().toJSON()));
  }

  @Test
  void testContinuousStreamWithSchema() throws TimeoutException {
    assumeTrue(supportsChangeStreams());
    SparkSession spark = getOrCreateSparkSession();

    Map<String, String> configs = new HashMap<>();
    Arrays.stream(getSparkConf().getAllWithPrefix(ReadConfig.PREFIX))
        .forEach(t -> configs.put(t._1(), t._2()));
    configs.put(ReadConfig.READ_PREFIX + ReadConfig.COLLECTION_NAME_CONFIG, "sourceColl");
    configs.put(WriteConfig.WRITE_PREFIX + WriteConfig.COLLECTION_NAME_CONFIG, "sinkColl");

    ReadConfig readConfig = MongoConfig.readConfig(configs);
    WriteConfig writeConfig = MongoConfig.writeConfig(configs);

    StructType schema =
        createStructType(
            asList(
                createStructField("operationType", DataTypes.StringType, false),
                createStructField("fullDocument", DataTypes.StringType, true)));

    Dataset<Row> streamingMongoDataset =
        spark.readStream().format("mongodb").options(readConfig.getOptions()).schema(schema).load();

    StreamingQuery streamingQuery =
        streamingMongoDataset
            .writeStream()
            .trigger(Trigger.Continuous("1 seconds"))
            .format("mongodb")
            .options(writeConfig.getOptions())
            .outputMode("append")
            .start();

    try {
      retryAssertion(
          () ->
              assertFalse(
                  streamingQuery.status().message().contains("Initializing"),
                  "Stream is not initialized"));

      readConfig.doWithCollection(
          coll ->
              coll.insertMany(
                  IntStream.range(0, 50)
                      .mapToObj(i -> new BsonDocument("_id", new BsonInt32(i)))
                      .collect(Collectors.toList())));

      retryAssertion(
          () ->
              assertEquals(
                  50,
                  (Long) writeConfig.withCollection(MongoCollection::countDocuments),
                  "Seen 50 documents"));

    } finally {
      streamingQuery.stop();
    }
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
}
