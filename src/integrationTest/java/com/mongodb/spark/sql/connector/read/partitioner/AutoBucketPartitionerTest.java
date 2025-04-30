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

package com.mongodb.spark.sql.connector.read.partitioner;

import static com.mongodb.spark.sql.connector.config.MongoConfig.COMMENT_CONFIG;
import static com.mongodb.spark.sql.connector.config.ReadConfig.PARTITIONER_OPTIONS_PREFIX;
import static com.mongodb.spark.sql.connector.read.partitioner.AutoBucketPartitioner.PARTITION_CHUNK_SIZE_MB_CONFIG;
import static com.mongodb.spark.sql.connector.read.partitioner.AutoBucketPartitioner.PARTITION_FIELD_LIST_CONFIG;
import static com.mongodb.spark.sql.connector.read.partitioner.AutoBucketPartitioner.SAMPLES_PER_PARTITION_CONFIG;
import static com.mongodb.spark.sql.connector.read.partitioner.AutoBucketPartitioner.createMongoInputPartitions;
import static com.mongodb.spark.sql.connector.read.partitioner.PartitionerHelper.SINGLE_PARTITIONER;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.ConfigException;
import com.mongodb.spark.sql.connector.read.MongoInputPartition;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

public class AutoBucketPartitionerTest extends PartitionerTestCase {

  private static final Partitioner PARTITIONER = new AutoBucketPartitioner();
  private static final String PARTITION_FIELD_KEY = "__idx";

  @Override
  List<String> defaultReadConfigOptions() {
    return asList(
        ReadConfig.PARTITIONER_CONFIG,
        PARTITIONER.getClass().getName(),
        ReadConfig.PARTITIONER_OPTIONS_PREFIX + PARTITION_CHUNK_SIZE_MB_CONFIG,
        "1");
  }

  @Test
  void testNonExistentCollection() {
    ReadConfig readConfig = createReadConfig("nonExist");
    List<MongoInputPartition> partitions = PARTITIONER.generatePartitions(readConfig);
    assertIterableEquals(SINGLE_PARTITIONER.generatePartitions(readConfig), partitions);
  }

  @Test
  void testFewerRecordsThanData() {
    ReadConfig readConfig = createReadConfig(
        "few", ReadConfig.PARTITIONER_OPTIONS_PREFIX + PARTITION_CHUNK_SIZE_MB_CONFIG, "4");
    loadSampleData(10, 2, readConfig);

    List<MongoInputPartition> partitions = PARTITIONER.generatePartitions(readConfig);
    assertIterableEquals(SINGLE_PARTITIONER.generatePartitions(readConfig), partitions);

    readConfig = createReadConfig(
        "few", ReadConfig.PARTITIONER_OPTIONS_PREFIX + PARTITION_CHUNK_SIZE_MB_CONFIG, "2");
    partitions = PARTITIONER.generatePartitions(readConfig);
    assertIterableEquals(SINGLE_PARTITIONER.generatePartitions(readConfig), partitions);
  }

  @Test
  void testCreatesExpectedPartitions() {
    ReadConfig readConfig = createReadConfig("expected");
    loadSampleData(51, 5, readConfig);

    List<MongoInputPartition> expectedPartitions = createMongoInputPartitions(
        toBsonDocuments(
            "{\"_id\": {\"min\": \"IGNORED\", \"max\": \"00009\"}}",
            "{\"_id\": {\"min\": \"00009\", \"max\": \"00018\"}}",
            "{\"_id\": {\"min\": \"00018\", \"max\": \"00027\"}}",
            "{\"_id\": {\"min\": \"00027\", \"max\": \"00036\"}}",
            "{\"_id\": {\"min\": \"00036\", \"max\": \"00045\"}}",
            "{\"_id\": {\"min\": \"00045\", \"max\": \"IGNORED\"}}"),
        emptyList(),
        singletonList("_id"),
        PARTITION_FIELD_KEY,
        getPreferredLocations());

    assertIterableEquals(expectedPartitions, PARTITIONER.generatePartitions(readConfig));
    assertEquals(51, getDataSetCount(readConfig));
  }

  @Test
  void testUsingAlternativePartitionField() {
    ReadConfig readConfig = createReadConfig(
        "alt", ReadConfig.PARTITIONER_OPTIONS_PREFIX + PARTITION_FIELD_LIST_CONFIG, "pk");
    loadSampleData(52, 5, readConfig);

    List<MongoInputPartition> expectedPartitions = createMongoInputPartitions(
        toBsonDocuments(
            "{\"_id\": {\"min\": \"IGNORED\", \"max\": \"_10009\"}}",
            "{\"_id\": {\"min\": \"_10009\", \"max\": \"_10018\"}}",
            "{\"_id\": {\"min\": \"_10018\", \"max\": \"_10027\"}}",
            "{\"_id\": {\"min\": \"_10027\", \"max\": \"_10036\"}}",
            "{\"_id\": {\"min\": \"_10036\", \"max\": \"_10045\"}}",
            "{\"_id\": {\"min\": \"_10045\", \"max\": \"IGNORED\"}}"),
        emptyList(),
        singletonList("pk"),
        PARTITION_FIELD_KEY,
        getPreferredLocations());

    assertIterableEquals(expectedPartitions, PARTITIONER.generatePartitions(readConfig));
    assertEquals(52, getDataSetCount(readConfig));
  }

  @Test
  void testUsingPartitionFieldThatContainsDuplicates() {
    ReadConfig readConfig =
        createReadConfig("dups", PARTITIONER_OPTIONS_PREFIX + PARTITION_FIELD_LIST_CONFIG, "dups");
    loadSampleData(101, 5, readConfig);

    List<MongoInputPartition> expectedPartitions = createMongoInputPartitions(
        toBsonDocuments(
            "{\"_id\": {\"min\": \"IGNORED\", \"max\": \"00001\"}}",
            "{\"_id\": {\"min\": \"00001\", \"max\": \"00026\"}}",
            "{\"_id\": {\"min\": \"00026\", \"max\": \"00052\"}}",
            "{\"_id\": {\"min\": \"00052\", \"max\": \"00077\"}}",
            "{\"_id\": {\"min\": \"00077\", \"max\": \"IGNORED\"}}"),
        emptyList(),
        singletonList("dups"),
        PARTITION_FIELD_KEY,
        getPreferredLocations());

    assertIterableEquals(expectedPartitions, PARTITIONER.generatePartitions(readConfig));
    assertEquals(101, getDataSetCount(readConfig));
  }

  @Test
  void testCreatesExpectedPartitionsWithUsersPipeline() {
    ReadConfig readConfig = createReadConfig(
        "pipeline",
        ReadConfig.AGGREGATION_PIPELINE_CONFIG,
        "{'$match': {'_id': {'$gt': '00010', '$lte': '00045'}}}");
    List<BsonDocument> userSuppliedPipeline =
        singletonList(BsonDocument.parse("{'$match': {'_id': {'$gt': '00010', '$lte': '00045'}}}"));

    // No data
    assertIterableEquals(
        SINGLE_PARTITIONER.generatePartitions(readConfig),
        PARTITIONER.generatePartitions(readConfig));

    loadSampleData(55, 5, readConfig);

    List<MongoInputPartition> expectedPartitions = createMongoInputPartitions(
        toBsonDocuments(
            "{\"_id\": {\"min\": \"IGNORED\", \"max\": \"00020\"}}",
            "{\"_id\": {\"min\": \"00020\", \"max\": \"00029\"}}",
            "{\"_id\": {\"min\": \"00029\", \"max\": \"00038\"}}",
            "{\"_id\": {\"min\": \"00038\", \"max\": \"IGNORED\"}}"),
        userSuppliedPipeline,
        singletonList("_id"),
        PARTITION_FIELD_KEY,
        getPreferredLocations());

    assertIterableEquals(expectedPartitions, PARTITIONER.generatePartitions(readConfig));
    assertEquals(35, getDataSetCount(readConfig));
  }

  @Test
  void testUsingCompoundPartitionFieldThatContainsDuplicates() {
    assumeTrue(isAtLeastSevenDotZero());
    ReadConfig readConfig = createReadConfig(
        "compound", PARTITIONER_OPTIONS_PREFIX + PARTITION_FIELD_LIST_CONFIG, "pk,dups");
    loadSampleData(250, 10, readConfig);

    List<MongoInputPartition> expectedPartitions = createMongoInputPartitions(
        toBsonDocuments(
            "{\"_id\": {\"min\": \"IGNORED\", \"max\": {\"0\": \"_10025\", \"1\": \"00025\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"_10025\", \"1\": \"00025\"}, \"max\": {\"0\": \"_10050\", \"1\": \"00050\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"_10050\", \"1\": \"00050\"}, \"max\": {\"0\": \"_10075\", \"1\": \"00000\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"_10075\", \"1\": \"00000\"}, \"max\": {\"0\": \"_10100\", \"1\": \"00100\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"_10100\", \"1\": \"00100\"}, \"max\": {\"0\": \"_10125\", \"1\": \"00125\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"_10125\", \"1\": \"00125\"}, \"max\": {\"0\": \"_10150\", \"1\": \"00000\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"_10150\", \"1\": \"00000\"}, \"max\": {\"0\": \"_10175\", \"1\": \"00175\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"_10175\", \"1\": \"00175\"}, \"max\": {\"0\": \"_10200\", \"1\": \"00200\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"_10200\", \"1\": \"00200\"}, \"max\": {\"0\": \"_10225\", \"1\": \"00000\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"_10225\", \"1\": \"00000\"}, \"max\": \"IGNORED\"}}}"),
        emptyList(),
        asList("pk", "dups"),
        PARTITION_FIELD_KEY,
        getPreferredLocations());

    assertIterableEquals(expectedPartitions, PARTITIONER.generatePartitions(readConfig));
    assertEquals(250, getDataSetCount(readConfig));
  }

  @Test
  void testNestedField() {
    ReadConfig readConfig = createReadConfig(
        "nested", PARTITIONER_OPTIONS_PREFIX + PARTITION_FIELD_LIST_CONFIG, "nested.pk");
    loadComplexSampleData(60, 5, readConfig);

    List<MongoInputPartition> expectedPartitions = createMongoInputPartitions(
        toBsonDocuments(
            "{\"_id\": {\"min\": \"IGNORED\", \"max\": \"_10012\"}}",
            "{\"_id\": {\"min\": \"_10012\", \"max\": \"_10024\"}}",
            "{\"_id\": {\"min\": \"_10024\", \"max\": \"_10036\"}}",
            "{\"_id\": {\"min\": \"_10036\", \"max\": \"_10048\"}}",
            "{\"_id\": {\"min\": \"_10048\", \"max\": \"IGNORED\"}}"),
        emptyList(),
        singletonList("nested.pk"),
        PARTITION_FIELD_KEY,
        getPreferredLocations());

    assertIterableEquals(expectedPartitions, PARTITIONER.generatePartitions(readConfig));
    assertEquals(60, getDataSetCount(readConfig));
  }

  @Test
  void testCompoundWithNestedField() {
    ReadConfig readConfig = createReadConfig(
        "compoundWithNested",
        PARTITIONER_OPTIONS_PREFIX + PARTITION_FIELD_LIST_CONFIG,
        "_id, nested.dups");
    loadComplexSampleData(300, 5, readConfig);

    List<MongoInputPartition> expectedPartitions = createMongoInputPartitions(
        toBsonDocuments(
            "{\"_id\": {\"min\": \"IGNORED\", \"max\": {\"0\": \"00060\", \"1\": \"00000\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"00060\", \"1\": \"00000\"}, \"max\": {\"0\": \"00120\", \"1\": \"00000\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"00120\", \"1\": \"00000\"}, \"max\": {\"0\": \"00180\", \"1\": \"00000\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"00180\", \"1\": \"00000\"}, \"max\": {\"0\": \"00240\", \"1\": \"00000\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"00240\", \"1\": \"00000\"}, \"max\": \"IGNORED\"}}"),
        emptyList(),
        asList("_id", "nested.dups"),
        PARTITION_FIELD_KEY,
        getPreferredLocations());

    assertIterableEquals(expectedPartitions, PARTITIONER.generatePartitions(readConfig));
    assertEquals(300, getDataSetCount(readConfig));
  }

  @Test
  void testWorksWithHashedShardKeys() {
    assumeTrue(isSharded());
    assumeTrue(isAtLeastFourDotFour());

    ReadConfig readConfig = createReadConfig("simpleHashed");
    shardCollection(readConfig.getNamespace(), "{\"_id\": \"hashed\"}");

    loadSampleData(51, 5, readConfig);

    List<MongoInputPartition> expectedPartitions = createMongoInputPartitions(
        toBsonDocuments(
            "{\"_id\": {\"min\": \"IGNORED\", \"max\": \"00009\"}}",
            "{\"_id\": {\"min\": \"00009\", \"max\": \"00018\"}}",
            "{\"_id\": {\"min\": \"00018\", \"max\": \"00027\"}}",
            "{\"_id\": {\"min\": \"00027\", \"max\": \"00036\"}}",
            "{\"_id\": {\"min\": \"00036\", \"max\": \"00045\"}}",
            "{\"_id\": {\"min\": \"00045\", \"max\": \"IGNORED\"}}"),
        emptyList(),
        singletonList("_id"),
        PARTITION_FIELD_KEY,
        getPreferredLocations());

    assertIterableEquals(expectedPartitions, PARTITIONER.generatePartitions(readConfig));
    assertEquals(51, getDataSetCount(readConfig));
  }

  @Test
  void testWorksWithCompoundAndHashedShardKeys() {
    assumeTrue(isSharded());
    assumeTrue(isAtLeastFourDotFour());
    ReadConfig readConfig = createReadConfig(
        "complexHashed",
        PARTITIONER_OPTIONS_PREFIX + PARTITION_FIELD_LIST_CONFIG,
        "_id, nested.dups");

    shardCollection(readConfig.getNamespace(), "{\"_id\": 1, \"nested,dups\": \"hashed\"}");
    loadComplexSampleData(300, 10, readConfig);

    List<MongoInputPartition> expectedPartitions = createMongoInputPartitions(
        toBsonDocuments(
            "{\"_id\": {\"min\": \"IGNORED\", \"max\": {\"0\": \"00030\", \"1\": \"00000\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"00030\", \"1\": \"00000\"}, \"max\": {\"0\": \"00060\", \"1\": \"00000\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"00060\", \"1\": \"00000\"}, \"max\": {\"0\": \"00090\", \"1\": \"00000\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"00090\", \"1\": \"00000\"}, \"max\": {\"0\": \"00120\", \"1\": \"00000\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"00120\", \"1\": \"00000\"}, \"max\": {\"0\": \"00150\", \"1\": \"00000\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"00150\", \"1\": \"00000\"}, \"max\": {\"0\": \"00180\", \"1\": \"00000\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"00180\", \"1\": \"00000\"}, \"max\": {\"0\": \"00210\", \"1\": \"00000\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"00210\", \"1\": \"00000\"}, \"max\": {\"0\": \"00240\", \"1\": \"00000\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"00240\", \"1\": \"00000\"}, \"max\": {\"0\": \"00270\", \"1\": \"00000\"}}}",
            "{\"_id\": {\"min\": {\"0\": \"00270\", \"1\": \"00000\"}, \"max\": \"IGNORED\"}}"),
        emptyList(),
        asList("_id", "nested.dups"),
        PARTITION_FIELD_KEY,
        getPreferredLocations());

    assertIterableEquals(expectedPartitions, PARTITIONER.generatePartitions(readConfig));
    assertEquals(300, getDataSetCount(readConfig));
  }

  @Test
  void shouldValidateReadConfigs() {
    loadSampleData(50, 2, createReadConfig("validate"));

    assertAll(
        () -> assertThrows(
            ConfigException.class,
            () -> PARTITIONER.generatePartitions(createReadConfig(
                "validate", PARTITIONER_OPTIONS_PREFIX + SAMPLES_PER_PARTITION_CONFIG, "-1")),
            SAMPLES_PER_PARTITION_CONFIG + " is negative"),
        () -> assertThrows(
            ConfigException.class,
            () -> PARTITIONER.generatePartitions(createReadConfig(
                "validate", PARTITIONER_OPTIONS_PREFIX + SAMPLES_PER_PARTITION_CONFIG, "0")),
            SAMPLES_PER_PARTITION_CONFIG + " is zero"),
        () -> assertThrows(
            ConfigException.class,
            () -> PARTITIONER.generatePartitions(createReadConfig(
                "validate", PARTITIONER_OPTIONS_PREFIX + SAMPLES_PER_PARTITION_CONFIG, "1")),
            SAMPLES_PER_PARTITION_CONFIG + " is one"),
        () -> assertThrows(
            ConfigException.class,
            () -> PARTITIONER.generatePartitions(createReadConfig(
                "validate", PARTITIONER_OPTIONS_PREFIX + PARTITION_CHUNK_SIZE_MB_CONFIG, "-1")),
            PARTITION_CHUNK_SIZE_MB_CONFIG + " is negative"),
        () -> assertThrows(
            ConfigException.class,
            () -> PARTITIONER.generatePartitions(createReadConfig(
                "validate", PARTITIONER_OPTIONS_PREFIX + PARTITION_CHUNK_SIZE_MB_CONFIG, "0")),
            PARTITION_CHUNK_SIZE_MB_CONFIG + " is zero"));
  }

  @Test
  void shouldLogCommentsInProfilerLogs() {
    ReadConfig readConfig = createReadConfig("commentInLogs");
    loadSampleData(50, 1, readConfig);

    ReadConfig configWithComment = readConfig.withOption(COMMENT_CONFIG, TEST_COMMENT);
    assertCommentsInProfile(
        () -> PARTITIONER.generatePartitions(configWithComment), configWithComment);
  }

  private long getDataSetCount(final ReadConfig readConfig) {
    return getOrCreateSparkSession()
        .read()
        .format("mongodb")
        .options(readConfig.getOptions())
        .load()
        .count();
  }

  private static List<BsonDocument> toBsonDocuments(final String... json) {
    return Arrays.stream(json).map(BsonDocument::parse).collect(Collectors.toList());
  }
}
