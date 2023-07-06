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
import static com.mongodb.spark.sql.connector.read.partitioner.FieldPartitioner.PARTITION_FIELD_CONFIG;
import static com.mongodb.spark.sql.connector.read.partitioner.PaginateIntoPartitionsPartitioner.MAX_NUMBER_OF_PARTITIONS_CONFIG;
import static com.mongodb.spark.sql.connector.read.partitioner.PartitionerHelper.SINGLE_PARTITIONER;
import static com.mongodb.spark.sql.connector.read.partitioner.PartitionerHelper.createPartitionPipeline;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.mongodb.client.MongoCollection;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.ConfigException;
import com.mongodb.spark.sql.connector.read.MongoInputPartition;
import java.util.List;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

public class PaginateIntoPartitionsPartitionerTest extends PartitionerTestCase {

  private static final Partitioner PARTITIONER = new PaginateIntoPartitionsPartitioner();

  @Override
  List<String> defaultReadConfigOptions() {
    return asList(PARTITIONER_OPTIONS_PREFIX + MAX_NUMBER_OF_PARTITIONS_CONFIG, "5");
  }

  @Test
  void testNonExistentCollection() {
    ReadConfig readConfig = createReadConfig("noExist");
    List<MongoInputPartition> partitions = PARTITIONER.generatePartitions(readConfig);
    assertIterableEquals(SINGLE_PARTITIONER.generatePartitions(readConfig), partitions);
  }

  @Test
  void testSingleResultData() {
    ReadConfig readConfig = createReadConfig(
        "single",
        PARTITIONER_OPTIONS_PREFIX
            + PaginateIntoPartitionsPartitioner.MAX_NUMBER_OF_PARTITIONS_CONFIG,
        "1");
    loadSampleData(1, 1, readConfig);

    assertIterableEquals(
        SINGLE_PARTITIONER.generatePartitions(readConfig),
        PARTITIONER.generatePartitions(readConfig));

    // Drop and add more data but include a user pipeline that limits the results
    readConfig = createReadConfig(
        "single",
        PARTITIONER_OPTIONS_PREFIX
            + PaginateIntoPartitionsPartitioner.MAX_NUMBER_OF_PARTITIONS_CONFIG,
        "1",
        ReadConfig.AGGREGATION_PIPELINE_CONFIG,
        "{'$match': {'_id': {'$gte': '00010'}}}");
    readConfig.doWithCollection(MongoCollection::drop);
    loadSampleData(10, 1, readConfig);

    assertIterableEquals(
        SINGLE_PARTITIONER.generatePartitions(readConfig),
        PARTITIONER.generatePartitions(readConfig));
  }

  @Test
  void testCreatesExpectedPartitions() {
    ReadConfig readConfig = createReadConfig("expected");
    loadSampleData(50, 1, readConfig);

    List<MongoInputPartition> expectedPartitions = asList(
        new MongoInputPartition(
            0,
            createPartitionPipeline(BsonDocument.parse("{_id: {$lt: '00010'}}"), emptyList()),
            getPreferredLocations()),
        new MongoInputPartition(
            1,
            createPartitionPipeline(
                BsonDocument.parse("{_id: {$gte: '00010', $lt: '00020'}}"), emptyList()),
            getPreferredLocations()),
        new MongoInputPartition(
            2,
            createPartitionPipeline(
                BsonDocument.parse("{_id: {$gte: '00020', $lt: '00030'}}"), emptyList()),
            getPreferredLocations()),
        new MongoInputPartition(
            3,
            createPartitionPipeline(
                BsonDocument.parse("{_id: {$gte: '00030', $lt: '00040'}}"), emptyList()),
            getPreferredLocations()),
        new MongoInputPartition(
            4,
            createPartitionPipeline(BsonDocument.parse("{_id: {$gte: '00040'}}"), emptyList()),
            getPreferredLocations()));

    assertPartitioner(PARTITIONER, expectedPartitions, readConfig);
  }

  @Test
  void testUsingAlternativePartitionField() {
    ReadConfig readConfig =
        createReadConfig("alt", PARTITIONER_OPTIONS_PREFIX + PARTITION_FIELD_CONFIG, "pk");
    loadSampleData(50, 1, readConfig);

    List<MongoInputPartition> expectedPartitions = asList(
        new MongoInputPartition(
            0,
            createPartitionPipeline(BsonDocument.parse("{pk: {$lt: '_10010'}}"), emptyList()),
            getPreferredLocations()),
        new MongoInputPartition(
            1,
            createPartitionPipeline(
                BsonDocument.parse("{pk: {$gte: '_10010', $lt: '_10020'}}"), emptyList()),
            getPreferredLocations()),
        new MongoInputPartition(
            2,
            createPartitionPipeline(
                BsonDocument.parse("{pk: {$gte: '_10020', $lt: '_10030'}}"), emptyList()),
            getPreferredLocations()),
        new MongoInputPartition(
            3,
            createPartitionPipeline(
                BsonDocument.parse("{pk: {$gte: '_10030', $lt: '_10040'}}"), emptyList()),
            getPreferredLocations()),
        new MongoInputPartition(
            4,
            createPartitionPipeline(BsonDocument.parse("{pk: {$gte: '_10040'}}"), emptyList()),
            getPreferredLocations()));

    assertPartitioner(PARTITIONER, expectedPartitions, readConfig);
  }

  @Test
  void testUsingPartitionFieldThatContainsDuplicates() {
    ReadConfig readConfig =
        createReadConfig("dups", PARTITIONER_OPTIONS_PREFIX + PARTITION_FIELD_CONFIG, "dups");
    loadSampleData(101, 1, readConfig);

    assertThrows(ConfigException.class, () -> PARTITIONER.generatePartitions(readConfig));
  }

  @Test
  void testCreatesExpectedPartitionsWithUsersPipeline() {
    ReadConfig readConfig = createReadConfig(
        "withPipeline",
        ReadConfig.AGGREGATION_PIPELINE_CONFIG,
        "{'$match': {'_id': {'$gte': '00010', '$lt': '00060'}}}");
    List<BsonDocument> userSuppliedPipeline = readConfig.getAggregationPipeline();

    // No data
    assertIterableEquals(
        SINGLE_PARTITIONER.generatePartitions(readConfig),
        PARTITIONER.generatePartitions(readConfig));

    loadSampleData(60, 1, readConfig);
    List<MongoInputPartition> expectedPartitions = asList(
        new MongoInputPartition(
            0,
            createPartitionPipeline(
                BsonDocument.parse("{_id: {$lt: '00020'}}"), userSuppliedPipeline),
            getPreferredLocations()),
        new MongoInputPartition(
            1,
            createPartitionPipeline(
                BsonDocument.parse("{_id: {$gte: '00020', $lt: '00030'}}"), userSuppliedPipeline),
            getPreferredLocations()),
        new MongoInputPartition(
            2,
            createPartitionPipeline(
                BsonDocument.parse("{_id: {$gte: '00030', $lt: '00040'}}"), userSuppliedPipeline),
            getPreferredLocations()),
        new MongoInputPartition(
            3,
            createPartitionPipeline(
                BsonDocument.parse("{_id: {$gte: '00040', $lt: '00050'}}"), userSuppliedPipeline),
            getPreferredLocations()),
        new MongoInputPartition(
            4,
            createPartitionPipeline(
                BsonDocument.parse("{_id: {$gte: '00050'}}"), userSuppliedPipeline),
            getPreferredLocations()));

    assertPartitioner(PARTITIONER, expectedPartitions, readConfig);
  }

  @Test
  void testCreatesUnevenPartitions() {
    ReadConfig readConfig = createReadConfig("uneven");
    loadSampleData(57, 1, readConfig);

    List<MongoInputPartition> expectedPartitions = asList(
        new MongoInputPartition(
            0,
            createPartitionPipeline(BsonDocument.parse("{_id: {$lt: '00012'}}"), emptyList()),
            getPreferredLocations()),
        new MongoInputPartition(
            1,
            createPartitionPipeline(
                BsonDocument.parse("{_id: {$gte: '00012', $lt: '00024'}}"), emptyList()),
            getPreferredLocations()),
        new MongoInputPartition(
            2,
            createPartitionPipeline(
                BsonDocument.parse("{_id: {$gte: '00024', $lt: '00036'}}"), emptyList()),
            getPreferredLocations()),
        new MongoInputPartition(
            3,
            createPartitionPipeline(
                BsonDocument.parse("{_id: {$gte: '00036', $lt: '00048'}}"), emptyList()),
            getPreferredLocations()),
        new MongoInputPartition(
            4,
            createPartitionPipeline(BsonDocument.parse("{_id: {$gte: '00048'}}"), emptyList()),
            getPreferredLocations()));

    assertPartitioner(PARTITIONER, expectedPartitions, readConfig);
  }

  @Test
  void shouldValidateReadConfigs() {
    loadSampleData(10, 1, createReadConfig("validate"));

    assertAll(
        () -> assertThrows(
            ConfigException.class,
            () -> PARTITIONER.generatePartitions(createReadConfig(
                "validate", PARTITIONER_OPTIONS_PREFIX + MAX_NUMBER_OF_PARTITIONS_CONFIG, "-1")),
            MAX_NUMBER_OF_PARTITIONS_CONFIG + " is negative"),
        () -> assertThrows(
            ConfigException.class,
            () -> PARTITIONER.generatePartitions(createReadConfig(
                "validate", PARTITIONER_OPTIONS_PREFIX + MAX_NUMBER_OF_PARTITIONS_CONFIG, "0")),
            MAX_NUMBER_OF_PARTITIONS_CONFIG + " is zero"));
  }

  @Test
  void shouldLogCommentsInProfilerLogs() {
    ReadConfig readConfig = createReadConfig("commentInLogs");
    loadSampleData(50, 1, readConfig);

    ReadConfig configWithComment = readConfig.withOption(COMMENT_CONFIG, TEST_COMMENT);
    assertCommentsInProfile(
        () -> PARTITIONER.generatePartitions(configWithComment), configWithComment);
  }
}
