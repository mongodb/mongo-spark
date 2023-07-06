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
import static com.mongodb.spark.sql.connector.read.partitioner.PartitionerHelper.SINGLE_PARTITIONER;
import static com.mongodb.spark.sql.connector.read.partitioner.PartitionerHelper.createPartitionPipeline;
import static com.mongodb.spark.sql.connector.read.partitioner.SamplePartitioner.PARTITION_SIZE_MB_CONFIG;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
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

public class PaginateBySizePartitionerTest extends PartitionerTestCase {
  private static final Partitioner PARTITIONER = new PaginateBySizePartitioner();

  @Override
  List<String> defaultReadConfigOptions() {
    return asList(
        ReadConfig.PARTITIONER_OPTIONS_PREFIX + PaginateBySizePartitioner.PARTITION_SIZE_MB_CONFIG,
        "1");
  }

  @Test
  void testNonExistentCollection() {
    ReadConfig readConfig = createReadConfig("noColl");
    List<MongoInputPartition> partitions = PARTITIONER.generatePartitions(readConfig);
    assertIterableEquals(SINGLE_PARTITIONER.generatePartitions(readConfig), partitions);
  }

  @Test
  void testSingleResultData() {
    ReadConfig readConfig = createReadConfig("singleResult");
    loadSampleData(1, 1, readConfig);

    assertIterableEquals(
        SINGLE_PARTITIONER.generatePartitions(readConfig),
        PARTITIONER.generatePartitions(readConfig));

    // Drop and add more data but include a user pipeline that limits the results
    readConfig.doWithCollection(MongoCollection::drop);
    loadSampleData(10, 1, readConfig);

    readConfig = readConfig.withOption(
        ReadConfig.PREFIX + ReadConfig.AGGREGATION_PIPELINE_CONFIG,
        "{'$match': {'_id': {'$gte': '00010'}}}");
    assertIterableEquals(
        SINGLE_PARTITIONER.generatePartitions(readConfig),
        PARTITIONER.generatePartitions(readConfig));
  }

  @Test
  void testCreatesExpectedPartitions() {
    ReadConfig readConfig = createReadConfig("expectedPart");
    loadSampleData(50, 5, readConfig);

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
    ReadConfig readConfig = createReadConfig(
        "altPartitionFiled", PARTITIONER_OPTIONS_PREFIX + PARTITION_FIELD_CONFIG, "pk");
    loadSampleData(51, 5, readConfig);

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
            createPartitionPipeline(
                BsonDocument.parse("{pk: {$gte: '_10040', $lt: '_10050'}}"), emptyList()),
            getPreferredLocations()),
        new MongoInputPartition(
            5,
            createPartitionPipeline(BsonDocument.parse("{pk: {$gte: '_10050'}}"), emptyList()),
            getPreferredLocations()));

    assertPartitioner(PARTITIONER, expectedPartitions, readConfig);
  }

  @Test
  void testUsingPartitionFieldThatContainsDuplicates() {
    ReadConfig readConfig =
        createReadConfig("dups", PARTITIONER_OPTIONS_PREFIX + PARTITION_FIELD_CONFIG, "dups");
    loadSampleData(101, 5, readConfig);

    assertThrows(ConfigException.class, () -> PARTITIONER.generatePartitions(readConfig));
  }

  @Test
  void testCreatesExpectedPartitionsWithUsersPipeline() {
    ReadConfig readConfig = createReadConfig(
        "pipeline",
        ReadConfig.AGGREGATION_PIPELINE_CONFIG,
        "{'$match': {'_id': {'$gte': '00010', '$lt': '00060'}}}");
    List<BsonDocument> userSuppliedPipeline = singletonList(
        BsonDocument.parse("{'$match': {'_id': {'$gte': '00010', " + "'$lt': '00060'}}}"));

    // No data
    assertIterableEquals(
        SINGLE_PARTITIONER.generatePartitions(readConfig),
        PARTITIONER.generatePartitions(readConfig));

    loadSampleData(60, 6, readConfig);
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
    loadSampleData(55, 5, readConfig);

    List<MongoInputPartition> expectedPartitions = asList(
        new MongoInputPartition(
            0,
            createPartitionPipeline(BsonDocument.parse("{_id: {$lt: '00011'}}"), emptyList()),
            getPreferredLocations()),
        new MongoInputPartition(
            1,
            createPartitionPipeline(
                BsonDocument.parse("{_id: {$gte: '00011', $lt: '00022'}}"), emptyList()),
            getPreferredLocations()),
        new MongoInputPartition(
            2,
            createPartitionPipeline(
                BsonDocument.parse("{_id: {$gte: '00022', $lt: '00033'}}"), emptyList()),
            getPreferredLocations()),
        new MongoInputPartition(
            3,
            createPartitionPipeline(
                BsonDocument.parse("{_id: {$gte: '00033', $lt: '00044'}}"), emptyList()),
            getPreferredLocations()),
        new MongoInputPartition(
            4,
            createPartitionPipeline(BsonDocument.parse("{_id: {$gte: '00044'}}"), emptyList()),
            getPreferredLocations()));

    assertPartitioner(PARTITIONER, expectedPartitions, readConfig);
  }

  @Test
  void shouldValidateReadConfigs() {
    loadSampleData(50, 2, createReadConfig("validate"));
    assertAll(
        () -> assertThrows(
            ConfigException.class,
            () -> PARTITIONER.generatePartitions(createReadConfig(
                "validate", PARTITIONER_OPTIONS_PREFIX + PARTITION_SIZE_MB_CONFIG, "-1")),
            PARTITION_SIZE_MB_CONFIG + " is negative"),
        () -> assertThrows(
            ConfigException.class,
            () -> PARTITIONER.generatePartitions(createReadConfig(
                "validate", PARTITIONER_OPTIONS_PREFIX + PARTITION_SIZE_MB_CONFIG, "0")),
            PARTITION_SIZE_MB_CONFIG + " is zero"));
  }

  @Test
  void shouldLogCommentsInProfilerLogs() {
    ReadConfig readConfig = createReadConfig("commentInLogs");
    loadSampleData(50, 5, readConfig);

    ReadConfig configWithComment = readConfig.withOption(COMMENT_CONFIG, TEST_COMMENT);
    assertCommentsInProfile(
        () -> PARTITIONER.generatePartitions(configWithComment), configWithComment);
  }
}
