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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.bson.BsonDocument;

import com.mongodb.client.MongoCollection;

import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.read.MongoInputPartition;

public class SinglePartitionerTest extends PartitionerTestCase {

  private static final Partitioner PARTITIONER = new SinglePartitionPartitioner();

  private final List<MongoInputPartition> singlePartitionsList;

  public SinglePartitionerTest() {
    super();
    this.singlePartitionsList =
        singletonList(new MongoInputPartition(0, emptyList(), getPreferredLocations()));
  }

  @Test
  void testNonExistentCollection() {
    ReadConfig readConfig = createReadConfig();
    readConfig.doWithCollection(MongoCollection::drop);

    assertIterableEquals(singlePartitionsList, PARTITIONER.generatePartitions(readConfig));
  }

  @Test
  void testCreatesExpectedPartitions() {
    ReadConfig readConfig = createReadConfig();
    loadSampleData(51, 5, readConfig);

    assertIterableEquals(singlePartitionsList, PARTITIONER.generatePartitions(readConfig));
  }

  @Test
  void testUsingAlternativePartitionFieldList() {
    ReadConfig readConfig =
        createReadConfig(
            ReadConfig.PARTITIONER_OPTIONS_PREFIX + SamplePartitioner.PARTITION_FIELD_LIST_CONFIG,
            "pk");
    assertIterableEquals(singlePartitionsList, PARTITIONER.generatePartitions(readConfig));
  }

  @Test
  void testCreatesExpectedPartitionsWithUsersPipeline() {
    ReadConfig readConfig =
        createReadConfig(
            ReadConfig.AGGREGATION_PIPELINE_CONFIG,
            "[{'$match': {'_id': {'$gte': '00010', '$lte': '00040'}}}]");
    List<BsonDocument> userSuppliedPipeline =
        singletonList(
            BsonDocument.parse("{'$match': {'_id': {'$gte': '00010', " + "'$lte': '00040'}}}"));
    List<MongoInputPartition> expectedPartitions =
        singletonList(new MongoInputPartition(0, userSuppliedPipeline, getPreferredLocations()));

    // No data
    assertIterableEquals(
        expectedPartitions, PartitionerHelper.SINGLE_PARTITIONER.generatePartitions(readConfig));

    loadSampleData(50, 1, readConfig);
    assertIterableEquals(
        expectedPartitions, PartitionerHelper.SINGLE_PARTITIONER.generatePartitions(readConfig));
  }
}
