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

import static com.mongodb.spark.sql.connector.read.partitioner.PartitionerHelper.SINGLE_PARTITIONER;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import com.mongodb.client.MongoCollection;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.read.MongoInputPartition;
import java.util.List;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

public class SinglePartitionerTest extends PartitionerTestCase {

  private final List<MongoInputPartition> singlePartitionsList;

  public SinglePartitionerTest() {
    super();
    this.singlePartitionsList =
        singletonList(new MongoInputPartition(0, emptyList(), getPreferredLocations()));
  }

  @Test
  void testNonExistentCollection() {
    ReadConfig readConfig = createReadConfig("nonExist");
    readConfig.doWithCollection(MongoCollection::drop);

    assertPartitioner(SINGLE_PARTITIONER, singlePartitionsList, readConfig);
  }

  @Test
  void testCreatesExpectedPartitions() {
    ReadConfig readConfig = createReadConfig("expected");
    loadSampleData(51, 5, readConfig);

    assertPartitioner(SINGLE_PARTITIONER, singlePartitionsList, readConfig);
  }

  @Test
  void testUsingAlternativePartitionFieldList() {
    ReadConfig readConfig = createReadConfig(
        "alt",
        ReadConfig.PARTITIONER_OPTIONS_PREFIX + SamplePartitioner.PARTITION_FIELD_CONFIG,
        "pk");
    assertPartitioner(SINGLE_PARTITIONER, singlePartitionsList, readConfig);
  }

  @Test
  void testCreatesExpectedPartitionsWithUsersPipeline() {
    String matchStage = "{'$match': {'_id': {'$gte': '00010', '$lte': '00040'}}}";
    ReadConfig readConfig = createReadConfig(
        "pipeline", ReadConfig.AGGREGATION_PIPELINE_CONFIG, "[" + matchStage + "]");
    List<BsonDocument> userSuppliedPipeline = singletonList(BsonDocument.parse(matchStage));
    List<MongoInputPartition> expectedPartitions =
        singletonList(new MongoInputPartition(0, userSuppliedPipeline, getPreferredLocations()));

    // No data
    assertPartitioner(SINGLE_PARTITIONER, expectedPartitions, readConfig);

    loadSampleData(50, 1, readConfig);
    assertPartitioner(SINGLE_PARTITIONER, expectedPartitions, readConfig);
  }
}
