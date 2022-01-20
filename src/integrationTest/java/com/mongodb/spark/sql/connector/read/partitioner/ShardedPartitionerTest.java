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
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.bson.BsonDocument;

import com.mongodb.client.MongoCollection;

import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.read.MongoInputPartition;

public class ShardedPartitionerTest extends PartitionerTestCase {
  private static final ShardedPartitioner PARTITIONER = new ShardedPartitioner();

  @Override
  List<String> defaultReadConfigOptions() {
    return asList(ReadConfig.PREFIX + ReadConfig.COLLECTION_NAME_CONFIG, "sharded.collection");
  }

  @Test
  void testNonShardedCollection() {
    assumeTrue(isSharded());
    ReadConfig readConfig = createReadConfig();
    readConfig.withCollection(coll -> coll.insertOne(new BsonDocument()));
    List<MongoInputPartition> partitions = PARTITIONER.generatePartitions(readConfig);
    assertIterableEquals(SINGLE_PARTITIONER.generatePartitions(readConfig), partitions);
  }

  @Test
  void testPartitionsTheCollectionAsExpected() {
    assumeTrue(isSharded());
    ReadConfig readConfig = createReadConfig();
    shardCollection(readConfig.getNamespace(), "{_id: 1}");
    loadSampleData(100, 10, readConfig);

    assertPartitioner(readConfig);
  }

  @Test
  void testPartitionsTheCollectionAsExpectedWithShardKeys() {
    assumeTrue(isSharded());

    ReadConfig readConfig =
        createReadConfig(
            ReadConfig.PARTITIONER_OPTIONS_PREFIX + ShardedPartitioner.SHARD_KEY_FIELD_LIST_CONFIG,
            "_id, pk");
    shardCollection(readConfig.getNamespace(), "{_id: 1, pk: 1}");
    loadSampleData(100, 10, readConfig);

    assertPartitioner(readConfig);
  }

  @Test
  void testCreatesExpectedPartitionsWithUsersPipeline() {
    assumeTrue(isSharded());
    ReadConfig readConfig =
        createReadConfig(
            ReadConfig.AGGREGATION_PIPELINE_CONFIG,
            "{'$match': {'_id': {'$gte': '00010', " + "'$lte': '00040'}}}");

    shardCollection(readConfig.getNamespace(), "{_id: 1}");
    loadSampleData(100, 10, readConfig);

    assertPartitioner(readConfig);
  }

  @Test
  void canSupportEmptyOrDeletedCollections() {
    assumeTrue(isSharded());

    // No collection
    assertIterableEquals(
        SINGLE_PARTITIONER.generatePartitions(createReadConfig()),
        PARTITIONER.generatePartitions(createReadConfig()));

    // No chunk data
    shardCollection(createReadConfig().getNamespace(), "{_id: 1}");
    assertIterableEquals(
        SINGLE_PARTITIONER.generatePartitions(createReadConfig()),
        PARTITIONER.generatePartitions(createReadConfig()));

    // Dropped collection
    createReadConfig().doWithCollection(MongoCollection::drop);
    assertIterableEquals(
        SINGLE_PARTITIONER.generatePartitions(createReadConfig()),
        PARTITIONER.generatePartitions(createReadConfig()));
  }

  @Test
  void shouldParseTheHostsListCorrectly() {
    // Single host
    assertIterableEquals(
        singletonList("sh0.example.com"), PARTITIONER.getHosts("tic/sh0.example.com:27018"));
    // Multiple hosts
    assertIterableEquals(
        asList("sh0.rs1.example.com", "sh0.rs2.example.com", "sh0.rs3.example.com"),
        PARTITIONER.getHosts(
            "tic/sh0.rs1.example.com:27018,sh0.rs2.example.com:27018,sh0.rs3.example.com:27018"));
  }

  private void assertPartitioner(final ReadConfig readConfig) {
    List<MongoInputPartition> mongoInputPartitions = PARTITIONER.generatePartitions(readConfig);
    assertPartitionsAreOrderedAndBounded(
        mongoInputPartitions, PARTITIONER.getShardKeys(readConfig));

    List<BsonDocument> aggregationPipeline = readConfig.getAggregationPipeline();
    if (!aggregationPipeline.isEmpty()) {
      mongoInputPartitions.forEach(
          mongoInputPartition ->
              assertTrue(mongoInputPartition.getPipeline().containsAll(aggregationPipeline)));
    }

    assertPartitionerCoversAllData(PARTITIONER, readConfig);
  }

  /**
   * Different versions of MongoDB will shard chunks slightly differently.
   *
   * <p>This prevents explicitly comparing the shard chunk partition ranges against expected ranges.
   * Instead, the MongoInputPartition ranges are checked to ensure they are bounded by continuous
   * `$lt` and `$gte` ranges.
   */
  private void assertPartitionsAreOrderedAndBounded(
      final List<MongoInputPartition> mongoInputPartitions, final List<String> shardKeys) {
    assertFalse(mongoInputPartitions.isEmpty(), "No partitions were produced");
    assertMongoPartitionHasBounds(getMatchStage(mongoInputPartitions.get(0)), shardKeys, "$lt");

    for (int i = 1; i < mongoInputPartitions.size() - 1; i++) {
      BsonDocument ltMatch = getMatchStage(mongoInputPartitions.get(i));
      BsonDocument gteMatch = getMatchStage(mongoInputPartitions.get(i + 1));
      assertMongoPartitionBounds(ltMatch, gteMatch, shardKeys);
    }

    if (mongoInputPartitions.size() > 1) {
      assertMongoPartitionHasBounds(
          getMatchStage(mongoInputPartitions.get(mongoInputPartitions.size() - 1)),
          shardKeys,
          "$gte");
    }
  }

  private void assertPartitionsAreOrderedAndBounded(final ReadConfig readConfig) {
    assertPartitionsAreOrderedAndBounded(
        PARTITIONER.generatePartitions(readConfig), PARTITIONER.getShardKeys(readConfig));
  }

  private BsonDocument getMatchStage(final MongoInputPartition mongoInputPartition) {
    return mongoInputPartition.getPipeline().isEmpty()
        ? new BsonDocument()
        : mongoInputPartition.getPipeline().get(0).getDocument("$match", new BsonDocument());
  }

  private void assertMongoPartitionHasBounds(
      final BsonDocument matchStage,
      final List<String> shardKeys,
      final String queryComparisonOperator) {
    shardKeys.forEach(
        shardKey ->
            assumeTrue(
                matchStage
                    .getDocument(shardKey, new BsonDocument())
                    .containsKey(queryComparisonOperator),
                format(
                    "Missing query comparison operator for %s in %s",
                    shardKey, matchStage.toJson())));
  }

  private void assertMongoPartitionBounds(
      final BsonDocument ltMatch, final BsonDocument gteMatch, final List<String> shardKeys) {
    shardKeys.forEach(
        shardKey -> {
          BsonDocument ltMatchForShardKey = ltMatch.getDocument(shardKey, new BsonDocument());
          BsonDocument gteMatchForShardKey = gteMatch.getDocument(shardKey, new BsonDocument());

          assertTrue(
              ltMatchForShardKey.containsKey("$lt"),
              format("Missing $lt match for shardKey '%s': %s", shardKey, ltMatch));
          assertTrue(
              gteMatchForShardKey.containsKey("$gte"),
              format("Missing $gte match for shardKey '%s': %s", shardKey, gteMatch));
          assertEquals(
              ltMatchForShardKey.get("$lt"),
              gteMatchForShardKey.get("$gte"),
              format(
                  "Match queries are not bounded correctly: "
                      + "%s does not have the upper bound of %s",
                  ltMatch, gteMatch));
        });
  }
}
