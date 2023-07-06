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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.mongodb.client.MongoCollection;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;
import com.mongodb.spark.sql.connector.read.MongoInputPartition;
import java.util.List;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

public class ShardedPartitionerTest extends PartitionerTestCase {
  private static final ShardedPartitioner PARTITIONER = new ShardedPartitioner();

  @Test
  void testNonShardedCollection() {
    assumeTrue(isSharded());
    ReadConfig readConfig = createReadConfig("withNonShardedCollection");
    readConfig.withCollection(coll -> coll.insertOne(new BsonDocument()));
    List<MongoInputPartition> partitions = PARTITIONER.generatePartitions(readConfig);
    assertIterableEquals(SINGLE_PARTITIONER.generatePartitions(readConfig), partitions);
  }

  @Test
  void testPartitionsTheCollectionAsExpected() {
    assumeTrue(isSharded());
    ReadConfig readConfig = createReadConfig("partitionsAsExpected");
    shardCollection(readConfig.getNamespace(), "{_id: 1}");
    loadSampleData(100, 10, readConfig);

    assertPartitioner(readConfig);
  }

  @Test
  void testThrowsExceptionWithCompoundShardKeys() {
    assumeTrue(isSharded());

    ReadConfig readConfig = createReadConfig("partitionsWithMultipleShardKeys");
    shardCollection(readConfig.getNamespace(), "{_id: 1, dups: 1}");
    loadSampleData(1000, 10, readConfig);

    assertThrows(MongoSparkException.class, () -> PARTITIONER.generatePartitions(readConfig));
  }

  @Test
  void testThrowsExceptionWithHashedShardKeys() {
    assumeTrue(isSharded());
    assumeTrue(isAtLeastFourDotFour());

    ReadConfig readConfig = createReadConfig("partitionsWithMultipleShardKeys");
    shardCollection(readConfig.getNamespace(), "{_id: 'hashed'}");
    loadSampleData(100, 10, readConfig);

    assertThrows(MongoSparkException.class, () -> PARTITIONER.generatePartitions(readConfig));
  }

  @Test
  void testCreatesExpectedPartitionsWithUsersPipeline() {
    assumeTrue(isSharded());
    ReadConfig readConfig = createReadConfig("partitionsWithUsersPipeline")
        .withOption(
            ReadConfig.AGGREGATION_PIPELINE_CONFIG,
            "{'$match': {'_id': {'$gte': '00010', '$lte': '00040'}}}");

    shardCollection(readConfig.getNamespace(), "{_id: 1}");
    loadSampleData(100, 10, readConfig);

    assertPartitioner(readConfig);
  }

  @Test
  void canSupportEmptyOrDeletedCollections() {
    assumeTrue(isSharded());
    ReadConfig readConfig = createReadConfig("empty");
    // No collection
    assertIterableEquals(
        SINGLE_PARTITIONER.generatePartitions(readConfig),
        PARTITIONER.generatePartitions(readConfig));

    // No chunk data
    shardCollection(readConfig.getNamespace(), "{_id: 1}");
    assertIterableEquals(
        SINGLE_PARTITIONER.generatePartitions(readConfig),
        PARTITIONER.generatePartitions(readConfig));

    // Dropped collection
    readConfig.doWithCollection(MongoCollection::drop);
    assertIterableEquals(
        SINGLE_PARTITIONER.generatePartitions(readConfig),
        PARTITIONER.generatePartitions(readConfig));
  }

  @Test
  void shouldParseTheHostsListCorrectly() {
    // Single host - with and without replicaset name
    List<String> expected = singletonList("sh0.example.com");
    assertIterableEquals(expected, PARTITIONER.getHosts("tic/sh0.example.com:27018"));
    assertIterableEquals(expected, PARTITIONER.getHosts("sh0.example.com:27018"));

    // Multiple hosts - with and without replicaset name
    expected = asList("sh0.rs1.example.com", "sh0.rs2.example.com", "sh0.rs3.example.com");
    assertIterableEquals(
        expected,
        PARTITIONER.getHosts(
            "tic/sh0.rs1.example.com:27018,sh0.rs2.example.com:27018,sh0.rs3.example.com:27018"));
    assertIterableEquals(
        expected,
        PARTITIONER.getHosts(
            "sh0.rs1.example.com:27018,sh0.rs2.example.com:27018,sh0.rs3.example.com:27018"));
  }

  private void assertPartitioner(final ReadConfig readConfig) {

    List<MongoInputPartition> mongoInputPartitions = PARTITIONER.generatePartitions(readConfig);
    assertPartitionsAreOrderedAndBounded(mongoInputPartitions);

    List<BsonDocument> aggregationPipeline = readConfig.getAggregationPipeline();
    if (!aggregationPipeline.isEmpty()) {
      mongoInputPartitions.forEach(mongoInputPartition -> assertTrue(
          mongoInputPartition.getPipeline().containsAll(aggregationPipeline),
          () -> format(
              "Pipeline missing aggregation pipeline: %s",
              mongoInputPartition.getPipeline().stream()
                  .map(BsonDocument::toJson)
                  .collect(Collectors.joining(",", "[", "]")))));
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
      final List<MongoInputPartition> mongoInputPartitions) {
    assertFalse(mongoInputPartitions.isEmpty(), "No partitions were produced");
    if (mongoInputPartitions.size() > 1) {
      assertMongoPartitionHasBounds(getMatchStage(mongoInputPartitions.get(0)), "$lt");

      for (int i = 1; i < mongoInputPartitions.size() - 1; i++) {
        BsonDocument ltMatch = getMatchStage(mongoInputPartitions.get(i));
        BsonDocument gteMatch = getMatchStage(mongoInputPartitions.get(i + 1));
        assertMongoPartitionBounds(ltMatch, gteMatch);
      }

      assertMongoPartitionHasBounds(
          getMatchStage(mongoInputPartitions.get(mongoInputPartitions.size() - 1)), "$gte");
    }
  }

  private BsonDocument getMatchStage(final MongoInputPartition mongoInputPartition) {
    return mongoInputPartition.getPipeline().isEmpty()
        ? new BsonDocument()
        : mongoInputPartition.getPipeline().get(0).getDocument("$match", new BsonDocument());
  }

  private void assertMongoPartitionHasBounds(
      final BsonDocument matchStage, final String queryComparisonOperator) {
    matchStage
        .keySet()
        .forEach(shardKey -> assertTrue(
            matchStage
                .getDocument(shardKey, new BsonDocument())
                .containsKey(queryComparisonOperator),
            format(
                "Missing query comparison operator (%s) for %s in %s",
                queryComparisonOperator, shardKey, matchStage.toJson())));
  }

  private void assertMongoPartitionBounds(final BsonDocument ltMatch, final BsonDocument gteMatch) {
    assertEquals(ltMatch.keySet(), gteMatch.keySet());

    ltMatch.keySet().forEach(shardKey -> {
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
