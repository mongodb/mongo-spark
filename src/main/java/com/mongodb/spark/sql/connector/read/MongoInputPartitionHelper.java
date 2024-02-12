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

import static com.mongodb.assertions.Assertions.fail;
import static com.mongodb.spark.sql.connector.read.partitioner.Partitioner.LOGGER;
import static com.mongodb.spark.sql.connector.read.partitioner.PartitionerHelper.SINGLE_PARTITIONER;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.spark.sql.connector.config.CollectionsConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;
import com.mongodb.spark.sql.connector.read.partitioner.Partitioner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.spark.sql.types.StructType;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;
import org.bson.conversions.Bson;

final class MongoInputPartitionHelper {

  static MongoInputPartition[] generateMongoBatchPartitions(
      final StructType schema, final ReadConfig readConfig) {
    try {
      Partitioner partitioner = readConfig.getPartitioner();
      LOGGER.debug("Generating partitions using '{}'.", partitioner.getClass().getSimpleName());

      List<MongoInputPartition> mongoInputPartitions = partitioner.generatePartitions(readConfig);

      if (mongoInputPartitions.isEmpty()) {
        LOGGER.warn(
            "Partitioner '{}' failed to create any partitions. Falling back to a single partition for the collection",
            partitioner.getClass().getSimpleName());
        mongoInputPartitions = SINGLE_PARTITIONER.generatePartitions(readConfig);
      } else {
        LOGGER.debug(
            "Partitioner '{}' created {} partition(s).",
            partitioner.getClass().getSimpleName(),
            mongoInputPartitions.size());
      }

      List<MongoInputPartition> partitions = mongoInputPartitions;
      return schemaProjections(schema, readConfig.streamPublishFullDocumentOnly())
          .map(schemaProjection -> {
            List<MongoInputPartition> partitionsWithProjection = new ArrayList<>(partitions.size());
            partitions.forEach(p -> partitionsWithProjection.add(new MongoInputPartition(
                p.getPartitionId(),
                mergePipelineFunction(p.getPipeline()).apply(schemaProjection),
                p.getPreferredLocations())));
            return partitionsWithProjection;
          })
          .orElse(mongoInputPartitions)
          .toArray(new MongoInputPartition[0]);
    } catch (RuntimeException ex) {
      throw new MongoSparkException("Partitioning failed. " + ex.getMessage(), ex);
    }
  }

  static MongoMicroBatchInputPartition[] generateMicroBatchPartitions(
      final StructType schema,
      final ReadConfig readConfig,
      final BsonTimestampOffset start,
      final BsonTimestampOffset end) {

    List<BsonDocument> partitionPipeline = generatePipeline(schema, readConfig);

    BsonTimestamp startTimestamp = start.getBsonTimestamp();
    BsonTimestamp endTimestamp = end.getBsonTimestamp();

    List<BsonTimestamp> partitions = new ArrayList<>();
    partitions.add(startTimestamp);

    if (startTimestamp.getTime() >= 0) {
      int partitionCount = readConfig.getMicroBatchMaxPartitionCount();
      int totalSecondsDiff = endTimestamp.getTime() - startTimestamp.getTime();
      int numberOfBatches = Math.min(totalSecondsDiff, partitionCount);
      int incPerBatch = (int) Math.ceil((double) totalSecondsDiff / numberOfBatches);

      BsonTimestamp previous = startTimestamp;
      while (previous.getTime() + incPerBatch < endTimestamp.getTime()) {
        BsonTimestamp next = new BsonTimestamp(previous.getTime() + incPerBatch, 0);
        partitions.add(next);
        previous = next;
      }
    }
    partitions.add(endTimestamp);

    return IntStream.range(1, partitions.size())
        .mapToObj(i -> new MongoMicroBatchInputPartition(
            i,
            partitionPipeline,
            new BsonTimestampOffset(partitions.get(i - 1)),
            new BsonTimestampOffset(partitions.get(i))))
        .toArray(MongoMicroBatchInputPartition[]::new);
  }

  static List<BsonDocument> generatePipeline(final StructType schema, final ReadConfig readConfig) {
    ArrayList<BsonDocument> result =
        new ArrayList<>(collectionsConfigPipeline(readConfig.getCollectionsConfig()));
    List<BsonDocument> customPipelineAndSchemaProjections = schemaProjections(
            schema, readConfig.streamPublishFullDocumentOnly())
        .map(mergePipelineFunction(readConfig.getAggregationPipeline()))
        .orElse(readConfig.getAggregationPipeline());
    result.addAll(customPipelineAndSchemaProjections);
    return result;
  }

  private static Optional<BsonDocument> schemaProjections(
      final StructType schema, final boolean streamPublishFullDocumentOnly) {
    if (schema.isEmpty()) {
      return Optional.empty();
    }

    String fieldPrefix = streamPublishFullDocumentOnly ? "fullDocument." : "";
    BsonDocument projections = new BsonDocument();
    Arrays.stream(schema.fields())
        .map(f -> fieldPrefix + f.name())
        .forEach(f -> projections.append(f, new BsonInt32(1)));
    return Optional.of(new BsonDocument("$project", projections));
  }

  private static Function<BsonDocument, List<BsonDocument>> mergePipelineFunction(
      final List<BsonDocument> pipeline) {
    return projectionStage -> {
      List<BsonDocument> pipelineWithSchemaProjection = new ArrayList<>(pipeline);
      pipelineWithSchemaProjection.add(projectionStage);
      return pipelineWithSchemaProjection;
    };
  }

  /**
   * Returns the stages of the aggregation pipeline
   * that are to be used before any other stages in order to account for the {@code collectionsConfig}.
   */
  private static Collection<BsonDocument> collectionsConfigPipeline(
      final CollectionsConfig collectionsConfig) {
    // `invalidate` events do not have the `ns.coll` field,
    // so we have to explicitly allow them in the filter
    Bson invalidateOperationType = Filters.eq("operationType", "invalidate");
    switch (collectionsConfig.getType()) {
      case SINGLE:
        return emptyList();
      case MULTIPLE:
        return singletonList(Aggregates.match(Filters.or(
                Filters.in("ns.coll", collectionsConfig.getNames().toArray()),
                invalidateOperationType))
            .toBsonDocument());
      case ALL:
        return singletonList(
            Aggregates.match(Filters.or(Filters.exists("ns.coll"), invalidateOperationType))
                .toBsonDocument());
      default:
        throw fail();
    }
  }

  private MongoInputPartitionHelper() {}
}
