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

import static com.mongodb.spark.sql.connector.read.partitioner.Partitioner.LOGGER;
import static com.mongodb.spark.sql.connector.read.partitioner.PartitionerHelper.SINGLE_PARTITIONER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.spark.sql.types.StructType;

import org.bson.BsonDocument;
import org.bson.BsonInt32;

import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;
import com.mongodb.spark.sql.connector.read.partitioner.Partitioner;

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
          .map(
              schemaProjection -> {
                List<MongoInputPartition> partitionsWithProjection =
                    new ArrayList<>(partitions.size());
                partitions.forEach(
                    p ->
                        partitionsWithProjection.add(
                            new MongoInputPartition(
                                p.getPartitionId(),
                                mergePipelineFunction(p.getPipeline()).apply(schemaProjection),
                                p.getPreferredLocations())));
                return partitionsWithProjection;
              })
          .orElse(mongoInputPartitions)
          .toArray(new MongoInputPartition[0]);
    } catch (RuntimeException ex) {
      throw new MongoSparkException("Partitioning failed.", ex);
    }
  }

  static List<BsonDocument> generatePipeline(final StructType schema, final ReadConfig readConfig) {
    return schemaProjections(schema, readConfig.streamPublishFullDocumentOnly())
        .map(mergePipelineFunction(readConfig.getAggregationPipeline()))
        .orElse(readConfig.getAggregationPipeline());
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

  private MongoInputPartitionHelper() {}
}
