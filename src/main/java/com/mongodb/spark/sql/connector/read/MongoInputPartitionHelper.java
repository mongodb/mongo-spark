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
import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;
import com.mongodb.spark.sql.connector.read.partitioner.Partitioner;

final class MongoInputPartitionHelper {

  static List<BsonDocument> generatePipeline(final StructType schema, final ReadConfig readConfig) {
    if (readConfig.includeSchemaFiltersAndProjections()) {
      List<BsonDocument> schemaPipeline =
          generateSchemaPipeline(schema, readConfig.streamPublishFullDocumentOnly());
      return mergePipelines(schemaPipeline, readConfig.getAggregationPipeline());
    } else {
      return readConfig.getAggregationPipeline();
    }
  }

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

      if (readConfig.includeSchemaFiltersAndProjections()) {
        mongoInputPartitions = appendSchemaPipeline(schema, mongoInputPartitions);
      }
      return mongoInputPartitions.toArray(new MongoInputPartition[0]);
    } catch (RuntimeException ex) {
      throw new MongoSparkException("Partitioning failed.", ex);
    }
  }

  private static List<MongoInputPartition> appendSchemaPipeline(
      final StructType schema, final List<MongoInputPartition> initialPartitions) {

    List<BsonDocument> schemaPipeline = generateSchemaPipeline(schema, false);

    if (schemaPipeline.isEmpty()) {
      return initialPartitions;
    }

    List<MongoInputPartition> mongoInputPartitions = new ArrayList<>(initialPartitions.size());
    initialPartitions.forEach(
        p ->
            mongoInputPartitions.add(
                new MongoInputPartition(
                    p.getPartitionId(),
                    mergePipelines(schemaPipeline, p.getPipeline()),
                    p.getPreferredLocations())));
    return mongoInputPartitions;
  }

  private static List<BsonDocument> mergePipelines(
      final List<BsonDocument> schemaPipeline, final List<BsonDocument> pipeline) {
    if (schemaPipeline.isEmpty()) {
      return pipeline;
    }
    List<BsonDocument> withSchemaPipeline = new ArrayList<>(pipeline);
    withSchemaPipeline.addAll(schemaPipeline);
    return withSchemaPipeline;
  }

  private static List<BsonDocument> generateSchemaPipeline(
      final StructType schema, final boolean streamPublishFullDocumentOnly) {
    if (schema.isEmpty()) {
      return emptyList();
    }

    String fieldPrefix = streamPublishFullDocumentOnly ? "fullDocument." : "";

    List<BsonDocument> schemaPipeline = new ArrayList<>();
    BsonDocument fieldExists = new BsonDocument();
    BsonDocument projections = new BsonDocument();
    for (StructField field : schema.fields()) {
      String fieldName = fieldPrefix + field.name();

      if (!field.nullable()) {
        fieldExists.append(fieldName, new BsonDocument("$exists", BsonBoolean.TRUE));
      }
      projections.append(fieldName, new BsonInt32(1));
    }
    if (!fieldExists.isEmpty()) {
      schemaPipeline.add(new BsonDocument("$match", fieldExists));
    }
    schemaPipeline.add(new BsonDocument("$project", projections));
    return schemaPipeline;
  }

  private MongoInputPartitionHelper() {}
}
