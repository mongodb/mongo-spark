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

import static com.mongodb.spark.sql.connector.read.partitioner.PartitionerHelper.getPreferredLocations;
import static java.lang.String.format;

import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.ConfigException;
import com.mongodb.spark.sql.connector.read.MongoInputPartition;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.bson.BsonDocument;
import org.jetbrains.annotations.ApiStatus;

/**
 * Partitions collections using a single field.
 *
 * <ul>
 *   <li>{@value PARTITION_FIELD_CONFIG}: The field to be used for partitioning. Defaults to:
 *       {@value PARTITION_FIELD_DEFAULT}.
 * </ul>
 */
@ApiStatus.Internal
abstract class FieldPartitioner implements Partitioner {
  public static final String ID_FIELD = "_id";
  public static final String PARTITION_FIELD_DEFAULT = ID_FIELD;
  public static final String PARTITION_FIELD_CONFIG = "partition.field";

  String getPartitionField(final ReadConfig readConfig) {
    return readConfig
        .getPartitionerOptions()
        .getOrDefault(PARTITION_FIELD_CONFIG, PARTITION_FIELD_DEFAULT);
  }

  /**
   * Creates MongoInputs from the right hand boundaries provided.
   *
   * @param partitionField the field used in partitioning of each partition
   * @param upperBounds an ordered list of the upper boundaries for each partition. The previous
   *     partition is used as the lower bounds. Must not contain any duplicates.
   * @param readConfig the read configuration
   * @return a list of {@link MongoInputPartition}s.
   */
  List<MongoInputPartition> createMongoInputPartitions(
      final String partitionField,
      final List<BsonDocument> upperBounds,
      final ReadConfig readConfig) {

    Set<BsonDocument> upperBoundSet = new HashSet<>(upperBounds);
    if (upperBounds.size() != upperBoundSet.size()) {
      throw new ConfigException(format(
          "Invalid partitioner configuration. The partitions generated contain duplicates: `%s`",
          upperBounds.stream()
              .map(BsonDocument::toJson)
              .collect(Collectors.joining(",", "[", "]"))));
    }

    List<String> preferredLocations = getPreferredLocations(readConfig);
    return IntStream.range(0, upperBounds.size() + 1)
        .mapToObj(i -> {
          BsonDocument previous = i > 0 ? upperBounds.get(i - 1) : null;
          BsonDocument current = i >= upperBounds.size() ? null : upperBounds.get(i);

          BsonDocument matchFilter = new BsonDocument();
          if (previous != null) {
            matchFilter.put(partitionField, new BsonDocument("$gte", previous.get(partitionField)));
          }

          if (current != null) {
            matchFilter.put(
                partitionField,
                matchFilter
                    .getDocument(partitionField, new BsonDocument())
                    .append("$lt", current.get(partitionField)));
          }

          return new MongoInputPartition(
              i,
              PartitionerHelper.createPartitionPipeline(
                  matchFilter, readConfig.getAggregationPipeline()),
              preferredLocations);
        })
        .collect(Collectors.toList());
  }
}
