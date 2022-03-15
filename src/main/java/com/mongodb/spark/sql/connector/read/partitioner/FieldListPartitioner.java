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

import static com.mongodb.spark.sql.connector.read.partitioner.BsonValueComparator.BSON_VALUE_COMPARATOR;
import static com.mongodb.spark.sql.connector.read.partitioner.PartitionerHelper.getPreferredLocations;
import static java.lang.String.format;
import static java.util.Collections.singletonList;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.jetbrains.annotations.ApiStatus;

import org.bson.BsonDocument;
import org.bson.BsonNull;

import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.ConfigException;
import com.mongodb.spark.sql.connector.read.MongoInputPartition;

/**
 * Partitions collections using one or more fields.
 *
 * <ul>
 *   <li>{@value PARTITION_FIELD_LIST_CONFIG}: A comma delimited list of fields to be used for
 *       partitioning. Defaults to: {@value ID_FIELD}.
 * </ul>
 *
 * <p>Note: The Partitioner must provide unique partitions without any duplicates or overlapping
 * values for each field in the field list. The partition field values must also be sorted ascending
 * so that they are growing in value.
 */
@ApiStatus.Internal
abstract class FieldListPartitioner implements Partitioner {
  public static final String ID_FIELD = "_id";
  public static final String PARTITION_FIELD_LIST_CONFIG = "partition.field.list";
  public static final List<String> PARTITION_FIELD_LIST_DEFAULT = singletonList(ID_FIELD);

  List<String> getPartitionFieldList(final ReadConfig readConfig) {
    return readConfig
        .getPartitionerOptions()
        .getList(PARTITION_FIELD_LIST_CONFIG, PARTITION_FIELD_LIST_DEFAULT);
  }

  /**
   * Creates MongoInputs from the right hand boundaries provided.
   *
   * @param partitionFieldList the fields to be used in partitioning each partition
   * @param upperBounds an ordered list of the upper boundaries for each partition. The previous
   *     partition is used as the lower bounds.
   * @param readConfig the read configuration
   * @return a list of {@link MongoInputPartition}s.
   */
  List<MongoInputPartition> createMongoInputPartitions(
      final List<String> partitionFieldList,
      final List<BsonDocument> upperBounds,
      final ReadConfig readConfig) {

    List<String> preferredLocations = getPreferredLocations(readConfig);
    return IntStream.range(0, upperBounds.size() + 1)
        .mapToObj(
            i -> {
              BsonDocument previous = i > 0 ? upperBounds.get(i - 1) : null;
              BsonDocument current = i >= upperBounds.size() ? null : upperBounds.get(i);

              BsonDocument matchFilter = new BsonDocument();
              if (previous != null) {
                partitionFieldList.forEach(
                    k -> {
                      if (previous.containsKey(k)) {
                        matchFilter.put(k, new BsonDocument("$gte", previous.get(k)));
                      }
                    });
              }

              if (current != null) {
                partitionFieldList.forEach(
                    k -> {
                      if (current.containsKey(k)) {
                        matchFilter.put(
                            k,
                            matchFilter
                                .getDocument(k, new BsonDocument())
                                .append("$lt", current.get(k)));
                      }
                    });
              }

              if (previous != null && current != null) {
                for (String k : partitionFieldList) {
                  int comparision =
                      BSON_VALUE_COMPARATOR.compare(
                          current.get(k, BsonNull.VALUE), previous.get(k, BsonNull.VALUE));
                  if (comparision < 0) {
                    throw new ConfigException(
                        "Invalid partitioner configuration. "
                            + "The partitions generated should be contingous and the partition values should be ascending in "
                            + "the partitions to ensure no duplicated data.");
                  } else if (comparision == 0) {
                    throw new ConfigException(
                        format(
                            "Invalid partitioner configuration. The partitions generated contain duplicates "
                                + "for the field: `%s`",
                            k));
                  }
                }
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
