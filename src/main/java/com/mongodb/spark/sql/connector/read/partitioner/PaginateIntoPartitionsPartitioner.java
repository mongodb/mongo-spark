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

import com.mongodb.client.model.CountOptions;
import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.read.MongoInputPartition;
import java.util.List;
import org.bson.BsonDocument;
import org.jetbrains.annotations.ApiStatus;

/**
 * Paginate into partitions partitioner.
 *
 * <p>Determines the number of documents per partition by dividing the collection count of documents
 * by the maximum number of allowable partitions.
 *
 * <ul>
 *   <li>{@value PARTITION_FIELD_CONFIG}: The field to be used for partitioning. Must be a unique
 *       field. Defaults to: {@value ID_FIELD}.
 *   <li>{@value MAX_NUMBER_OF_PARTITIONS_CONFIG}: The maximum number of partitions this partitioner
 *       will create. Defaults to: {@value MAX_NUMBER_OF_PARTITIONS_DEFAULT}.
 * </ul>
 */
@ApiStatus.Internal
public final class PaginateIntoPartitionsPartitioner extends PaginatePartitioner {

  public static final String MAX_NUMBER_OF_PARTITIONS_CONFIG = "max.number.of.partitions";
  private static final int MAX_NUMBER_OF_PARTITIONS_DEFAULT = 64;

  /** Construct an instance */
  public PaginateIntoPartitionsPartitioner() {}

  @Override
  public List<MongoInputPartition> generatePartitions(final ReadConfig readConfig) {
    MongoConfig partitionerOptions = readConfig.getPartitionerOptions();
    int maxNumberOfPartitions = Assertions.validateConfig(
        partitionerOptions.getInt(
            MAX_NUMBER_OF_PARTITIONS_CONFIG, MAX_NUMBER_OF_PARTITIONS_DEFAULT),
        i -> i > 0,
        () -> format(
            "Invalid config: %s should be greater than zero.", MAX_NUMBER_OF_PARTITIONS_CONFIG));

    BsonDocument matchQuery = PartitionerHelper.matchQuery(readConfig.getAggregationPipeline());
    long count = readConfig.withCollection(coll ->
        coll.countDocuments(matchQuery, new CountOptions().comment(readConfig.getComment())));
    if (count <= 1) {
      LOGGER.warn("Returning a single partition.");
      return SINGLE_PARTITIONER.generatePartitions(readConfig);
    } else if (count <= maxNumberOfPartitions) {
      LOGGER.warn(
          "Inefficient partitioning, returning a partition per document. Total documents = {}, max documents per partition = {}"
              + "Decrease the `{}` configuration or use an alternative partitioner.",
          count,
          maxNumberOfPartitions,
          MAX_NUMBER_OF_PARTITIONS_CONFIG);
    }

    int numDocumentsPerPartition = (int) Math.ceil(count / (double) maxNumberOfPartitions);
    return createMongoInputPartitions(count, numDocumentsPerPartition, readConfig);
  }
}
