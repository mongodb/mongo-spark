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
import org.bson.BsonInt32;
import org.jetbrains.annotations.ApiStatus;

/**
 * Paginate by size partitioner.
 *
 * <p>Paginates the collection using the average document size to split the collection into average
 * sized chunks:
 *
 * <ul>
 *   <li>{@value PARTITION_FIELD_CONFIG}: The field to be used for partitioning. Must be a unique
 *       field. Defaults to: {@value ID_FIELD}.
 *   <li>{@value PARTITION_SIZE_MB_CONFIG}: The average size per partition. Defaults to: {@value
 *       PARTITION_SIZE_MB_DEFAULT}.
 * </ul>
 */
@ApiStatus.Internal
public final class PaginateBySizePartitioner extends PaginatePartitioner {

  public static final String PARTITION_SIZE_MB_CONFIG = "partition.size";
  private static final int PARTITION_SIZE_MB_DEFAULT = 64;

  /** Construct an instance */
  public PaginateBySizePartitioner() {}

  @Override
  public List<MongoInputPartition> generatePartitions(final ReadConfig readConfig) {
    MongoConfig partitionerOptions = readConfig.getPartitionerOptions();
    int partitionSizeBytes = Assertions.validateConfig(
            partitionerOptions.getInt(PARTITION_SIZE_MB_CONFIG, PARTITION_SIZE_MB_DEFAULT),
            i -> i > 0,
            () ->
                format("Invalid config: %s should be greater than zero.", PARTITION_SIZE_MB_CONFIG))
        * 1000
        * 1000;

    BsonDocument storageStats = PartitionerHelper.storageStats(readConfig);
    if (storageStats.isEmpty()) {
      LOGGER.warn("Unable to get collection stats returning a single partition.");
      return SINGLE_PARTITIONER.generatePartitions(readConfig);
    }

    double avgObjSizeInBytes =
        storageStats.get("avgObjSize", new BsonInt32(0)).asNumber().doubleValue();
    if (avgObjSizeInBytes >= partitionSizeBytes) {
      LOGGER.warn(
          "Average document size `{}` is greater than the partition size `{}`. Please increase the partition size."
              + "Returning a single partition.",
          avgObjSizeInBytes,
          partitionSizeBytes);
      return SINGLE_PARTITIONER.generatePartitions(readConfig);
    }

    int numDocumentsPerPartition = (int) Math.floor(partitionSizeBytes / avgObjSizeInBytes);
    BsonDocument matchQuery = PartitionerHelper.matchQuery(readConfig.getAggregationPipeline());
    long count;
    if (matchQuery.isEmpty() && storageStats.containsKey("count")) {
      count = storageStats.getNumber("count").longValue();
    } else {
      count = readConfig.withCollection(coll ->
          coll.countDocuments(matchQuery, new CountOptions().comment(readConfig.getComment())));
    }

    if (count <= numDocumentsPerPartition) {
      LOGGER.warn(
          "The calculated number of documents per partition {} is greater than or equal to the number of matching documents. "
              + "Returning a single partition.",
          numDocumentsPerPartition);
      return SINGLE_PARTITIONER.generatePartitions(readConfig);
    }
    return createMongoInputPartitions(count, numDocumentsPerPartition, readConfig);
  }
}
