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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import org.bson.BsonDocument;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.read.MongoInputPartition;

/**
 * Sample Partitioner
 *
 * <p>Samples the collection to generate partitions.
 *
 * <p>Uses the average document size to split the collection into average sized chunks
 *
 * <p>The partitioner samples the collection, projects and sorts by the partition fields. Then uses
 * every {@code samplesPerPartition} as the value to use to calculate the partition boundaries.
 *
 * <ul>
 *   <li>{@value PARTITION_SIZE_MB_CONFIG}: The average size for each partition. Calculated using
 *       the average document size to determine the number of documents per partition. Defaults to:
 *       {@value PARTITION_SIZE_MB_DEFAULT}.
 *   <li>{@value SAMPLES_PER_PARTITION_CONFIG}: The number of samples to take per partition.
 *       Defaults to: {@value SAMPLES_PER_PARTITION_DEFAULT}. The total number of samples taken is
 *       calculated as: {@code (samples per partition * count) / number of documents per partition}.
 * </ul>
 *
 * {@inheritDoc}
 */
@ApiStatus.Internal
public final class SamplePartitioner extends FieldListPartitioner {
  public static final String PARTITION_SIZE_MB_CONFIG = "partition.size";
  private static final int PARTITION_SIZE_MB_DEFAULT = 64;

  static final String SAMPLES_PER_PARTITION_CONFIG = "samples.per.partition";
  private static final int SAMPLES_PER_PARTITION_DEFAULT = 10;

  /** Construct an instance */
  public SamplePartitioner() {}

  @Override
  public List<MongoInputPartition> generatePartitions(final ReadConfig readConfig) {
    MongoConfig partitionerOptions = readConfig.getPartitionerOptions();
    List<String> partitionFieldList = getPartitionFieldList(readConfig);

    long partitionSizeInBytes =
        Assertions.validateConfig(
                partitionerOptions.getInt(PARTITION_SIZE_MB_CONFIG, PARTITION_SIZE_MB_DEFAULT),
                i -> i > 0,
                () ->
                    format(
                        "Invalid config: %s should be greater than zero.",
                        PARTITION_SIZE_MB_CONFIG))
            * 1000
            * 1000;
    int samplesPerPartition =
        Assertions.validateConfig(
            partitionerOptions.getInt(SAMPLES_PER_PARTITION_CONFIG, SAMPLES_PER_PARTITION_DEFAULT),
            i -> i > 1,
            () ->
                format(
                    "Invalid config: %s should be greater than one.",
                    SAMPLES_PER_PARTITION_CONFIG));

    BsonDocument storageStats = PartitionerHelper.storageStats(readConfig);
    if (storageStats.isEmpty()) {
      LOGGER.warn("Unable to get collection stats (collstats) returning a single partition.");
      return SINGLE_PARTITIONER.generatePartitions(readConfig);
    }

    BsonDocument matchQuery = PartitionerHelper.matchQuery(readConfig.getAggregationPipeline());
    long count;
    if (matchQuery.isEmpty()) {
      count = storageStats.getNumber("count").longValue();
    } else {
      count = readConfig.withCollection(coll -> coll.countDocuments(matchQuery));
    }
    double avgObjSizeInBytes = storageStats.getNumber("avgObjSize").doubleValue();
    double numDocumentsPerPartition = Math.floor(partitionSizeInBytes / avgObjSizeInBytes);

    if (numDocumentsPerPartition >= count) {
      LOGGER.info(
          "Fewer documents ({}) than the calculated number of documents per partition ({}). Returning a single partition",
          count,
          numDocumentsPerPartition);
      return SINGLE_PARTITIONER.generatePartitions(readConfig);
    }

    int numberOfSamples = (int) Math.ceil((samplesPerPartition * count) / numDocumentsPerPartition);

    List<BsonDocument> samples =
        readConfig.withCollection(
            coll ->
                coll.aggregate(
                        asList(
                            Aggregates.match(matchQuery),
                            Aggregates.sample(numberOfSamples),
                            Aggregates.project(Projections.include(partitionFieldList)),
                            Aggregates.sort(Sorts.ascending(partitionFieldList))))
                    .allowDiskUse(readConfig.getAggregationAllowDiskUse())
                    .into(new ArrayList<>()));

    return createMongoInputPartitions(
        partitionFieldList, getRightHandBoundaries(samples, samplesPerPartition), readConfig);
  }

  /**
   * Reduces the partition samples into the right hand boundaries.
   *
   * <p>Takes every n partitions and uses that as the right hand boundary for the partition. Skips
   * the initial sample as only requires the right hand boundaries
   */
  @NotNull
  private List<BsonDocument> getRightHandBoundaries(
      final List<BsonDocument> samples, final int samplesPerPartition) {
    int lastIndex = samples.size() - 1;
    return IntStream.range(0, samples.size())
        .filter(n -> (n % samplesPerPartition == 0) || n == lastIndex)
        .mapToObj(samples::get)
        .skip(1)
        .collect(Collectors.toList());
  }
}
