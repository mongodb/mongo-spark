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
import static com.mongodb.spark.sql.connector.read.partitioner.PartitionerHelper.getPreferredLocations;
import static java.lang.String.format;
import static java.util.Collections.singletonList;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.Filters;
import com.mongodb.connection.ServerDescription;
import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;
import com.mongodb.spark.sql.connector.read.MongoInputPartition;
import java.util.ArrayList;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Auto Bucket Partitioner
 *
 * <p>A sample based partitioner that provides support for all collection types.
 * Supports partitioning across single or multiple fields, including nested fields.
 *
 * <p>The logic for the partitioner is as follows:</p>
 * <ul>
 *      <li>Calculate the number of documents per partition.<br>
 *       Runs a {@code $collStats} aggregation to get the average document size.</li>
 *       <li>Determines the total count of documents.<br>
 *          Uses the {@code $collStats} count or by running a {@code countDocuments} query
 *          if the user supplies their own {@code "aggregation.pipeline" } configuration.</li>
 *      <li>Determines the number of partitions.<br>
 *      Calculated as: {@code count / number of documents per partition}
 *      </li>
 *      <li>Determines the number of documents to {@code $sample}.<br>
 *      Calculated as: {@code samples per partition * number of partitions}.</li>
 *      <li>Creates the aggregation pipeline to generate the partitions.<br>
 *         <pre><code>
 *             [{$match: &lt;the $match stage of the users aggregation pipeline - iff the first stage is a $match&gt;},
 *              {$sample: &lt;number of documents to $sample&gt;},
 *              // The next stage is only added iff fieldList.size() &gt; 1
 *              {$addFields: {&lt;partition key projection field&gt;: {&lt;'i': '$fieldList[i]' ...&gt;}}
 *              {$bucketAuto: {
 *                      groupBy: &lt;partition key projection field&gt;,
 *                      buckets: &lt;number of partitions&gt;
 *                  }
 *              }]
 *        </code></pre>
 *      </li>
 *  </ul>
 *
 * <p>Configurations:</p>
 * <ul>
 *   <li>{@value PARTITION_FIELD_LIST_CONFIG}: The field list to be used for partitioning.
 *   Either a single field name or a list of comma separated fields. Defaults to: {@value ID}.
 *   <li>{@value PARTITION_CHUNK_SIZE_MB_CONFIG }: The average size (MB) for each partition.<br>
 *     <strong>Note:</strong> Uses the average document size to determine the number of documents per partition
 *     so partitions may not be even.<br> Defaults to: {@value PARTITION_CHUNK_SIZE_MB_DEFAULT}.
 *   <li>{@value SAMPLES_PER_PARTITION_CONFIG}: The number of samples to take per partition.<br>
 *       Defaults to: {@value SAMPLES_PER_PARTITION_DEFAULT}.
 *   <li>{@value PARTITION_KEY_PROJECTION_FIELD_CONFIG}: The field name to use for a projected field that contains all the
 *       fields used to partition the collection.<br>
 *       Defaults to: {@value PARTITION_KEY_PROJECTION_FIELD_DEFAULT}.<br>
 *       Recommended to only change if there already is a {@value PARTITION_KEY_PROJECTION_FIELD_DEFAULT} field in the document.
 * </ul>
 *
 * <p>Partitions are calculated as logical ranges. When using sharded clusters these will map closely to ranged chunks.
 * When using with hashed shard keys these logical ranges require broadcast operations.
 *
 * <p>Similar to the {@link SamplePartitioner} however uses the
 * <a href="https://www.mongodb.com/docs/manual/reference/operator/aggregation/bucketAuto/">$bucketAuto</a> aggregation stage
 * to generate the partition bounds.
 * {@inheritDoc}
 */
@ApiStatus.Internal
public final class AutoBucketPartitioner implements Partitioner {

  private static final String ID = "_id";
  private static final String MIN = "min";
  private static final String MAX = "max";
  private static final int SEVEN_DOT_ZERO_WIRE_VERSION = 21;

  public static final String PARTITION_FIELD_LIST_CONFIG = "fieldList";
  private static final List<String> PARTITION_FIELD_LIST_DEFAULT = singletonList(ID);

  public static final String PARTITION_CHUNK_SIZE_MB_CONFIG = "chunkSize";
  private static final int PARTITION_CHUNK_SIZE_MB_DEFAULT = 64;

  public static final String SAMPLES_PER_PARTITION_CONFIG = "samplesPerPartition";
  private static final int SAMPLES_PER_PARTITION_DEFAULT = 100;

  public static final String PARTITION_KEY_PROJECTION_FIELD_CONFIG = "partitionKeyProjectionField";
  private static final String PARTITION_KEY_PROJECTION_FIELD_DEFAULT = "__idx";

  /** Construct an instance */
  public AutoBucketPartitioner() {}

  @Override
  public List<MongoInputPartition> generatePartitions(final ReadConfig readConfig) {
    MongoConfig partitionerOptions = readConfig.getPartitionerOptions();

    List<String> partitionFieldList = Assertions.validateConfig(
        partitionerOptions.getList(PARTITION_FIELD_LIST_CONFIG, PARTITION_FIELD_LIST_DEFAULT),
        i -> !i.isEmpty(),
        () -> format("Invalid config: %s must not be empty.", PARTITION_FIELD_LIST_CONFIG));

    long partitionSizeInBytes = Assertions.validateConfig(
            partitionerOptions.getInt(
                PARTITION_CHUNK_SIZE_MB_CONFIG, PARTITION_CHUNK_SIZE_MB_DEFAULT),
            i -> i > 0,
            () -> format(
                "Invalid config: %s should be greater than zero.", PARTITION_CHUNK_SIZE_MB_CONFIG))
        * 1000
        * 1000;

    int samplesPerPartition = Assertions.validateConfig(
        partitionerOptions.getInt(SAMPLES_PER_PARTITION_CONFIG, SAMPLES_PER_PARTITION_DEFAULT),
        i -> i > 1,
        () ->
            format("Invalid config: %s should be greater than one.", SAMPLES_PER_PARTITION_CONFIG));

    String partitionProjectionKey = partitionerOptions.getOrDefault(
        PARTITION_KEY_PROJECTION_FIELD_CONFIG, PARTITION_KEY_PROJECTION_FIELD_DEFAULT);

    BsonDocument storageStats = PartitionerHelper.storageStats(readConfig);

    Integer serverMaxWireVersion =
        readConfig.withClient(c -> c.getClusterDescription().getServerDescriptions().stream()
            .mapToInt(ServerDescription::getMaxWireVersion)
            .max()
            .orElse(0));
    if (serverMaxWireVersion < SEVEN_DOT_ZERO_WIRE_VERSION && partitionFieldList.size() > 1) {
      throw new MongoSparkException(
          "Invalid partitioner strategy. The AutoBucketPartitioner only supports compound keys in MongoDB 7.0 and above.");
    }

    if (storageStats.isEmpty()) {
      LOGGER.warn("Unable to get collection stats (collstats) returning a single partition.");
      return SINGLE_PARTITIONER.generatePartitions(readConfig);
    }

    double avgObjSizeInBytes = PartitionerHelper.averageDocumentSize(storageStats);
    double numDocumentsPerPartition = Math.floor(partitionSizeInBytes / avgObjSizeInBytes);

    BsonDocument usersCollectionFilter =
        PartitionerHelper.matchQuery(readConfig.getAggregationPipeline());
    long count;
    if (usersCollectionFilter.isEmpty() && storageStats.containsKey("count")) {
      count = storageStats.getNumber("count").longValue();
    } else {
      count = readConfig.withCollection(coll -> coll.countDocuments(
          usersCollectionFilter, new CountOptions().comment(readConfig.getComment())));
    }

    if (numDocumentsPerPartition == 0 || numDocumentsPerPartition >= count) {
      LOGGER.info(
          "Fewer documents ({}) than the calculated number of documents per partition ({}). Returning a single partition",
          count,
          numDocumentsPerPartition);
      return SINGLE_PARTITIONER.generatePartitions(readConfig);
    }

    int numberOfBuckets = Math.toIntExact((long) Math.ceil(count / numDocumentsPerPartition));
    int numberOfSamples = Math.toIntExact((long) samplesPerPartition * numberOfBuckets);

    List<BsonDocument> buckets =
        readConfig.withCollection(coll -> coll.aggregate(createBucketAutoPipeline(
                usersCollectionFilter,
                partitionFieldList,
                partitionProjectionKey,
                numberOfSamples,
                numberOfBuckets))
            .allowDiskUse(readConfig.getAggregationAllowDiskUse())
            .comment(readConfig.getComment())
            .into(new ArrayList<>()));

    if (buckets.size() < 2) {
      LOGGER.info("Less than two buckets generated, so returning a single partition");
      return SINGLE_PARTITIONER.generatePartitions(readConfig);
    }

    return createMongoInputPartitions(
        buckets,
        readConfig.getAggregationPipeline(),
        partitionFieldList,
        partitionProjectionKey,
        getPreferredLocations(readConfig));
  }

  /**
   * Creates the $sample and $bucketAuto aggregation pipeline used for determining the partition bounds.
   *
   * @param usersCollectionFilter the filter part of a user supplied $match aggregation pipeline or an empty document
   * @param partitionFieldList the fields to partition the collection by
   * @param partitionProjectionKey the partition projection key only used if there are multiple partition fields
   * @param numberOfSamples the number of samples
   * @param numberOfBuckets the number of buckets
   * @return the pipeline
   */
  @VisibleForTesting
  static List<BsonDocument> createBucketAutoPipeline(
      final BsonDocument usersCollectionFilter,
      final List<String> partitionFieldList,
      final String partitionProjectionKey,
      final int numberOfSamples,
      final int numberOfBuckets) {

    List<BsonDocument> pipeline = new ArrayList<>();
    if (!usersCollectionFilter.isEmpty()) {
      pipeline.add(Aggregates.match(usersCollectionFilter).toBsonDocument());
    }
    pipeline.add(Aggregates.sample(numberOfSamples).toBsonDocument());

    if (partitionFieldList.size() > 1) {
      pipeline.add(addFieldsStage(partitionFieldList, partitionProjectionKey));
    }

    String groupByField =
        partitionFieldList.size() > 1 ? partitionProjectionKey : partitionFieldList.get(0);

    pipeline.add(Aggregates.bucketAuto("$" + groupByField, numberOfBuckets).toBsonDocument());

    return pipeline;
  }

  @VisibleForTesting
  static List<MongoInputPartition> createMongoInputPartitions(
      final List<BsonDocument> buckets,
      final List<BsonDocument> usersPipeline,
      final List<String> partitionFieldList,
      final String partitionProjectionKey,
      final List<String> preferredLocations) {

    String matchField =
        partitionFieldList.size() == 1 ? partitionFieldList.get(0) : partitionProjectionKey;
    List<MongoInputPartition> inputPartitions = new ArrayList<>();

    for (int i = 0; i < buckets.size(); i++) {
      BsonDocument bucket = buckets.get(i);
      Assertions.assertTrue(
          () -> bucket.containsKey(ID) && bucket.isDocument(ID),
          () -> format(
              "Unexpected auto bucket format %s field required. Got: %s.", ID, bucket.toJson()));
      BsonDocument bounds = bucket.getDocument(ID);

      Assertions.assertTrue(
          () -> bounds.containsKey(MIN) && bounds.containsKey(MAX),
          () -> format(
              "Unexpected auto bucket format. Expected %s and %s ranges got: %s.",
              MIN, MAX, bounds.toJson()));

      boolean includeMin = i > 0;
      boolean includeMax = i < buckets.size() - 1;

      List<Bson> partitionBounds = new ArrayList<>();
      if (includeMin) {
        partitionBounds.add(Filters.gte(matchField, bounds.get(MIN)));
      }
      if (includeMax) {
        partitionBounds.add(Filters.lt(matchField, bounds.get(MAX)));
      }
      BsonDocument partitionBoundsMatch =
          Aggregates.match(Filters.and(partitionBounds)).toBsonDocument();

      List<BsonDocument> partitionPipeline = createPartitionPipeline(
          partitionFieldList, partitionProjectionKey, partitionBoundsMatch, usersPipeline);
      inputPartitions.add(new MongoInputPartition(i, partitionPipeline, preferredLocations));
    }
    return inputPartitions;
  }

  /**
   * Creates the aggregation pipeline for the partition
   *
   * @param partitionFieldList the fields to partition the collection by
   * @param partitionProjectionKey the partition projection key only used if there are multiple partition fields
   * @param partitionBounds the calculated partition bounds $match query
   * @param usersPipeline the configured user supplied aggregation pipeline
   * @return the partition pipeline
   */
  @VisibleForTesting
  static List<BsonDocument> createPartitionPipeline(
      final List<String> partitionFieldList,
      final String partitionProjectionKey,
      final BsonDocument partitionBounds,
      final List<BsonDocument> usersPipeline) {
    List<BsonDocument> partitionPipeline = new ArrayList<>();

    if (partitionFieldList.size() > 1) {
      partitionPipeline.add(addFieldsStage(partitionFieldList, partitionProjectionKey));
    }

    partitionPipeline.add(partitionBounds);

    if (partitionFieldList.size() > 1) {
      partitionPipeline.add(Aggregates.unset(partitionProjectionKey).toBsonDocument());
    }
    partitionPipeline.addAll(usersPipeline);
    return partitionPipeline;
  }

  /**
   * Adds a new document to match against containing the values of the partition field list fields.
   *
   * <p>Uses a numeric index so sub documents can be supported when partitioning the collection.
   *
   * @param partitionFieldList the fields to partition the collection by
   * @param partitionProjectionKey the partition projection key only used if there are multiple partition fields
   * @return the $addFields pipeline stage
   */
  private static BsonDocument addFieldsStage(
      final List<String> partitionFieldList, final String partitionProjectionKey) {
    BsonDocument addFieldValue = new BsonDocument();
    for (int i = 0; i < partitionFieldList.size(); i++) {
      addFieldValue.put(String.valueOf(i), new BsonString("$" + partitionFieldList.get(i)));
    }
    return Aggregates.addFields(new Field<>(partitionProjectionKey, addFieldValue))
        .toBsonDocument();
  }
}
