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

import static com.mongodb.spark.sql.connector.read.partitioner.Partitioner.LOGGER;
import static java.util.Collections.singletonList;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.ConfigException;
import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.spark.sql.connector.read.Scan;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

/** Partitioner helper class, contains various utility methods used by the partitioner instances. */
public final class PartitionerHelper {

  private static final List<BsonDocument> COLL_STATS_AGGREGATION_PIPELINE =
      singletonList(BsonDocument.parse("{'$collStats': {'storageStats': { } } }"));
  private static final BsonDocument PING_COMMAND = BsonDocument.parse("{ping: 1}");
  public static final Partitioner SINGLE_PARTITIONER = new SinglePartitionPartitioner();

  /**
   * Returns the head {@code $match} aggregation stage or an empty document.
   *
   * @param userPipeline configured aggregation pipeline
   * @return the head {@code $match} aggregation stage or an empty document.
   */
  public static BsonDocument matchQuery(final List<BsonDocument> userPipeline) {
    BsonDocument firstPipelineStage =
        userPipeline.isEmpty() ? new BsonDocument() : userPipeline.get(0);
    return firstPipelineStage.getDocument("$match", new BsonDocument());
  }

  /**
   * Creates the upper and lower boundary query
   *
   * <p>Note: does not include min and max key values in the boundary as these are implicit bounds,
   * so not required.
   *
   * @param lower the value of the lower bound
   * @param upper the value of the upper bound
   * @return the document containing the partition bounds
   */
  public static BsonDocument createPartitionBounds(final BsonValue lower, final BsonValue upper) {
    BsonDocument partitionBoundary = new BsonDocument();
    if (lower.getBsonType() != BsonType.MIN_KEY) {
      partitionBoundary.append("$gte", lower);
    }
    if (upper.getBsonType() != BsonType.MAX_KEY) {
      partitionBoundary.append("$lt", upper);
    }
    return partitionBoundary;
  }

  /**
   * Creates the aggregation pipeline for a partition.
   *
   * @param partitionBounds a document representing the partition bounds
   * @param userPipeline the user supplied pipeline
   * @return the aggregation pipeline for a partition
   */
  public static List<BsonDocument> createPartitionPipeline(
      final BsonDocument partitionBounds, final List<BsonDocument> userPipeline) {
    List<BsonDocument> partitionPipeline = new ArrayList<>();
    partitionPipeline.add(new BsonDocument("$match", partitionBounds));
    partitionPipeline.addAll(userPipeline);
    return partitionPipeline;
  }

  /**
   * @param readConfig the read config
   * @return the storage stats or an empty document if the collection does not exist
   * @throws ConfigException
   * If either {@linkplain com.mongodb.spark.sql.connector.config.CollectionsConfig.Type#MULTIPLE multiple}
   * or {@linkplain com.mongodb.spark.sql.connector.config.CollectionsConfig.Type#ALL all}
   * collections are {@linkplain ReadConfig#getCollectionsConfig() configured} to be {@linkplain Scan scanned}.
   */
  public static BsonDocument storageStats(final ReadConfig readConfig) {
    LOGGER.info("Getting collection stats for: {}", readConfig.getNamespace().getFullName());
    try {
      return readConfig
          .withCollection(
              coll -> Optional.ofNullable(coll.aggregate(COLL_STATS_AGGREGATION_PIPELINE)
                      .allowDiskUse(readConfig.getAggregationAllowDiskUse())
                      .comment(readConfig.getComment())
                      .first())
                  .orElseGet(BsonDocument::new))
          .getDocument("storageStats", new BsonDocument());
    } catch (RuntimeException ex) {
      if (ex instanceof MongoCommandException
          && (ex.getMessage().contains("not found.")
              || ((MongoCommandException) ex).getCode() == 26)) {
        LOGGER.info("Could not find collection: {}", readConfig.getCollectionName());
        return new BsonDocument();
      }
      throw new MongoSparkException("Partitioner calling collStats command failed", ex);
    }
  }

  /**
   * @param readConfig the read config
   * @return the list of mongodb hosts
   */
  public static List<String> getPreferredLocations(final ReadConfig readConfig) {
    return readConfig
        .withClient(c -> {
          MongoDatabase db = c.getDatabase(readConfig.getDatabaseName());
          db.runCommand(PING_COMMAND, db.getReadPreference());
          return c.getClusterDescription();
        })
        .getServerDescriptions()
        .stream()
        .flatMap(sd -> sd.getHosts().stream())
        .distinct()
        .collect(Collectors.toList());
  }

  private PartitionerHelper() {}
}
