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

import static java.lang.String.format;

import com.mongodb.ServerAddress;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;
import com.mongodb.spark.sql.connector.read.MongoInputPartition;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Sharded Partitioner
 *
 * <p>Uses the chunks collection and partitions the collection based on the sharded collections
 * chunk ranges.
 *
 * <p>Generated partitions are ordered by their shard key range, which groups together all the
 * partitions that belong to the same shard. As Spark schedules tasks in partition order, this can
 * cause the executors to query a single shard at a time rather than spreading the load across all
 * shards. The following options can be used to shuffle the partitions so the load is balanced
 * across shards:
 *
 * <ul>
 *   <li>{@value SHUFFLE_CONFIG}: Shuffle the generated partitions so consecutive partitions are
 *       unlikely to belong to the same shard. Defaults to: {@value SHUFFLE_DEFAULT}.
 *   <li>{@value SHUFFLE_SEED_CONFIG}: The seed to use when shuffling the partitions. Set a seed to
 *       get a reproducible shuffle. Only actuated when {@value SHUFFLE_CONFIG} is {@code true}.
 *       Defaults to a random seed.
 * </ul>
 *
 * <p><strong>Note:</strong> Does not support collections sharded using hashed shard keys or
 * compound shard keys.
 */
@ApiStatus.Internal
public final class ShardedPartitioner implements Partitioner {

  private static final boolean SHUFFLE_DEFAULT = false;

  static final String SHUFFLE_SEED_CONFIG = "shuffle.seed";
  static final String SHUFFLE_CONFIG = "shuffle";
  private static final String CONFIG_DATABASE = "config";
  private static final String CONFIG_COLLECTIONS = "collections";
  private static final String CONFIG_CHUNKS = "chunks";
  private static final String CONFIG_SHARDS = "shards";
  private static final String NAMESPACE_FIELD = "ns";
  private static final String UUID_FIELD = "uuid";
  private static final String ID_FIELD = "_id";
  private static final String HOST_FIELD = "host";
  private static final Bson CHUNKS_PROJECTIONS = Projections.include("min", "max", "shard");
  private static final Bson SHARDS_PROJECTIONS = Projections.include(ID_FIELD, HOST_FIELD);
  private static final Bson SORTS = Sorts.ascending("min");
  private static final BsonValue BSON_MIN = new BsonMinKey();
  private static final BsonValue BSON_MAX = new BsonMaxKey();

  /** Construct an instance */
  public ShardedPartitioner() {}

  @Override
  public List<MongoInputPartition> generatePartitions(final ReadConfig readConfig) {
    LOGGER.info("Getting shard chunk bounds for '{}'", readConfig.getNamespace().getFullName());

    BsonDocument configCollectionMetadata = readConfig.withClient(client -> client
        .getDatabase(CONFIG_DATABASE)
        .getCollection(CONFIG_COLLECTIONS, BsonDocument.class)
        .find(Filters.eq(ID_FIELD, readConfig.getNamespace().getFullName()))
        .projection(Projections.include("_id", "timestamp", "uuid", "dropped", "key"))
        .first());

    if (configCollectionMetadata == null) {
      LOGGER.warn(
          "Collection '{}' does not appear to be sharded, continuing with a single partition. "
              + "To split the collections into multiple partitions please use a suitable partitioner.",
          readConfig.getNamespace().getFullName());
      return new SinglePartitionPartitioner().generatePartitions(readConfig);
    }

    if (configCollectionMetadata.getBoolean("dropped", BsonBoolean.FALSE).getValue()) {
      LOGGER.warn(
          "Collection '{}' has been dropped continuing with a single partition.",
          readConfig.getNamespace().getFullName());
      return new SinglePartitionPartitioner().generatePartitions(readConfig);
    }

    BsonDocument keyDocument = configCollectionMetadata.getDocument("key", new BsonDocument());
    if (keyDocument.keySet().size() > 1) {
      throw new MongoSparkException(
          "Invalid partitioner strategy. The Sharded partitioner does not support compound shard keys.");
    } else if (keyDocument.containsValue(new BsonString("hashed"))) {
      throw new MongoSparkException(
          "Invalid partitioner strategy. The Sharded partitioner does not support hashed shard keys.");
    }

    // Depending on MongoDB version the chunks collection will either use the collection namespace
    // or the metadata uuid as the identifier for the chunks data.
    Bson chunksMatchPredicate = Filters.or(
        new BsonDocument(NAMESPACE_FIELD, configCollectionMetadata.get(ID_FIELD)),
        new BsonDocument(UUID_FIELD, configCollectionMetadata.get(UUID_FIELD)));

    List<BsonDocument> chunks = readConfig.withClient(client -> client
        .getDatabase(CONFIG_DATABASE)
        .getCollection(CONFIG_CHUNKS, BsonDocument.class)
        .find(chunksMatchPredicate)
        .projection(CHUNKS_PROJECTIONS)
        .sort(SORTS)
        .allowDiskUse(readConfig.getAggregationAllowDiskUse())
        .into(new ArrayList<>()));

    List<MongoInputPartition> partitions = createMongoInputPartitions(chunks, readConfig);

    if (partitions.isEmpty()) {
      LOGGER.warn(
          "There is no chunk information for '{}' using a single partition",
          readConfig.getNamespace().getFullName());
      return new SinglePartitionPartitioner().generatePartitions(readConfig);
    }

    return partitions;
  }

  private static Random createShuffleRandom(final MongoConfig partitionerOptions) {
    // this seed can be used for easier testing and reproducibility of the shuffle
    final String seed = partitionerOptions.get(SHUFFLE_SEED_CONFIG);
    if (seed == null) {
      return new Random();
    }
    try {
      return new Random(Long.parseLong(seed.trim()));
    } catch (final NumberFormatException e) {
      throw new MongoSparkException(
          format("Invalid config: %s must be a valid long, got: %s", SHUFFLE_SEED_CONFIG, seed));
    }
  }

  @NotNull
  private List<MongoInputPartition> createMongoInputPartitions(
      final List<BsonDocument> chunks, final ReadConfig readConfig) {
    MongoConfig partitionerOptions = readConfig.getPartitionerOptions();
    boolean shuffle = partitionerOptions.getBoolean(SHUFFLE_CONFIG, SHUFFLE_DEFAULT);
    Random shuffleRandom = shuffle ? createShuffleRandom(partitionerOptions) : null;
    Map<String, List<String>> shardMap = createShardMap(readConfig);

    return IntStream.range(0, chunks.size())
        .mapToObj(i -> {
          BsonDocument chunkDocument = chunks.get(i);
          BsonDocument min = chunkDocument.getDocument("min");
          BsonDocument max = chunkDocument.getDocument("max");
          BsonDocument partitionBounds = new BsonDocument();

          Assertions.ensureState(
              () -> min.keySet().equals(max.keySet()),
              () -> format(
                  "Unexpected chunk data information. Differing keys for min / max ranges. %s",
                  chunkDocument.toJson()));
          min.keySet().forEach(shardKey -> {
            BsonDocument shardKeyBoundary = PartitionerHelper.createPartitionBounds(
                min.getOrDefault(shardKey, BSON_MIN), max.get(shardKey, BSON_MAX));
            if (!shardKeyBoundary.isEmpty()) {
              partitionBounds.put(shardKey, shardKeyBoundary);
            }
          });
          if (partitionBounds.isEmpty()) {
            return null;
          }

          return new MongoInputPartition(
              i,
              PartitionerHelper.createPartitionPipeline(
                  partitionBounds, readConfig.getAggregationPipeline()),
              shardMap.get(chunkDocument.getString("shard", new BsonString("")).getValue()));
        })
        .filter(Objects::nonNull)
        .collect(Collectors.collectingAndThen(Collectors.toCollection(ArrayList::new), collected -> {
          if (shuffle) {
            Collections.shuffle(collected, shuffleRandom);
          }
          return collected;
        }));
  }

  @NotNull
  private Map<String, List<String>> createShardMap(final ReadConfig readConfig) {
    return readConfig.withClient(client -> client
        .getDatabase(CONFIG_DATABASE)
        .getCollection(CONFIG_SHARDS, BsonDocument.class)
        .find()
        .projection(SHARDS_PROJECTIONS)
        .into(new ArrayList<>())
        .stream()
        .collect(Collectors.toMap(
            s -> s.getString(ID_FIELD).getValue(),
            s -> getHosts(s.getString(HOST_FIELD).getValue()))));
  }

  /**
   * See: https://docs.mongodb.com/manual/reference/config-database/#mongodb-data-config.shards for
   * format of the hosts string
   */
  @VisibleForTesting
  @NotNull
  List<String> getHosts(final String hosts) {
    return Arrays.stream(hosts.split(","))
        .map(String::trim)
        .map(hostAndPort -> {
          String[] splitHostAndPort = hostAndPort.split("/");
          return new ServerAddress(splitHostAndPort[splitHostAndPort.length - 1]).getHost();
        })
        .distinct()
        .collect(Collectors.toList());
  }
}
