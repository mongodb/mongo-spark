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

import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import com.mongodb.ServerAddress;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.read.MongoInputPartition;

/**
 * Sharded Partitioner
 *
 * <p>Uses the chunks collection and partitions the collection based on the sharded collections
 * chunk ranges.
 *
 * <p>Partitions the collection using:
 *
 * <ul>
 *   <li>{@value SHARD_KEY_FIELD_LIST_CONFIG}: A comma delimited list of the shard keys for the
 *       collection. Defaults to: _id`.
 * </ul>
 *
 * <p><strong>Note:</strong> Does not support collections sharded using hashed shard keys.
 */
@ApiStatus.Internal
public final class ShardedPartitioner implements Partitioner {

  public static final String SHARD_KEY_FIELD_LIST_CONFIG = "shard.key.field.list";
  public static final List<String> SHARD_KEY_FIELD_LIST_DEFAULT = singletonList("_id");

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
    List<String> shardKeys = getShardKeys(readConfig);

    LOGGER.info(
        "Getting shard chunk bounds for '{}' with shard key: {}",
        readConfig.getNamespace().getFullName(),
        shardKeys);

    BsonDocument configCollectionMetadata =
        readConfig.withClient(
            client ->
                client
                    .getDatabase(CONFIG_DATABASE)
                    .getCollection(CONFIG_COLLECTIONS, BsonDocument.class)
                    .find(Filters.eq(ID_FIELD, readConfig.getNamespace().getFullName()))
                    .projection(Projections.include("_id", "timestamp", "uuid", "dropped"))
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

    BsonDocument chunksMatchPredicate =
        configCollectionMetadata.containsKey("timestamp")
            ? new BsonDocument(UUID_FIELD, configCollectionMetadata.get(UUID_FIELD))
            : new BsonDocument(NAMESPACE_FIELD, configCollectionMetadata.get(ID_FIELD));

    List<BsonDocument> chunks =
        readConfig.withClient(
            client ->
                client
                    .getDatabase(CONFIG_DATABASE)
                    .getCollection(CONFIG_CHUNKS, BsonDocument.class)
                    .find(chunksMatchPredicate)
                    .projection(CHUNKS_PROJECTIONS)
                    .sort(SORTS)
                    .into(new ArrayList<>()));

    List<MongoInputPartition> partitions =
        createMongoInputPartitions(shardKeys, chunks, readConfig);

    if (partitions.isEmpty()) {
      LOGGER.warn(
          "There is no chunk information for '{}' using a single partition",
          readConfig.getNamespace().getFullName());
      return new SinglePartitionPartitioner().generatePartitions(readConfig);
    }
    return partitions;
  }

  @VisibleForTesting
  List<String> getShardKeys(final ReadConfig readConfig) {
    return readConfig
        .getPartitionerOptions()
        .getList(SHARD_KEY_FIELD_LIST_CONFIG, SHARD_KEY_FIELD_LIST_DEFAULT);
  }

  @NotNull
  private List<MongoInputPartition> createMongoInputPartitions(
      final List<String> shardKeys, final List<BsonDocument> chunks, final ReadConfig readConfig) {
    Map<String, List<String>> shardMap = createShardMap(readConfig);

    return IntStream.range(0, chunks.size())
        .mapToObj(
            i -> {
              BsonDocument chunkDocument = chunks.get(i);
              BsonDocument min = chunkDocument.getDocument("min");
              BsonDocument max = chunkDocument.getDocument("max");
              BsonDocument partitionBounds = new BsonDocument();

              shardKeys.forEach(
                  shardKey -> {
                    BsonDocument shardKeyBoundary =
                        PartitionerHelper.createPartitionBounds(
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
        .collect(Collectors.toList());
  }

  @NotNull
  private Map<String, List<String>> createShardMap(final ReadConfig readConfig) {
    return readConfig.withClient(
        client ->
            client
                .getDatabase(CONFIG_DATABASE)
                .getCollection(CONFIG_SHARDS, BsonDocument.class)
                .find()
                .projection(SHARDS_PROJECTIONS)
                .into(new ArrayList<>())
                .stream()
                .collect(
                    Collectors.toMap(
                        s -> s.getString(ID_FIELD).getValue(),
                        s -> getHosts(s.getString(HOST_FIELD).getValue()))));
  }

  @VisibleForTesting
  @NotNull
  List<String> getHosts(final String hosts) {
    return Arrays.stream(hosts.split(","))
        .map(String::trim)
        .map(
            hostAndPort -> {
              String[] splitHostAndPort = hostAndPort.split("/");
              return new ServerAddress(splitHostAndPort[splitHostAndPort.length - 1]).getHost();
            })
        .distinct()
        .collect(Collectors.toList());
  }
}
