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

import static java.lang.String.format;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;
import java.io.Serializable;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

/**
 * Manages resume token persistence in a sidecar collection.
 *
 * <p>Only instantiated when {@code ReadLimit.maxRows(n)} is configured.
 * The sidecar collection acts as a communication channel between the Executor
 * (which writes the last-seen resume token) and the Driver (which reads it
 * to determine the starting offset for the next micro batch).
 *
 * <p>All documents are scoped by a {@code checkpointHash} derived from the
 * checkpoint location, which prevents collisions when multiple streams read
 * from the same database and share the same sidecar collection.
 */
final class MicroBatchResumeTokenStore implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final String RESUME_TOKEN_FIELD = "resumeToken";
  private static final String PARTITION_ID_FIELD = "partitionId";
  private static final String CHECKPOINT_HASH_FIELD = "checkpointHash";
  private static final String TIMESTAMP_FIELD = "updatedAt";

  private final ReadConfig readConfig;
  private final String collectionName;
  private final String checkpointHash;

  MicroBatchResumeTokenStore(final ReadConfig readConfig, final String checkpointLocation) {
    this.readConfig = readConfig;
    this.collectionName = readConfig.getMicroBatchMaxRowsOffsetCollection();
    this.checkpointHash = Long.toHexString(hash64(checkpointLocation));
  }

  String getCollectionName() {
    return collectionName;
  }

  void storeResumeToken(final int partitionId, final BsonDocument resumeToken) {
    withCollection(
        coll -> coll.replaceOne(
            Filters.and(
                Filters.eq(CHECKPOINT_HASH_FIELD, checkpointHash),
                Filters.eq(PARTITION_ID_FIELD, partitionId)),
            new BsonDocument()
                .append(CHECKPOINT_HASH_FIELD, new BsonString(checkpointHash))
                .append(PARTITION_ID_FIELD, new BsonInt32(partitionId))
                .append(RESUME_TOKEN_FIELD, resumeToken)
                .append(TIMESTAMP_FIELD, new BsonDateTime(System.currentTimeMillis())),
            new ReplaceOptions().upsert(true)),
        () -> format(
            "Failed to persist resume token for partition %d. Ensure the user has permissions to read / write to '%s.%s'",
            partitionId, readConfig.getDatabaseName(), collectionName));
  }

  Optional<BsonDocument> getLatestResumeToken() {
    BsonDocument result = withCollection(
        coll -> coll.find(Filters.eq(CHECKPOINT_HASH_FIELD, checkpointHash))
            .sort(Sorts.descending(TIMESTAMP_FIELD))
            .first(),
        () -> format(
            "Failed to get latest resume token. Ensure the user has permissions to read / write to '%s.%s'",
            readConfig.getDatabaseName(), collectionName));
    if (result == null) {
      return Optional.empty();
    }
    return Optional.of(result.getDocument(RESUME_TOKEN_FIELD));
  }

  void cleanup() {
    withCollection(
        coll -> coll.deleteMany(Filters.eq(CHECKPOINT_HASH_FIELD, checkpointHash)),
        () -> format(
            "Failed to cleanup data from '%s.%s'. All fields matching: {\"%s\": %s} can be removed.",
            readConfig.getDatabaseName(), collectionName, CHECKPOINT_HASH_FIELD, checkpointHash));
  }

  private <T> T withCollection(
      final Function<MongoCollection<BsonDocument>, T> function,
      final Supplier<String> exceptionMessageSupplier) {
    try (MongoClient client = readConfig.getMongoClient()) {
      return function.apply(client
          .getDatabase(readConfig.getDatabaseName())
          .getCollection(getCollectionName(), BsonDocument.class));
    } catch (RuntimeException e) {
      throw new MongoSparkException(exceptionMessageSupplier.get(), e);
    }
  }

  /** FNV-1a 64-bit hash. */
  private static long hash64(final String input) {
    long fnvPrime = 0x100000001b3L;
    long hash = 0xcbf29ce484222325L; // FNV offset basis 64-bit
    for (int i = 0; i < input.length(); i++) {
      hash ^= input.charAt(i);
      hash *= fnvPrime;
    }
    return hash;
  }
}
