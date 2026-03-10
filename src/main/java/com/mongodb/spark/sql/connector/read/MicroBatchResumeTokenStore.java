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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;
import java.io.Serializable;
import java.util.Optional;
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
    this.checkpointHash = Integer.toHexString(checkpointLocation.hashCode());
  }

  String getCollectionName() {
    return collectionName;
  }

  void storeResumeToken(final int partitionId, final BsonDocument resumeToken) {
    try {
      getCollection()
          .replaceOne(
              Filters.and(
                  Filters.eq(CHECKPOINT_HASH_FIELD, checkpointHash),
                  Filters.eq(PARTITION_ID_FIELD, partitionId)),
              new BsonDocument()
                  .append(CHECKPOINT_HASH_FIELD, new BsonString(checkpointHash))
                  .append(PARTITION_ID_FIELD, new BsonInt32(partitionId))
                  .append(RESUME_TOKEN_FIELD, resumeToken)
                  .append(TIMESTAMP_FIELD, new BsonDateTime(System.currentTimeMillis())),
              new ReplaceOptions().upsert(true));
    } catch (RuntimeException e) {
      throw new MongoSparkException(
          format(
              "Failed to persist resume token for partition %d. Ensure the user has permissions to read / write to '%s.%s'",
              partitionId, readConfig.getDatabaseName(), collectionName),
          e);
    }
  }

  Optional<BsonDocument> getLatestResumeToken() {
    BsonDocument result = null;
    try {
      result = getCollection()
          .find(Filters.eq(CHECKPOINT_HASH_FIELD, checkpointHash))
          .sort(Sorts.descending(TIMESTAMP_FIELD))
          .first();
    } catch (RuntimeException e) {
      throw new MongoSparkException(
          format(
              "Failed to get latest resume token. Ensure the user has permissions to read / write to '%s.%s'",
              readConfig.getDatabaseName(), collectionName),
          e);
    }
    if (result == null) {
      return Optional.empty();
    }
    return Optional.of(result.getDocument(RESUME_TOKEN_FIELD));
  }

  void cleanup() {
    try {
      getCollection().deleteMany(Filters.eq(CHECKPOINT_HASH_FIELD, checkpointHash));
    } catch (RuntimeException e) {
      throw new MongoSparkException(
          format(
              "Failed to cleanup data from '%s.%s'. All fields matching: {\"%s\": %s} can be removed.",
              readConfig.getDatabaseName(), collectionName, CHECKPOINT_HASH_FIELD, checkpointHash),
          e);
    }
  }

  private MongoCollection<BsonDocument> getCollection() {
    return readConfig
        .getMongoClient()
        .getDatabase(readConfig.getDatabaseName())
        .getCollection(collectionName, BsonDocument.class);
  }
}
