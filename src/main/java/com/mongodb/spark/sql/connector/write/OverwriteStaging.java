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

package com.mongodb.spark.sql.connector.write;

import static java.lang.String.format;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.WriteConfig;
import com.mongodb.spark.sql.connector.exceptions.DataException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds an overwrite in a staging collection until the Spark driver commits it.
 *
 * <p>Tasks write the staging collection. {@link #publish()} runs on the driver after every task
 * succeeds. {@link #discard()} drops the staging collection when the job fails before the target
 * has been modified, so a failed overwrite leaves the previous collection in place.
 */
final class OverwriteStaging {
  static final String COLLECTION_PREFIX = "__spark_staging_";

  private static final Logger LOGGER = LoggerFactory.getLogger(OverwriteStaging.class);

  private final WriteConfig targetConfig;
  private final WriteConfig stagedConfig;
  private boolean targetMutated;

  private OverwriteStaging(final WriteConfig targetConfig, final WriteConfig stagedConfig) {
    this.targetConfig = targetConfig;
    this.stagedConfig = stagedConfig;
  }

  /**
   * Drops any previous staging collection for this query and creates an empty one.
   *
   * @param targetConfig configuration of the collection Spark is overwriting
   * @param queryId Spark write query id. Stable for one batch query, and for every epoch of a
   *     streaming query.
   * @return the staging collection tasks should write
   */
  static OverwriteStaging prepare(final WriteConfig targetConfig, final String queryId) {
    WriteConfig stagedConfig =
        targetConfig.withOption(MongoConfig.COLLECTION_NAME_CONFIG, collectionName(queryId));
    stagedConfig.doWithClient(client -> {
      MongoDatabase database = client.getDatabase(stagedConfig.getDatabaseName());
      database.getCollection(stagedConfig.getCollectionName()).drop();
      database.createCollection(stagedConfig.getCollectionName());
    });
    LOGGER.debug(
        "Prepared staging collection {} for overwrite of {}.",
        stagedConfig.getNamespace().getFullName(),
        targetConfig.getNamespace().getFullName());
    return new OverwriteStaging(targetConfig, stagedConfig);
  }

  WriteConfig stagedConfig() {
    return stagedConfig;
  }

  /**
   * Replaces the target with the staged documents.
   *
   * <p>{@code drop} renames the staging collection over the target, which is atomic for an
   * unsharded target and removes the previous indexes and options. {@code truncate} deletes the
   * target documents and copies the staged documents in, so indexes, collation, and sharding stay.
   * That copy is not atomic: a failure after the delete keeps the staging collection for recovery.
   */
  void publish() {
    MongoNamespace target = targetConfig.getNamespace();
    MongoNamespace staging = stagedConfig.getNamespace();
    LOGGER.debug(
        "Publishing staged overwrite of {} from {}.", target.getFullName(), staging.getFullName());
    switch (targetConfig.truncateMode()) {
      case DROP:
        renameOverTarget(target);
        break;
      case TRUNCATE:
        replaceContents(target);
        break;
      default:
        throw new DataException("Unsupported truncate mode: " + targetConfig.truncateMode());
    }
  }

  /**
   * Drops the staging collection when the target was not modified. A commit that already changed
   * the target leaves the staging collection in place and records that in {@link #abortMessage()}.
   */
  void discard() {
    if (targetMutated) {
      LOGGER.warn(
          "Overwrite of {} modified the target before it failed. Left staging collection {} in place.",
          targetConfig.getNamespace().getFullName(),
          stagedConfig.getNamespace().getFullName());
      return;
    }
    stagedConfig.doWithCollection(MongoCollection::drop);
  }

  String abortMessage() {
    if (targetMutated) {
      return format(
          " The target collection was modified during commit. Staging collection %s was kept for recovery.",
          stagedConfig.getNamespace().getFullName());
    }
    return " The staged overwrite was discarded and the target collection was left unchanged.";
  }

  private void renameOverTarget(final MongoNamespace target) {
    try {
      stagedConfig.doWithCollection(collection ->
          collection.renameCollection(target, new RenameCollectionOptions().dropTarget(true)));
    } catch (MongoCommandException ex) {
      if (!shardedDropTargetRejected(ex)) {
        throw new DataException(
            format(
                "Unable to publish overwrite of %s from %s: %s",
                target.getFullName(),
                stagedConfig.getNamespace().getFullName(),
                ex.getErrorMessage()),
            ex);
      }
      // renameCollection rejects dropTarget when the target is sharded. Drop first, then rename.
      // A crash between the two leaves the replacement data in the staging collection.
      targetMutated = true;
      targetConfig.doWithCollection(MongoCollection::drop);
      stagedConfig.doWithCollection(collection -> collection.renameCollection(target));
    }
  }

  private void replaceContents(final MongoNamespace target) {
    targetConfig.doWithCollection(collection ->
        collection.withWriteConcern(targetConfig.getWriteConcern()).deleteMany(new BsonDocument()));
    targetMutated = true;
    // Copy with insertMany. $merge rejects collections whose collation has no matching unique
    // index, which is the normal _id index on a collection created with a collation.
    stagedConfig.doWithClient(client -> {
      MongoDatabase database = client.getDatabase(target.getDatabaseName());
      MongoCollection<BsonDocument> staging =
          database.getCollection(stagedConfig.getCollectionName(), BsonDocument.class);
      MongoCollection<BsonDocument> destination = database
          .getCollection(target.getCollectionName(), BsonDocument.class)
          .withWriteConcern(targetConfig.getWriteConcern());
      List<BsonDocument> batch = new ArrayList<>();
      int batchSize = Math.max(1, targetConfig.getMaxBatchSize());
      for (BsonDocument document : staging.find()) {
        batch.add(document);
        if (batch.size() >= batchSize) {
          destination.insertMany(batch);
          batch.clear();
        }
      }
      if (!batch.isEmpty()) {
        destination.insertMany(batch);
      }
    });
    stagedConfig.doWithCollection(MongoCollection::drop);
  }

  private static boolean shardedDropTargetRejected(final MongoCommandException ex) {
    String message =
        ex.getErrorMessage() == null ? "" : ex.getErrorMessage().toLowerCase(Locale.ROOT);
    return message.contains("sharded");
  }

  static String collectionName(final String queryId) {
    String raw = queryId == null ? "" : queryId;
    String sanitized = raw.replaceAll("[^A-Za-z0-9_]", "_");
    String name = COLLECTION_PREFIX + sanitized;
    if (sanitized.isEmpty() || name.length() > 120) {
      name = COLLECTION_PREFIX + hex(sha256(raw)).substring(0, 32);
    }
    return name;
  }

  private static byte[] sha256(final String value) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
    } catch (NoSuchAlgorithmException e) {
      throw new DataException("Unable to name the overwrite staging collection.", e);
    }
  }

  private static String hex(final byte[] bytes) {
    StringBuilder hex = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      hex.append(String.format("%02x", b));
    }
    return hex.toString();
  }
}
