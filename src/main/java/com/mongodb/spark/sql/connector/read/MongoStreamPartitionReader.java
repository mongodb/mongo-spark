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

import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.function.Function;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReader;
import org.apache.spark.sql.connector.read.streaming.PartitionOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;

import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;

import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;
import com.mongodb.spark.sql.connector.schema.BsonDocumentToRowConverter;

/**
 * A partition reader returned by {@link
 * MongoStreamPartitionReaderFactory#createReader(org.apache.spark.sql.connector.read.InputPartition)}.
 *
 * <p>It's responsible for outputting data for a continuous stream using change streams.
 */
public class MongoStreamPartitionReader implements ContinuousPartitionReader<InternalRow> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoPartitionReader.class);
  private static final String INVALIDATE = "invalidate";
  private static final ResumeTokenPartitionOffset EMPTY_RESUME_TOKEN_PARTITION_OFFSET =
      new ResumeTokenPartitionOffset(new BsonDocument());
  private final MongoInputPartition partition;
  private final BsonDocumentToRowConverter bsonDocumentToRowConverter;
  private final ReadConfig readConfig;

  private ResumeTokenPartitionOffset lastOffset;
  private InternalRow currentRow;
  private boolean closed = false;
  private MongoClient mongoClient;
  private MongoChangeStreamCursor<BsonDocument> changeStreamCursor;

  /**
   * Construct a new instance
   *
   * @param partition the partition
   * @param bsonDocumentToRowConverter the converter from {@link BsonDocument} to {@link
   *     InternalRow}
   * @param readConfig the read configuration for reading from the partition
   */
  public MongoStreamPartitionReader(
      final MongoInputPartition partition,
      final BsonDocumentToRowConverter bsonDocumentToRowConverter,
      final ReadConfig readConfig) {
    this.partition = partition;
    this.bsonDocumentToRowConverter = bsonDocumentToRowConverter;

    this.readConfig = readConfig;
    this.currentRow = null;
    this.lastOffset =
        new ResumeTokenPartitionOffset(partition.getResumeTokenOffset().getResumeToken());

    LOGGER.debug(
        "Creating partition reader for: PartitionId: {} with Schema: {}",
        partition.getPartitionId(),
        bsonDocumentToRowConverter.getSchema());
  }

  @Override
  public PartitionOffset getOffset() {
    Assertions.ensureState(
        () -> !closed, () -> "Cannot call getOffset() on a closed PartitionReader.");
    LOGGER.trace("getOffset called, returning: {}", lastOffset);
    return lastOffset;
  }

  @Override
  public boolean next() {
    Assertions.ensureState(() -> !closed, () -> "Cannot call next() on a closed PartitionReader.");
    return withCursor(
        c -> {
          try {
            BsonDocument next = c.next();
            lastOffset = new ResumeTokenPartitionOffset(c.getResumeToken());
            if (next.getString("operationType", new BsonString(""))
                .getValue()
                .toLowerCase(Locale.ROOT)
                .equals(INVALIDATE)) {
              LOGGER.info(
                  "Change stream cursor has been invalidated. This happens when a collection is dropped. Closing cursor.");
              lastOffset = EMPTY_RESUME_TOKEN_PARTITION_OFFSET;
              releaseCursor();
              releaseClient();
            }
            currentRow = bsonDocumentToRowConverter.toInternalRow(next);
          } catch (NoSuchElementException e) {
            LOGGER.warn("No such element! {}", e.getMessage());
            return false;
          }
          return true;
        });
  }

  @Override
  public InternalRow get() {
    Assertions.ensureState(() -> !closed, () -> "Cannot call get() on a closed PartitionReader.");
    Assertions.ensureState(
        () -> currentRow != null,
        () -> "Current row is null, this should not happen if `next()` returns false.");
    LOGGER.trace("Get called: {}", currentRow);
    return currentRow;
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      releaseCursor();
      releaseClient();
    }
  }

  private MongoChangeStreamCursor<BsonDocument> getCursor() {
    if (mongoClient == null) {
      mongoClient = readConfig.getMongoClient();
    }
    if (changeStreamCursor == null) {
      LOGGER.info("Opened change stream cursor for partitionId: {}", partition.getPartitionId());
      ChangeStreamIterable<Document> changeStreamIterable =
          mongoClient
              .getDatabase(readConfig.getDatabaseName())
              .getCollection(readConfig.getCollectionName())
              .watch(partition.getPipeline());

      if (!lastOffset.getResumeToken().isEmpty()) {
        changeStreamIterable = changeStreamIterable.resumeAfter(lastOffset.getResumeToken());
      }
      changeStreamCursor =
          (MongoChangeStreamCursor<BsonDocument>)
              changeStreamIterable.withDocumentClass(BsonDocument.class).cursor();
    }
    return changeStreamCursor;
  }

  private <T> T withCursor(
      final Function<MongoChangeStreamCursor<BsonDocument>, T> cursorConsumer) {
    try {
      return cursorConsumer.apply(getCursor());
    } catch (MongoInterruptedException e) {
      releaseCursor();
      throw new MongoSparkException("Change stream cursor interrupted.");
    } catch (MongoException e) {
      LOGGER.info("Cursor failure: {}", e.getMessage());
      releaseCursor();
      throw new MongoSparkException("Change stream cursor failure.", e);
    }
  }

  private void releaseCursor() {
    if (changeStreamCursor != null) {
      LOGGER.debug(
          "Releasing change stream cursor for partitionId: {}", partition.getPartitionId());
      try {
        changeStreamCursor.close();
      } finally {
        changeStreamCursor = null;
      }
    }
  }

  private void releaseClient() {
    if (changeStreamCursor != null) {
      LOGGER.debug("Releasing mongoClient for partitionId: {}", partition.getPartitionId());
      try {
        mongoClient.close();
      } finally {
        mongoClient = null;
      }
    }
  }
}
