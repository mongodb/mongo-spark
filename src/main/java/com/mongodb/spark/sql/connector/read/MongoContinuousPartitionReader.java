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

import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.config.CollectionsConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;
import com.mongodb.spark.sql.connector.schema.BsonDocumentToRowConverter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReader;
import org.apache.spark.sql.connector.read.streaming.PartitionOffset;
import org.bson.BsonDocument;
import org.bson.Document;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A partition reader returned by {@link
 * MongoContinuousPartitionReaderFactory#createReader(org.apache.spark.sql.connector.read.InputPartition)}.
 *
 * <p>Utilizes MongoDBs change stream functionality, the continuous stream will consist of <a
 * href="https://docs.mongodb.com/manual/reference/change-events/">change events</a>.
 */
final class MongoContinuousPartitionReader implements ContinuousPartitionReader<InternalRow> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(MongoContinuousPartitionReader.class);
  private static final String FULL_DOCUMENT = "fullDocument";
  private final MongoInputPartition partition;
  private final BsonDocumentToRowConverter bsonDocumentToRowConverter;
  private final ReadConfig readConfig;
  private MongoContinuousInputPartitionOffset lastOffset;
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
  MongoContinuousPartitionReader(
      final MongoContinuousInputPartition partition,
      final BsonDocumentToRowConverter bsonDocumentToRowConverter,
      final ReadConfig readConfig) {
    this.partition = partition;
    this.bsonDocumentToRowConverter = bsonDocumentToRowConverter;

    this.readConfig = readConfig;
    this.currentRow = null;
    this.lastOffset = partition.getPartitionOffset();

    LOGGER.debug(
        "Creating partition reader for: Partition: {} with Schema: {}",
        partition,
        bsonDocumentToRowConverter);
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

    /*
     * Returning false from next() causes an:
     * `IllegalStateException("Continuous reader reported no elements! Reader should have blocked waiting.")
     *
     * So we block until a result is available or the stream is cancelled
     */
    boolean hasNext = false;
    while (!hasNext) {
      hasNext = tryNext();
    }
    return true;
  }

  @Override
  public InternalRow get() {
    Assertions.ensureState(() -> !closed, () -> "Cannot call get() on a closed PartitionReader.");
    Assertions.ensureState(
        () -> currentRow != null,
        () -> "Current row is null, this should not happen if `next()` returns true.");
    LOGGER.trace("Get called: {}", currentRow);
    return currentRow;
  }

  @Override
  public void close() {
    LOGGER.info("Closing the stream partition reader.");
    if (!closed) {
      closed = true;
      releaseCursorAndClient();
    }
  }

  private boolean tryNext() {
    return withCursor(c -> {
      try {
        if (c.hasNext()) {
          BsonDocument next = c.next();
          if (next.containsKey("_id") && next.isDocument("_id")) {
            setLastOffset(next.getDocument("_id"));
          }
          if (readConfig.streamPublishFullDocumentOnly()) {
            next = next.getDocument(FULL_DOCUMENT, new BsonDocument());
          }
          currentRow = bsonDocumentToRowConverter.toInternalRow(next);
          return true;
        }
        setLastOffset(c.getResumeToken());
      } catch (MongoException e) {
        LOGGER.info("Trying to get more data from the change stream failed, releasing cursor.", e);
      }
      releaseCursorAndClient();
      currentRow = null;
      return false;
    });
  }

  private void setLastOffset(@Nullable final BsonDocument resumeToken) {
    if (resumeToken != null) {
      lastOffset = new MongoContinuousInputPartitionOffset(new ResumeTokenBasedOffset(resumeToken));
    }
  }

  private MongoChangeStreamCursor<BsonDocument> getCursor() {
    if (mongoClient == null) {
      mongoClient = readConfig.getMongoClient();
    }
    if (changeStreamCursor == null) {
      List<BsonDocument> pipeline = new ArrayList<>();
      if (readConfig.streamPublishFullDocumentOnly()) {
        pipeline.add(Aggregates.match(Filters.exists(FULL_DOCUMENT)).toBsonDocument());
      }
      pipeline.addAll(partition.getPipeline());
      ChangeStreamIterable<Document> changeStreamIterable;
      if (readConfig.getCollectionsConfig().getType() == CollectionsConfig.Type.SINGLE) {
        changeStreamIterable = mongoClient
            .getDatabase(readConfig.getDatabaseName())
            .getCollection(readConfig.getCollectionName())
            .watch(pipeline);
      } else {
        changeStreamIterable =
            mongoClient.getDatabase(readConfig.getDatabaseName()).watch(pipeline);
      }
      changeStreamIterable
          .fullDocument(readConfig.getStreamFullDocument())
          .comment(readConfig.getComment());
      changeStreamIterable = lastOffset.applyToChangeStreamIterable(changeStreamIterable);

      try {
        changeStreamCursor = (MongoChangeStreamCursor<BsonDocument>)
            changeStreamIterable.withDocumentClass(BsonDocument.class).cursor();
        LOGGER.debug("Opened change stream cursor for partition: {}", partition);
      } catch (RuntimeException e) {
        throw new MongoSparkException("Could not create the change stream cursor.", e);
      }
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
      releaseCursor();
      throw new MongoSparkException("Change stream cursor failure.", e);
    }
  }

  private void releaseCursorAndClient() {
    try {
      releaseCursor();
    } catch (RuntimeException e) {
      // ignore
    }
    try {
      releaseClient();
    } catch (RuntimeException e) {
      // ignore
    }
  }

  private void releaseCursor() {
    if (changeStreamCursor != null) {
      LOGGER.debug("Releasing change stream cursor for partition: {}", partition);
      try {
        changeStreamCursor.close();
      } finally {
        changeStreamCursor = null;
      }
    }
  }

  private void releaseClient() {
    if (changeStreamCursor != null) {
      LOGGER.debug("Releasing mongoClient for partition: {}", partition);
      try {
        mongoClient.close();
      } finally {
        mongoClient = null;
      }
    }
  }
}
