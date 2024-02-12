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

import static com.mongodb.spark.sql.connector.read.ResumeTokenTimestampHelper.getTimestamp;

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
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A partition reader returned by {@link
 * MongoMicroBatchPartitionReaderFactory#createReader(org.apache.spark.sql.connector.read.InputPartition)}.
 *
 * <p>Utilizes MongoDBs change stream functionality, each micro batch stream will consist of <a
 * href="https://docs.mongodb.com/manual/reference/change-events/">change events</a>.
 */
final class MongoMicroBatchPartitionReader implements PartitionReader<InternalRow> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(MongoMicroBatchPartitionReader.class);
  private static final String FULL_DOCUMENT = "fullDocument";
  private final MongoMicroBatchInputPartition partition;
  private final BsonDocumentToRowConverter bsonDocumentToRowConverter;
  private final ReadConfig readConfig;
  private final MongoClient mongoClient;
  private volatile boolean closed = false;
  private MongoChangeStreamCursor<BsonDocument> changeStreamCursor;
  private InternalRow currentRow;

  /**
   * Construct a new instance
   *
   * @param partition the partition
   * @param bsonDocumentToRowConverter the converter from {@link BsonDocument} to {@link
   *     InternalRow}
   * @param readConfig the read configuration for reading from the partition
   */
  MongoMicroBatchPartitionReader(
      final MongoMicroBatchInputPartition partition,
      final BsonDocumentToRowConverter bsonDocumentToRowConverter,
      final ReadConfig readConfig) {

    this.partition = partition;
    this.bsonDocumentToRowConverter = bsonDocumentToRowConverter;
    this.readConfig = readConfig;
    this.mongoClient = readConfig.getMongoClient();
    LOGGER.info(
        "Creating partition reader for: PartitionId: {} with Schema: {}",
        partition.getPartitionId(),
        bsonDocumentToRowConverter.getSchema());
  }

  /**
   * Proceed to next record, returns false if there is no more records.
   *
   * <p>Enter a busy loop until one of the following scenarios exits the loop:
   *
   * <ul>
   *   <li>There is a result returned from {@code cursor.tryNext()}.
   *   <li>The cursor is closed. Happens when the collection is dropped.
   *   <li>The postBatchResumeToken's timestamp from the change stream is after the
   *       endOffsetTimestamp. Indicating that newer events are in the oplog but none within this
   *       time period for this collection.
   * </ul>
   */
  @Override
  public boolean next() {
    Assertions.ensureState(() -> !closed, () -> "Cannot call next() on a closed PartitionReader.");

    MongoChangeStreamCursor<BsonDocument> cursor = getCursor();
    BsonDocument cursorNext;

    do {
      try {
        cursorNext = cursor.tryNext();
      } catch (RuntimeException e) {
        throw new MongoSparkException("Calling `cursor.tryNext()` errored.", e);
      }
    } while (cursorNext == null
        && cursor.getServerCursor() != null
        && (cursor.getResumeToken() == null
            || getTimestamp(cursor.getResumeToken()).compareTo(partition.getEndOffsetTimestamp())
                < 0));

    boolean hasNext = cursorNext != null;
    if (hasNext) {
      if (readConfig.streamPublishFullDocumentOnly()) {
        cursorNext = cursorNext.getDocument(FULL_DOCUMENT, new BsonDocument());
      }
      currentRow = bsonDocumentToRowConverter.toInternalRow(cursorNext);
    }
    return hasNext;
  }

  /** Return the current record. This method should return same value until `next` is called. */
  @Override
  public InternalRow get() {
    Assertions.ensureState(() -> !closed, () -> "Cannot call get() on a closed PartitionReader.");
    return currentRow;
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      if (changeStreamCursor != null) {
        LOGGER.debug("Closing cursor for partitionId: {}", partition.getPartitionId());
        try {
          changeStreamCursor.close();
        } catch (Exception e) {
          // Ignore
        } finally {
          changeStreamCursor = null;
        }
      }
      mongoClient.close();
    }
  }

  /**
   * The change stream cursor has includes bounds for start and end offsets.
   *
   * <p>The time stored in the resumeToken and the clusterTime are equal. `startAtOperationTime` is
   * used to match the lower bounds. `clusterTime` is used to match the upper bounds.
   *
   * <p>If the configured start offset value less than zero it is ignored. Meaning the cursor will
   * start at the end of the oplog.
   *
   * @return the change stream cursor
   */
  private MongoChangeStreamCursor<BsonDocument> getCursor() {
    if (changeStreamCursor == null) {
      List<BsonDocument> pipeline = new ArrayList<>();
      pipeline.add(Aggregates.match(Filters.lt("clusterTime", partition.getEndOffsetTimestamp()))
          .toBsonDocument());
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
      if (partition.getStartOffsetTimestamp().getTime() >= 0) {
        changeStreamIterable.startAtOperationTime(partition.getStartOffsetTimestamp());
      }

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
}
