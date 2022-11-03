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

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;

import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;
import com.mongodb.spark.sql.connector.schema.BsonDocumentToRowConverter;

/**
 * A partition reader returned by {@link
 * MongoPartitionReaderFactory#createReader(org.apache.spark.sql.connector.read.InputPartition)}.
 * It's responsible for outputting data for a RDD partition.
 */
public class MongoMicroBatchPartitionReader implements PartitionReader<InternalRow> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(MongoMicroBatchPartitionReader.class);
  private static final String FULL_DOCUMENT = "fullDocument";
  private final MongoMicroBatchInputPartition partition;
  private final BsonDocumentToRowConverter bsonDocumentToRowConverter;
  private final ReadConfig readConfig;
  private boolean closed = false;
  private MongoClient mongoClient;
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
  public MongoMicroBatchPartitionReader(
      final MongoMicroBatchInputPartition partition,
      final BsonDocumentToRowConverter bsonDocumentToRowConverter,
      final ReadConfig readConfig) {

    this.partition = partition;
    this.bsonDocumentToRowConverter = bsonDocumentToRowConverter;
    this.readConfig = readConfig;
    LOGGER.info(
        "Creating partition reader for: PartitionId: {} with Schema: {}",
        partition.getPartitionId(),
        bsonDocumentToRowConverter.getSchema());
  }

  /** Proceed to next record, returns false if there is no more records. */
  @Override
  public boolean next() {
    Assertions.ensureState(() -> !closed, () -> "Cannot call next() on a closed PartitionReader.");
    MongoChangeStreamCursor<BsonDocument> cursor = getCursor();
    BsonDocument next = cursor.tryNext();
    if (next != null) {
      if (readConfig.streamPublishFullDocumentOnly()) {
        next = next.getDocument(FULL_DOCUMENT, new BsonDocument());
      }
      currentRow = bsonDocumentToRowConverter.toInternalRow(next);
    }
    return next != null;
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
      releaseCursor();
    }
  }

  private MongoChangeStreamCursor<BsonDocument> getCursor() {
    if (mongoClient == null) {
      mongoClient = readConfig.getMongoClient();
    }
    if (changeStreamCursor == null) {

      List<BsonDocument> pipeline = new ArrayList<>();
      pipeline.add(
          Aggregates.match(Filters.lt("clusterTime", partition.getEndOffsetTimestamp()))
              .toBsonDocument());

      if (readConfig.streamPublishFullDocumentOnly()) {
        pipeline.add(Aggregates.match(Filters.exists(FULL_DOCUMENT)).toBsonDocument());
      }
      pipeline.addAll(partition.getPipeline());

      ChangeStreamIterable<Document> changeStreamIterable =
          mongoClient
              .getDatabase(readConfig.getDatabaseName())
              .getCollection(readConfig.getCollectionName())
              .watch(pipeline)
              .fullDocument(readConfig.getStreamFullDocument())
              .startAtOperationTime(partition.getStartAtOperationTime());

      try {
        changeStreamCursor =
            (MongoChangeStreamCursor<BsonDocument>)
                changeStreamIterable.withDocumentClass(BsonDocument.class).cursor();
        LOGGER.debug("Opened change stream cursor for partition: {}", partition);
      } catch (RuntimeException e) {
        throw new MongoSparkException("Could not create the change stream cursor.", e);
      }
    }
    return changeStreamCursor;
  }

  private void releaseCursor() {
    if (changeStreamCursor != null) {
      LOGGER.debug("Closing cursor for partitionId: {}", partition.getPartitionId());
      try {
        changeStreamCursor.close();
      } finally {
        changeStreamCursor = null;
        try {
          mongoClient.close();
        } finally {
          mongoClient = null;
        }
      }
    }
  }
}
