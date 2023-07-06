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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.schema.BsonDocumentToRowConverter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A partition reader returned by {@link
 * MongoBatchPartitionReaderFactory#createReader(org.apache.spark.sql.connector.read.InputPartition)}.
 * It's responsible for outputting data for a RDD partition.
 */
class MongoBatchPartitionReader implements PartitionReader<InternalRow> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoBatchPartitionReader.class);
  private final MongoInputPartition partition;
  private final BsonDocumentToRowConverter bsonDocumentToRowConverter;
  private final ReadConfig readConfig;

  private boolean closed = false;
  private MongoClient mongoClient;
  private MongoCursor<BsonDocument> mongoCursor;
  private InternalRow currentRow;

  /**
   * Construct a new instance
   *
   * @param partition the partition
   * @param bsonDocumentToRowConverter the converter from {@link BsonDocument} to {@link
   *     InternalRow}
   * @param readConfig the read configuration for reading from the partition
   */
  MongoBatchPartitionReader(
      final MongoInputPartition partition,
      final BsonDocumentToRowConverter bsonDocumentToRowConverter,
      final ReadConfig readConfig) {
    this.partition = partition;
    this.bsonDocumentToRowConverter = bsonDocumentToRowConverter;
    this.readConfig = readConfig;
    LOGGER.debug(
        "Creating partition reader for: PartitionId: {} with Schema: {}",
        partition.getPartitionId(),
        bsonDocumentToRowConverter.getSchema());
  }

  /** Proceed to next record, returns false if there is no more records. */
  @Override
  public boolean next() {
    Assertions.ensureState(() -> !closed, () -> "Cannot call next() on a closed PartitionReader.");
    boolean hasNext = getCursor().hasNext();
    if (hasNext) {
      currentRow = bsonDocumentToRowConverter.toInternalRow(getCursor().next());
    }
    return hasNext;
  }

  /** Return the current record. This method should return same value until `next` is called. */
  @Override
  public InternalRow get() {
    Assertions.ensureState(() -> !closed, () -> "Cannot call get() on a closed PartitionReader.");
    return currentRow;
  }

  /**
   * Closes this stream and releases any system resources associated with it. If the stream is
   * already closed then invoking this method has no effect.
   */
  @Override
  public void close() {
    if (!closed) {
      closed = true;
      releaseCursor();
    }
  }

  private MongoCursor<BsonDocument> getCursor() {
    if (mongoCursor == null) {
      LOGGER.debug("Opened cursor for partitionId: {}", partition.getPartitionId());
      mongoClient = readConfig.getMongoClient();
      mongoCursor = mongoClient
          .getDatabase(readConfig.getDatabaseName())
          .getCollection(readConfig.getCollectionName(), BsonDocument.class)
          .aggregate(partition.getPipeline())
          .allowDiskUse(readConfig.getAggregationAllowDiskUse())
          .comment(readConfig.getComment())
          .cursor();
    }
    return mongoCursor;
  }

  private void releaseCursor() {
    if (mongoCursor != null) {
      LOGGER.debug("Closing cursor for partitionId: {}", partition.getPartitionId());
      try {
        mongoCursor.close();
      } finally {
        mongoCursor = null;
        try {
          mongoClient.close();
        } finally {
          mongoClient = null;
        }
      }
    }
  }
}
