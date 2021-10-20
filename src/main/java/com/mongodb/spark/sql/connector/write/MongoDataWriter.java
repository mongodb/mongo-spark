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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

import com.mongodb.spark.sql.connector.config.WriteConfig;
import com.mongodb.spark.sql.connector.exceptions.DataException;
import com.mongodb.spark.sql.connector.schema.RowToBsonDocumentConverter;

/**
 * A MongoDataWriter returned by {@link MongoDataWriterFactory#createWriter(int, long)} and is
 * responsible for writing data for an input RDD partition.
 *
 * <p>One Spark task has one exclusive data writer, so there is no thread-safe concern.
 *
 * <p>{@link #write(InternalRow)} is called for each record in the input RDD partition. If one
 * record fails the {@link #write(InternalRow)}, {@link #abort()} is called afterwards and the
 * remaining records will not be processed. If all records are successfully written, {@link
 * #commit()} is called.
 *
 * <p>Once a data writer returns successfully from {@link #commit()} or {@link #abort()}, Spark will
 * call {@link #close()} to let DataWriter doing resource cleanup. After calling {@link #close()},
 * its lifecycle is over and Spark will not use it again.
 *
 * <p>If this data writer succeeds(all records are successfully written and {@link #commit()}
 * succeeds), a {@link WriterCommitMessage} will be sent to the driver side and pass to {@link
 * MongoBatchWrite#commit(WriterCommitMessage[])} with commit messages from other data writers. If
 * this data writer fails(one record fails to write or {@link #commit()} fails), an exception will
 * be sent to the driver side, and Spark may retry this writing task a few times. In each retry,
 * {@link MongoDataWriterFactory#createWriter(int, long)} will receive a different `taskId`. Spark
 * will call {@link MongoBatchWrite#abort(WriterCommitMessage[])} when the configured number of
 * retries is exhausted.
 *
 * <p>Besides the retry mechanism, Spark may launch speculative tasks if the existing writing task
 * takes too long to finish. Different from retried tasks, which are launched one by one after the
 * previous one fails, speculative tasks are running simultaneously. It's possible that one input
 * RDD partition has multiple data writers with different `taskId` running at the same time, and
 * data sources should guarantee that these data writers don't conflict and can work together.
 * Implementations can coordinate with driver during {@link #commit()} to make sure only one of
 * these data writers can commit successfully. Or implementations can allow all of them to commit
 * successfully, and have a way to revert committed data writers without the commit message, because
 * Spark only accepts the commit message that arrives first and ignore others.
 */
class MongoDataWriter implements DataWriter<InternalRow> {

  private final int partitionId;
  private final long taskId;
  private final RowToBsonDocumentConverter rowToBsonDocumentConverter;
  private final WriteConfig writeConfig;
  private final long epochId;
  private final BulkWriteOptions bulkWriteOptions;
  private final List<WriteModel<BsonDocument>> writeModelList = new ArrayList<>();

  private MongoClient mongoClient;

  /**
   * Construct a new instance
   *
   * @param partitionId A unique id of the RDD partition that the returned writer will process.
   *     Usually Spark processes many RDD partitions at the same time, implementations should use
   *     the partition id to distinguish writers for different partitions.
   * @param taskId The task id returned by {@link org.apache.spark.TaskContext#taskAttemptId()}.
   * @param writeConfig the MongoDB write configuration
   */
  MongoDataWriter(
      final int partitionId,
      final long taskId,
      final RowToBsonDocumentConverter rowToBsonDocumentConverter,
      final WriteConfig writeConfig,
      final long epochId) {
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.rowToBsonDocumentConverter = rowToBsonDocumentConverter;
    this.writeConfig = writeConfig;
    this.epochId = epochId;
    this.bulkWriteOptions = new BulkWriteOptions().ordered(writeConfig.isOrdered());
  }

  /**
   * Write
   *
   * <p>Converts a record to a {@link BsonDocument} and adds it to a list for staging a write. Then
   * checks to see if the bulkWrite should occur.
   *
   * <p>If this method fails (by throwing an exception), {@link #abort()} will be called and this
   * data writer is considered to have been failed.
   *
   * @param record the row to be written
   */
  @Override
  public void write(final InternalRow record) throws IOException {
    BsonDocument bsonDocument = rowToBsonDocumentConverter.fromRow(record);
    writeModelList.add(getWriteModel(bsonDocument));
    writeModels(writeConfig.getMaxBatchSize());
  }

  private WriteModel<BsonDocument> getWriteModel(final BsonDocument bsonDocument) {
    if (!hasIdFields(bsonDocument)) {
      return new InsertOneModel<>(bsonDocument);
    }

    switch (writeConfig.getOperationType()) {
      case INSERT:
        return new InsertOneModel<>(bsonDocument);
      case REPLACE:
        return new ReplaceOneModel<>(
            getIdFieldDocument(bsonDocument), bsonDocument, new ReplaceOptions().upsert(true));
      case UPDATE:
        BsonDocument idFields = getIdFieldDocument(bsonDocument);
        idFields.keySet().forEach(bsonDocument::remove);
        BsonDocument setDocument = new BsonDocument("$set", bsonDocument);
        return new UpdateOneModel<>(idFields, setDocument, new UpdateOptions().upsert(true));
      default:
        throw new DataException("Unsupported operation type: " + writeConfig.getOperationType());
    }
  }

  private boolean hasIdFields(final BsonDocument bsonDocument) {
    return bsonDocument.keySet().containsAll(writeConfig.getIdFields());
  }

  private BsonDocument getIdFieldDocument(final BsonDocument bsonDocument) {
    BsonDocument idFields = new BsonDocument();
    writeConfig
        .getIdFields()
        .forEach(
            k -> {
              BsonValue v = bsonDocument.get(k);
              if (v == null) {
                throw new DataException(
                    format("Missing id field: '%s' from: %s", k, bsonDocument.toJson()));
              }
              idFields.append(k, v);
            });
    return idFields;
  }

  /**
   * Commits this writer after all records are written successfully, returns a commit message which
   * will be sent back to driver side and passed to {@link
   * MongoBatchWrite#commit(WriterCommitMessage[])}.
   *
   * <p>The written data should only be visible to data source readers after {@link
   * MongoBatchWrite#commit(WriterCommitMessage[])} succeeds, which means this method should still
   * "hide" the written data and ask the {@link MongoBatchWrite} at driver side to do the final
   * commit via {@link WriterCommitMessage}.
   *
   * <p>If this method fails (by throwing an exception), {@link #abort()} will be called and this
   * data writer is considered to have been failed.
   */
  @Override
  public WriterCommitMessage commit() {
    writeModels(1);
    return new MongoWriterCommitMessage(partitionId, taskId, epochId);
  }

  /**
   * Aborts this writer if it is failed. Implementations should clean up the data for already
   * written records.
   *
   * <p>This method will only be called if there is one record failed to write, or {@link #commit()}
   * failed.
   *
   * <p>If this method fails(by throwing an exception), the underlying data source may have garbage
   * that need to be cleaned by {@link MongoBatchWrite#abort(WriterCommitMessage[])} or manually,
   * but these garbage should not be visible to data source readers.
   */
  @Override
  public void abort() {
    releaseClient();
  }

  @Override
  public void close() {
    releaseClient();
  }

  private MongoClient getMongoClient() {
    if (mongoClient == null) {
      mongoClient = writeConfig.getMongoClient();
    }
    return mongoClient;
  }

  private void releaseClient() {
    if (mongoClient != null) {
      mongoClient.close();
      mongoClient = null;
    }
  }

  private void writeModels(final int minQueueSize) {
    if (writeModelList.size() >= minQueueSize) {
      getMongoClient()
          .getDatabase(writeConfig.getDatabaseName())
          .getCollection(writeConfig.getCollectionName(), BsonDocument.class)
          .withWriteConcern(writeConfig.getWriteConcern())
          .bulkWrite(writeModelList, bulkWriteOptions);
      writeModelList.clear();
    }
  }
}
