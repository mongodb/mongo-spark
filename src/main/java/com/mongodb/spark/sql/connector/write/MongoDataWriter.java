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

/** The MongoDB writer that writes the input RDD partition into MongoDB. */
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
   * Converts the record into a {@link BsonDocument} and stages the write.
   *
   * <p>Once {@link WriteConfig#getMaxBatchSize} is hit then the bulk operation takes place.
   *
   * @param record the row to be written
   * @see WriteConfig#getMaxBatchSize
   */
  @Override
  public void write(final InternalRow record) throws IOException {
    BsonDocument bsonDocument = rowToBsonDocumentConverter.fromRow(record);
    writeModelList.add(getWriteModel(bsonDocument));
    writeModels(writeConfig.getMaxBatchSize());
  }

  /**
   * Commits this writer after all records are written successfully.
   *
   * <p>Ensures any remain writes in the batch are written.
   *
   * @return a MongoWriterCommitMessage
   */
  @Override
  public WriterCommitMessage commit() {
    writeModels(1);
    return new MongoWriterCommitMessage(partitionId, taskId, epochId);
  }

  /**
   * Aborts the write.
   *
   * <p>Note: Data is not cleaned up and will require manual cleaning.
   */
  @Override
  public void abort() {
    releaseClient();
  }

  @Override
  public void close() {
    releaseClient();
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
