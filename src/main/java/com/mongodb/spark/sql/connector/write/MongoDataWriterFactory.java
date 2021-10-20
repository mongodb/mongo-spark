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

import java.io.Serializable;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;

import com.mongodb.spark.sql.connector.config.WriteConfig;
import com.mongodb.spark.sql.connector.schema.RowToBsonDocumentConverter;

/**
 * A factory of {@link MongoDataWriter} returned by {@link
 * MongoBatchWrite#createBatchWriterFactory(org.apache.spark.sql.connector.write.PhysicalWriteInfo)},
 * which is responsible for creating and initializing the actual data writer at executor side.
 *
 * <p>Note that, the writer factory will be serialized and sent to executors, then the data writer
 * will be created on executors and do the actual writing. So this interface must be serializable
 * and {@link MongoDataWriter} doesn't need to be.
 */
class MongoDataWriterFactory
    implements DataWriterFactory, StreamingDataWriterFactory, Serializable {

  static final long serialVersionUID = 1L;

  private final RowToBsonDocumentConverter rowToBsonDocumentConverter;
  private final WriteConfig writeConfig;

  /**
   * Construct a new instance
   *
   * @param rowToBsonDocumentConverter the row to BsonDocument converter
   * @param writeConfig the configuration for the write
   */
  MongoDataWriterFactory(
      final RowToBsonDocumentConverter rowToBsonDocumentConverter, final WriteConfig writeConfig) {
    this.rowToBsonDocumentConverter = rowToBsonDocumentConverter;
    this.writeConfig = writeConfig;
  }

  /**
   * Returns a data writer to do the actual writing work. Note that, Spark will reuse the same data
   * object instance when sending data to the data writer, for better performance. Data writers are
   * responsible for defensive copies if necessary, e.g. copy the data before buffer it in a list.
   *
   * <p>If this method fails (by throwing an exception), the corresponding Spark write task would
   * fail and get retried until hitting the maximum retry times.
   *
   * @param partitionId A unique id of the RDD partition that the returned writer will process.
   *     Usually Spark processes many RDD partitions at the same time, implementations should use
   *     the partition id to distinguish writers for different partitions.
   * @param taskId The task id returned by {@link org.apache.spark.TaskContext#taskAttemptId()}.
   */
  @Override
  public DataWriter<InternalRow> createWriter(final int partitionId, final long taskId) {
    return new MongoDataWriter(partitionId, taskId, rowToBsonDocumentConverter, writeConfig, -1);
  }

  /**
   * Returns a data writer to do the actual writing work. Note that, Spark will reuse the same data
   * object instance when sending data to the data writer, for better performance. Data writers are
   * responsible for defensive copies if necessary, e.g. copy the data before buffer it in a list.
   *
   * <p>If this method fails (by throwing an exception), the corresponding Spark write task would
   * fail and get retried until hitting the maximum retry times.
   *
   * @param partitionId A unique id of the RDD partition that the returned writer will process.
   *     Usually Spark processes many RDD partitions at the same time, implementations should use
   *     the partition id to distinguish writers for different partitions.
   * @param taskId The task id returned by {@link TaskContext#taskAttemptId()}. Spark may run
   *     multiple tasks for the same partition (due to speculation or task failures, for example).
   * @param epochId A monotonically increasing id for streaming queries that are split into discrete
   *     periods of execution.
   */
  @Override
  public DataWriter<InternalRow> createWriter(
      final int partitionId, final long taskId, final long epochId) {
    return new MongoDataWriter(
        partitionId, taskId, rowToBsonDocumentConverter, writeConfig, epochId);
  }
}
