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

import com.mongodb.spark.sql.connector.config.WriteConfig;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.types.StructType;

/** The factory responsible for creating the write operations for the batch or streaming write. */
final class MongoDataWriterFactory implements DataWriterFactory, StreamingDataWriterFactory {

  static final long serialVersionUID = 1L;
  private final StructType schema;
  private final WriteConfig writeConfig;

  /**
   * Construct a new instance
   *
   * @param schema the schema for the writer
   * @param writeConfig the configuration for the write
   */
  MongoDataWriterFactory(final StructType schema, final WriteConfig writeConfig) {
    this.schema = schema;
    this.writeConfig = writeConfig;
  }

  /**
   * Creates the {@link MongoDataWriter} that performs part of the write.
   *
   * @param partitionId A unique id of the RDD partition that the returned writer will process.
   * @param taskId The task id returned by {@link org.apache.spark.TaskContext#taskAttemptId()}.
   */
  @Override
  public DataWriter<InternalRow> createWriter(final int partitionId, final long taskId) {
    return new MongoDataWriter(partitionId, taskId, schema, writeConfig, -1);
  }

  /**
   * Creates the {@link MongoDataWriter} that performs part of the streaming write.
   *
   * @param partitionId A unique id of the RDD partition that the returned writer will process.
   * @param taskId The task id returned by {@link TaskContext#taskAttemptId()}.
   * @param epochId A monotonically increasing id for streaming queries that are split into discrete
   *     periods of execution.
   */
  @Override
  public DataWriter<InternalRow> createWriter(
      final int partitionId, final long taskId, final long epochId) {
    return new MongoDataWriter(partitionId, taskId, schema, writeConfig, epochId);
  }
}
