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

import com.mongodb.client.MongoCollection;
import com.mongodb.spark.sql.connector.config.WriteConfig;
import com.mongodb.spark.sql.connector.exceptions.DataException;
import java.util.Arrays;
import java.util.Objects;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** MongoStreamingWrite defines how to write the data to MongoDB when streaming data. */
final class MongoStreamingWrite implements StreamingWrite {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoStreamingWrite.class);
  private final LogicalWriteInfo info;
  private final WriteConfig writeConfig;
  private final boolean truncate;

  /**
   * Construct a new instance
   *
   * @param info the logical write information
   * @param writeConfig the configuration for the write
   * @param truncate truncate the table
   */
  MongoStreamingWrite(
      final LogicalWriteInfo info, final WriteConfig writeConfig, final boolean truncate) {
    this.info = info;
    this.writeConfig = writeConfig;
    this.truncate = truncate;
  }

  /**
   * Creates the MongoDataWriterFactory instance will be serialized and sent to executors.
   *
   * @param physicalWriteInfo Physical information about the input data that will be written to this
   *     table.
   */
  @Override
  public StreamingDataWriterFactory createStreamingWriterFactory(
      final PhysicalWriteInfo physicalWriteInfo) {
    if (truncate) {
      writeConfig.doWithCollection(MongoCollection::drop);
    }
    return new MongoDataWriterFactory(info.schema(), writeConfig);
  }

  /**
   * Logs the that the write has been committed
   *
   * @param epochId the epoch id for the write
   * @param messages WriterCommitMessages
   */
  @Override
  public void commit(final long epochId, final WriterCommitMessage[] messages) {
    LOGGER.debug("Write committed for: {}, with {} task(s).", info.queryId(), messages.length);
  }

  /**
   * The write was aborted due to a failure.
   *
   * <p>There is no automatic clean up, so the database state is undetermined.
   *
   * @param epochId the epoch id for the write
   * @param messages the WriterCommitMessages
   * @throws DataException with information regarding the failed write
   */
  @Override
  public void abort(final long epochId, final WriterCommitMessage[] messages) {
    long tasksCompleted = Arrays.stream(messages).filter(Objects::nonNull).count();
    throw new DataException(format(
        "Write aborted for: %s. %s/%s tasks completed. EpochId: %s",
        info.queryId(), tasksCompleted, messages.length, epochId));
  }
}
