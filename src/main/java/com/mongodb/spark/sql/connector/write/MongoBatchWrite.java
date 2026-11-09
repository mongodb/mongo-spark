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

import com.mongodb.spark.sql.connector.config.WriteConfig;
import com.mongodb.spark.sql.connector.exceptions.DataException;
import java.util.Arrays;
import java.util.Objects;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** MongoBatchWrite defines how to write the data to MongoDB when batch processing. */
final class MongoBatchWrite implements BatchWrite {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoBatchWrite.class);
  private final LogicalWriteInfo info;
  private final WriteConfig writeConfig;
  private final boolean truncate;
  private OverwriteStaging overwrite;

  /**
   * Construct a new instance
   *
   * @param info the logical write information
   * @param writeConfig the configuration for the write
   * @param truncate truncate the table
   */
  MongoBatchWrite(
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
  public DataWriterFactory createBatchWriterFactory(final PhysicalWriteInfo physicalWriteInfo) {
    return writerFactory();
  }

  /**
   * Publishes a staged overwrite, if this write is replacing the target collection.
   *
   * @param messages WriterCommitMessage
   */
  @Override
  public void commit(final WriterCommitMessage[] messages) {
    publishOverwrite();
    LOGGER.debug("Write committed for: {}, with {} task(s).", info.queryId(), messages.length);
  }

  /**
   * The write was aborted due to a failure.
   *
   * <p>A staged overwrite that has not yet been published is dropped. The target collection is left
   * as it was.
   *
   * @param messages the WriterCommitMessages
   * @throws DataException with information regarding the failed write
   */
  @Override
  public void abort(final WriterCommitMessage[] messages) {
    long tasksCompleted = Arrays.stream(messages).filter(Objects::nonNull).count();
    throw new DataException(format(
        "Write aborted for: %s. %s/%s tasks completed.%s",
        info.queryId(), tasksCompleted, messages.length, discardOverwrite()));
  }

  private DataWriterFactory writerFactory() {
    WriteConfig taskConfig = writeConfig;
    if (truncate) {
      overwrite = OverwriteStaging.prepare(writeConfig, info.queryId());
      taskConfig = overwrite.stagedConfig();
    }
    return new MongoDataWriterFactory(info.schema(), taskConfig, info.queryId());
  }

  private void publishOverwrite() {
    if (overwrite != null) {
      overwrite.publish();
      overwrite = null;
    }
  }

  private String discardOverwrite() {
    if (overwrite == null) {
      return "";
    }
    overwrite.discard();
    String message = overwrite.abortMessage();
    overwrite = null;
    return message;
  }
}
