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

import java.util.Arrays;
import java.util.Objects;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoCollection;

import com.mongodb.spark.sql.connector.config.WriteConfig;
import com.mongodb.spark.sql.connector.exceptions.DataException;
import com.mongodb.spark.sql.connector.schema.RowToBsonDocumentConverter;

/**
 * MongoBatchWrite defines how to write the data to data source for batch processing.
 *
 * <p>The writing procedure is:
 *
 * <ol>
 *   <li>Create a writer factory by {@link #createBatchWriterFactory(PhysicalWriteInfo)}, serialize
 *       and send it to all the partitions of the input data(RDD).
 *   <li>For each partition, create the data writer, and write the data of the partition with this
 *       writer. If all the data are written successfully, call {@link DataWriter#commit()}. If
 *       exception happens during the writing, call {@link DataWriter#abort()}.
 *   <li>If all writers are successfully committed, call {@link #commit(WriterCommitMessage[])}. If
 *       some writers are aborted, or the job failed with an unknown reason, call {@link
 *       #abort(WriterCommitMessage[])}.
 * </ol>
 *
 * <p>While Spark will retry failed writing tasks, Spark won't retry failed writing jobs. Users
 * should do it manually in their Spark applications if they want to retry.
 *
 * <p>Please refer to the documentation of commit/abort methods for detailed specifications.
 */
class MongoBatchWrite implements BatchWrite {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(MongoBatchWrite.class);
  private final LogicalWriteInfo info;
  private final WriteConfig writeConfig;
  private final RowToBsonDocumentConverter rowToBsonDocumentConverter;
  private final boolean truncate;

  /**
   * Construct a new instance
   *
   * @param info the logical write information
   * @param rowToBsonDocumentConverter the row to BsonDocument converter
   * @param writeConfig the configuration for the write
   * @param truncate truncate the table
   */
  MongoBatchWrite(
      final LogicalWriteInfo info,
      final RowToBsonDocumentConverter rowToBsonDocumentConverter,
      final WriteConfig writeConfig,
      final boolean truncate) {
    this.info = info;
    this.rowToBsonDocumentConverter = rowToBsonDocumentConverter;
    this.writeConfig = writeConfig;
    this.truncate = truncate;
  }

  /**
   * Creates a writer factory which will be serialized and sent to executors.
   *
   * <p>If this method fails (by throwing an exception), the action will fail and no Spark job will
   * be submitted.
   *
   * @param info Physical information about the input data that will be written to this table.
   */
  @Override
  public DataWriterFactory createBatchWriterFactory(final PhysicalWriteInfo info) {
    if (truncate) {
      writeConfig.doWithCollection(MongoCollection::drop);
    }
    return new MongoDataWriterFactory(rowToBsonDocumentConverter, writeConfig);
  }

  /**
   * Commits this writing job with a list of commit messages. The commit messages are collected from
   * successful data writers and are produced by {@link MongoDataWriter#commit()}.
   *
   * <p>If this method fails (by throwing an exception), this writing job is considered to have been
   * failed, and {@link #abort(WriterCommitMessage[])} would be called. The state of the destination
   * is undefined and @{@link #abort(WriterCommitMessage[])} may not be able to deal with it.
   *
   * @param messages WriterCommitMessage
   */
  @Override
  public void commit(final WriterCommitMessage[] messages) {
    LOGGER.info("Write committed for: {}, with {} task(s).", info.queryId(), messages.length);
  }

  /**
   * Aborts this writing job because some data writers are failed and keep failing when retry, or
   * the Spark job fails with some unknown reasons, or {@link
   * #onDataWriterCommit(WriterCommitMessage)} fails, or {@link #commit(WriterCommitMessage[])}
   * fails.
   *
   * <p>If this method fails (by throwing an exception), the underlying data source may require
   * manual cleanup.
   *
   * <p>Unless the abort is triggered by the failure of commit, the given messages should have some
   * null slots as there maybe only a few data writers that are committed before the abort happens,
   * or some data writers were committed but their commit messages haven't reached the driver when
   * the abort is triggered. So this is just a "best effort" for data sources to clean up the data
   * left by data writers.
   *
   * @param messages the WriterCommitMessages
   */
  @Override
  public void abort(final WriterCommitMessage[] messages) {
    long tasksCompleted = Arrays.stream(messages).filter(Objects::nonNull).count();
    throw new DataException(
        format(
            "Write aborted for: %s. %s/%s tasks completed.",
            info.queryId(), tasksCompleted, messages.length));
  }
}
