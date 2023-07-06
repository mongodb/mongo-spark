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

import java.util.Objects;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

/** Represents a successful write to MongoDB */
final class MongoWriterCommitMessage implements WriterCommitMessage {
  private static final long serialVersionUID = 1L;

  private final int partitionId;
  private final long taskId;
  private final long epochId;

  /**
   * Construct a new instance
   *
   * @param partitionId A unique id of the RDD partition that the returned writer will process.
   * @param taskId The task id returned by {@link org.apache.spark.TaskContext#taskAttemptId()}.
   * @param epochId the epochId or -1 if not set
   */
  MongoWriterCommitMessage(final int partitionId, final long taskId, final long epochId) {
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.epochId = epochId;
  }

  @Override
  public String toString() {
    return "MongoWriterCommitMessage{"
        + "partitionId="
        + partitionId
        + ", taskId="
        + taskId
        + ", epochId="
        + epochId
        + '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final MongoWriterCommitMessage that = (MongoWriterCommitMessage) o;
    return partitionId == that.partitionId && taskId == that.taskId && epochId == that.epochId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionId, taskId, epochId);
  }
}
