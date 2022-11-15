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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.spark.sql.execution.streaming.LongOffset;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

/**
 * The MongoMicroBatchInputPartition.
 *
 * <p>Provides the meta information regarding a partition of a collection
 */
public class MongoMicroBatchInputPartition extends MongoInputPartition {
  private static final long serialVersionUID = 1L;

  private final LongOffset startOffset;
  private final LongOffset endOffset;

  /**
   * Construct a new instance
   *
   * @param partitionId the id of the partition
   * @param pipeline the pipeline to partition the collection
   * @param startOffset the start offset in seconds since epoch
   * @param endOffset the end offset in seconds since epoch
   */
  public MongoMicroBatchInputPartition(
      final int partitionId,
      final List<BsonDocument> pipeline,
      final LongOffset startOffset,
      final LongOffset endOffset) {
    super(partitionId, pipeline);
    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }

  /** @return the bson timestamp at the start offset */
  public BsonTimestamp getStartOffsetTimestamp() {
    return new BsonTimestamp((int) startOffset.offset(), 0);
  }

  /** @return the bson timestamp at end offset */
  public BsonTimestamp getEndOffsetTimestamp() {
    return new BsonTimestamp((int) endOffset.offset(), 0);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final MongoMicroBatchInputPartition that = (MongoMicroBatchInputPartition) o;
    return Objects.equals(startOffset, that.startOffset)
        && Objects.equals(endOffset, that.endOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), startOffset, endOffset);
  }

  @Override
  public String toString() {
    return "MongoMicroBatchInputPartition{"
        + "partitionId="
        + getPartitionId()
        + ", pipeline="
        + getPipeline().stream()
            .map(BsonDocument::toJson)
            .collect(Collectors.joining(",", "[", "]"))
        + ", preferredLocations="
        + Arrays.toString(preferredLocations())
        + ", startOffset="
        + startOffset
        + ", endOffset="
        + endOffset
        + "} ";
  }
}
