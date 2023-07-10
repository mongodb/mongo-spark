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
import org.bson.BsonDocument;

/**
 * The MongoContinuousInputPartition.
 *
 * <p>Provides the meta information regarding a partition of a collection
 */
final class MongoContinuousInputPartition extends MongoInputPartition {
  private static final long serialVersionUID = 1L;

  private final MongoContinuousInputPartitionOffset partitionOffset;

  /**
   * Construct a new instance
   *
   * @param partitionId the id of the partition
   * @param pipeline the pipeline to partition the collection
   * @param partitionOffset the resume token offset for the partition
   */
  MongoContinuousInputPartition(
      final int partitionId,
      final List<BsonDocument> pipeline,
      final MongoContinuousInputPartitionOffset partitionOffset) {
    super(partitionId, pipeline);
    this.partitionOffset = partitionOffset;
  }

  /** @return the resume token offset */
  MongoContinuousInputPartitionOffset getPartitionOffset() {
    return partitionOffset;
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
    final MongoContinuousInputPartition that = (MongoContinuousInputPartition) o;
    return Objects.equals(partitionOffset, that.partitionOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), partitionOffset);
  }

  @Override
  public String toString() {
    return "MongoContinuousInputPartition{"
        + "partitionId="
        + getPartitionId()
        + ", pipeline="
        + getPipeline().stream()
            .map(BsonDocument::toJson)
            .collect(Collectors.joining(",", "[", "]"))
        + ", preferredLocations="
        + Arrays.toString(preferredLocations())
        + "partitionOffset="
        + partitionOffset
        + "} ";
  }
}
