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

import static com.mongodb.spark.sql.connector.read.ResumeTokenPartitionOffset.INITIAL_RESUME_TOKEN_PARTITION_OFFSET;
import static java.util.Collections.emptyList;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.spark.sql.connector.read.InputPartition;

import org.bson.BsonDocument;

/**
 * The MongoInputPartition.
 *
 * <p>Provides the meta information regarding a partition of a collection
 */
public class MongoInputPartition implements InputPartition {
  private static final long serialVersionUID = 1L;

  private final int partitionId;
  private final List<BsonDocument> pipeline;
  private final List<String> preferredLocations;
  private final ResumeTokenPartitionOffset resumeTokenPartitionOffset;

  /**
   * Construct a new instance
   *
   * @param partitionId the id of the partition
   * @param pipeline the pipeline to partition the collection
   */
  public MongoInputPartition(final int partitionId, final List<BsonDocument> pipeline) {
    this(partitionId, pipeline, emptyList());
  }

  /**
   * Construct a new instance
   *
   * @param partitionId the id of the partition
   * @param pipeline the pipeline to partition the collection
   * @param resumeTokenPartitionOffset the resume token offset for the partition
   */
  public MongoInputPartition(
      final int partitionId,
      final List<BsonDocument> pipeline,
      final ResumeTokenPartitionOffset resumeTokenPartitionOffset) {
    this(partitionId, pipeline, emptyList(), resumeTokenPartitionOffset);
  }

  /**
   * Construct a new instance
   *
   * @param partitionId the id of the partition
   * @param pipeline the pipeline to partition the collection
   * @param preferredLocations the preferred server locations
   */
  public MongoInputPartition(
      final int partitionId,
      final List<BsonDocument> pipeline,
      final List<String> preferredLocations) {
    this(partitionId, pipeline, preferredLocations, INITIAL_RESUME_TOKEN_PARTITION_OFFSET);
  }

  MongoInputPartition(
      final int partitionId,
      final List<BsonDocument> pipeline,
      final List<String> preferredLocations,
      final ResumeTokenPartitionOffset resumeTokenPartitionOffset) {
    this.partitionId = partitionId;
    this.pipeline = pipeline;
    this.preferredLocations = preferredLocations;
    this.resumeTokenPartitionOffset = resumeTokenPartitionOffset;
  }

  /** @return the partition id */
  public int getPartitionId() {
    return partitionId;
  }

  /** @return the resume token offset */
  public ResumeTokenPartitionOffset getResumeTokenPartitionOffset() {
    return resumeTokenPartitionOffset;
  }

  /** @return the aggregation pipeline for the partition */
  public List<BsonDocument> getPipeline() {
    return pipeline;
  }

  /**
   * The preferred locations for the read.
   *
   * <p>This may be the hostname of an individual mongos shard host or just the locations of the
   * MongoDB cluster.
   */
  @Override
  public String[] preferredLocations() {
    return preferredLocations.toArray(new String[0]);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final MongoInputPartition that = (MongoInputPartition) o;
    return partitionId == that.partitionId
        && Objects.equals(pipeline, that.pipeline)
        && Objects.equals(preferredLocations, that.preferredLocations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionId, pipeline, preferredLocations);
  }

  @Override
  public String toString() {
    return "MongoInputPartition{"
        + "partitionId="
        + partitionId
        + ", pipeline="
        + pipeline.stream().map(BsonDocument::toJson).collect(Collectors.joining(",", "[", "]"))
        + ", preferredLocations="
        + preferredLocations
        + ", resumeTokenPartitionOffset="
        + resumeTokenPartitionOffset
        + ", preferredLocations="
        + preferredLocations
        + '}';
  }
}
