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

package com.mongodb.spark.sql.connector.read.partitioner;

import static java.util.Collections.singletonList;

import com.mongodb.spark.sql.connector.annotations.ThreadSafe;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.read.MongoInputPartition;
import java.util.List;

/**
 * Single Partition Partitioner
 *
 * <p>Creates a single partition and includes any user provided aggregation pipelines.
 */
@ThreadSafe
public final class SinglePartitionPartitioner implements Partitioner {
  /** Construct an instance */
  public SinglePartitionPartitioner() {}

  @Override
  public List<MongoInputPartition> generatePartitions(final ReadConfig readConfig) {
    return singletonList(new MongoInputPartition(
        0,
        readConfig.getAggregationPipeline(),
        PartitionerHelper.getPreferredLocations(readConfig)));
  }
}
