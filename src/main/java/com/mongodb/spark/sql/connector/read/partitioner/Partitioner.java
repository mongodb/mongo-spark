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

import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.read.MongoInputPartition;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Partitioner provides the logic to partition a collection individual processable partitions.
 *
 * <p>The Partitioner must provide unique partitions without any duplicates or overlapping
 * partitions.
 */
public interface Partitioner {

  Logger LOGGER = LoggerFactory.getLogger(Partitioner.class);

  /**
   * Generate the partitions for the collection based upon the read configuration
   *
   * @param readConfig the read configuration
   * @return the partitions
   */
  List<MongoInputPartition> generatePartitions(ReadConfig readConfig);
}
