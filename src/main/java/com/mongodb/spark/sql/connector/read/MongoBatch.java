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

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.read.partitioner.PartitionerHelper;
import com.mongodb.spark.sql.connector.schema.BsonDocumentToRowConverter;

/** MongoBatch defines how to read data from MongoDB. */
public class MongoBatch implements Batch {

  private final BsonDocumentToRowConverter bsonDocumentToRowConverter;
  private final ReadConfig readConfig;

  /**
   * Construct a new instance
   *
   * @param schema the schema for the data
   * @param readConfig the read configuration
   */
  public MongoBatch(final StructType schema, final ReadConfig readConfig) {
    this.bsonDocumentToRowConverter = new BsonDocumentToRowConverter(schema);
    this.readConfig = readConfig;
  }

  /** Returns a list of partitions that split the collection into parts */
  @Override
  public InputPartition[] planInputPartitions() {
    return PartitionerHelper.generatePartitions(readConfig);
  }

  /** Returns a factory to create a {@link PartitionReader} for each {@link InputPartition}. */
  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new MongoPartitionReaderFactory(bsonDocumentToRowConverter, readConfig);
  }
}
