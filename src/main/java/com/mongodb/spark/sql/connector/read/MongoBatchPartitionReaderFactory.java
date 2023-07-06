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

import static java.lang.String.format;

import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.schema.BsonDocumentToRowConverter;
import java.io.Serializable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

/** A factory used to create {@link MongoBatchPartitionReader} instances. */
final class MongoBatchPartitionReaderFactory implements PartitionReaderFactory, Serializable {
  private static final long serialVersionUID = 1L;
  private final BsonDocumentToRowConverter bsonDocumentToRowConverter;
  private final ReadConfig readConfig;

  /**
   * Construct a new instance
   *
   * @param bsonDocumentToRowConverter the bson document to internal row converter
   * @param readConfig the read configuration
   */
  MongoBatchPartitionReaderFactory(
      final BsonDocumentToRowConverter bsonDocumentToRowConverter, final ReadConfig readConfig) {
    this.bsonDocumentToRowConverter = bsonDocumentToRowConverter;
    this.readConfig = readConfig;
  }

  /**
   * Returns a row-based partition reader to read data from the given {@link MongoInputPartition}.
   *
   * @param partition the input partition information
   */
  @Override
  public PartitionReader<InternalRow> createReader(final InputPartition partition) {
    Assertions.ensureState(
        () -> partition instanceof MongoInputPartition,
        () -> format(
            "Unsupported InputPartition type, a MongoInputPartition instance is required. Got: %s",
            partition.getClass()));
    return new MongoBatchPartitionReader(
        (MongoInputPartition) partition, bsonDocumentToRowConverter, readConfig);
  }
}
