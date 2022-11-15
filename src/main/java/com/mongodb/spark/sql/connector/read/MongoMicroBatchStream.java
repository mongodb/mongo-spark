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

import java.time.Instant;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.schema.BsonDocumentToRowConverter;

/**
 * MongoMicroBatchStream defines how to read a stream of data from MongoDB.
 *
 * <p>Utilizes MongoDBs change stream functionality, the continuous streams will consist of <a
 * href="https://docs.mongodb.com/manual/reference/change-events/">change events</a>.
 *
 * <p>Note: Requires MongoDB 4.2+ To support continuing a change stream after a collection has been
 * dropped.
 *
 * <p>Uses seconds since epoch offsets to create boundaries between partitions.
 */
public class MongoMicroBatchStream implements MicroBatchStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoMicroBatchStream.class);
  private final BsonDocumentToRowConverter bsonDocumentToRowConverter;
  private final ReadConfig readConfig;
  private volatile Long lastTime = Instant.now().getEpochSecond();

  private int partitionId;

  /**
   * Construct a new instance
   *
   * @param schema the schema for the data
   * @param readConfig the read configuration
   */
  public MongoMicroBatchStream(final StructType schema, final ReadConfig readConfig) {
    Assertions.validateConfig(
        schema,
        (s) -> !s.isEmpty(),
        () -> "Mongo micro batch streams require a schema to be defined");
    this.bsonDocumentToRowConverter = new BsonDocumentToRowConverter(schema);
    this.readConfig = readConfig;
  }

  @Override
  public Offset latestOffset() {
    long now = Instant.now().getEpochSecond();
    if (lastTime < now) {
      lastTime = now;
    }
    return new LongOffset(lastTime);
  }

  @Override
  public InputPartition[] planInputPartitions(final Offset start, final Offset end) {
    return new InputPartition[] {
      new MongoMicroBatchInputPartition(
          partitionId++, readConfig.getAggregationPipeline(), (LongOffset) start, (LongOffset) end)
    };
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new MongoMicroBatchPartitionReaderFactory(bsonDocumentToRowConverter, readConfig);
  }

  @Override
  public Offset initialOffset() {
    return new LongOffset(-1);
  }

  @Override
  public Offset deserializeOffset(final String json) {
    return new LongOffset(Long.parseLong(json));
  }

  @Override
  public void commit(final Offset end) {
    LOGGER.info("MicroBatchStream commit: {}", end);
  }

  @Override
  public void stop() {
    LOGGER.info("MicroBatchStream stopped.");
  }
}
