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

import static com.mongodb.spark.sql.connector.read.ResumeTokenOffset.INITIAL_RESUME_TOKEN_OFFSET;
import static java.lang.String.format;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.PartitionOffset;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.schema.BsonDocumentToRowConverter;

/**
 * MongoContinuousStream defines how to read a stream of data from MongoDB.
 *
 * <p>Utilizes MongoDBs change stream functionality, the continuous streams will consist of <a
 * href="https://docs.mongodb.com/manual/reference/change-events/">change events</a>.
 *
 * <p>Note: Requires MongoDB 4.2+ To support continuing a change stream after a collection has been
 * dropped.
 */
public class MongoContinuousStream implements ContinuousStream {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoContinuousStream.class);
  private final BsonDocumentToRowConverter bsonDocumentToRowConverter;
  private final ReadConfig readConfig;

  /**
   * Construct a new instance
   *
   * @param schema the schema for the data
   * @param readConfig the read configuration
   */
  public MongoContinuousStream(final StructType schema, final ReadConfig readConfig) {
    Assertions.validateConfig(
        schema,
        (s) -> !s.isEmpty(),
        () -> "Mongo Continuous streams require a schema to be defined");
    this.bsonDocumentToRowConverter = new BsonDocumentToRowConverter(schema);
    this.readConfig = readConfig;
  }

  @Override
  public InputPartition[] planInputPartitions(final Offset start) {
    return new InputPartition[] {
      new MongoInputPartition(
          0,
          readConfig.getAggregationPipeline(),
          new ResumeTokenPartitionOffset(((ResumeTokenOffset) start).getResumeToken()))
    };
  }

  @Override
  public ContinuousPartitionReaderFactory createContinuousReaderFactory() {
    return new MongoStreamPartitionReaderFactory(bsonDocumentToRowConverter, readConfig);
  }

  @Override
  public Offset mergeOffsets(final PartitionOffset[] offsets) {
    Assertions.ensureState(
        () -> offsets.length == 1, () -> "Multiple offsets found when there should only be one.");
    Assertions.ensureState(
        () -> offsets[0] instanceof ResumeTokenPartitionOffset,
        () ->
            format(
                "Unexpected partition offset type. "
                    + "Expected ResumeTokenPartitionOffset` found `%s`",
                offsets[0].getClass()));
    return new ResumeTokenOffset(((ResumeTokenPartitionOffset) offsets[0]).getResumeToken());
  }

  @Override
  public Offset initialOffset() {
    return INITIAL_RESUME_TOKEN_OFFSET;
  }

  @Override
  public Offset deserializeOffset(final String json) {
    return ResumeTokenOffset.parse(json);
  }

  @Override
  public void commit(final Offset end) {
    LOGGER.info("ContinuousStream commit: {}", end);
  }

  @Override
  public void stop() {
    LOGGER.info("ContinuousStream stopped.");
  }
}
