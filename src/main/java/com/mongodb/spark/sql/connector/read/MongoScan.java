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
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;

import com.mongodb.spark.sql.connector.config.ReadConfig;

/** A logical representation of MongoDB data source scan. */
public class MongoScan implements Scan {

  private final StructType schema;
  private final ReadConfig readConfig;

  /**
   * Construct a new instance
   *
   * @param schema the schema for the data
   * @param readConfig the read configuration
   */
  public MongoScan(final StructType schema, final ReadConfig readConfig) {
    this.schema = schema;
    this.readConfig = readConfig;
  }

  /**
   * Returns the actual schema of this data source scan, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   */
  @Override
  public StructType readSchema() {
    return schema;
  }

  /** A description string of this scan. */
  @Override
  public String description() {
    return "MongoScan{" + "namespace=" + readConfig.getNamespace().toString() + '}';
  }

  /** Returns the physical representation of this scan for batch query. */
  @Override
  public Batch toBatch() {
    return new MongoBatch(schema, readConfig);
  }

  /**
   * Returns the physical representation of this scan for streaming query with micro mode.
   *
   * <p>Utilizes MongoDBs change stream functionality, the continuous streams will consist of <a
   * href="https://docs.mongodb.com/manual/reference/change-events/">change events</a>.
   *
   * <p>Note: Requires MongoDB 4.2+ To support continuing a change stream after a collection has
   * been dropped.
   *
   * @param checkpointLocation check point locations are not supported
   */
  @Override
  public MicroBatchStream toMicroBatchStream(final String checkpointLocation) {
    return new MongoMicroBatchStream(schema, readConfig);
  }

  /**
   * Returns the physical representation of this scan for streaming query with continuous mode.
   *
   * <p>Utilizes MongoDBs change stream functionality, the continuous streams will consist of <a
   * href="https://docs.mongodb.com/manual/reference/change-events/">change events</a>.
   *
   * <p>Note: Requires MongoDB 4.2+ To support continuing a change stream after a collection has
   * been dropped.
   *
   * @param checkpointLocation check point locations are not supported
   */
  @Override
  public ContinuousStream toContinuousStream(final String checkpointLocation) {
    return new MongoContinuousStream(schema, readConfig);
  }
}
