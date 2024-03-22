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

import com.mongodb.spark.sql.connector.config.CollectionsConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.ConfigException;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;

/** A logical representation of MongoDB data source scan. */
final class MongoScan implements Scan {
  private final StructType schema;
  private final ReadConfig readConfig;

  /**
   * Construct a new instance
   *
   * @param schema the schema for the data
   * @param readConfig the read configuration
   */
  MongoScan(final StructType schema, final ReadConfig readConfig) {
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
    return "MongoScan{" + "namespaceDescription=" + readConfig.getNamespaceDescription() + '}';
  }

  /**
   * Returns the physical representation of this scan for batch query.
   * This mode does not support scanning
   * {@linkplain com.mongodb.spark.sql.connector.config.CollectionsConfig.Type#MULTIPLE multiple} or
   * {@linkplain com.mongodb.spark.sql.connector.config.CollectionsConfig.Type#ALL all} collections.
   *
   * @throws ConfigException
   * If either {@linkplain CollectionsConfig.Type#MULTIPLE multiple} or {@linkplain CollectionsConfig.Type#ALL all}
   * collections are {@linkplain ReadConfig#getCollectionsConfig() configured} to be {@linkplain Scan scanned}.
   */
  @Override
  public Batch toBatch() {
    if (readConfig.getCollectionsConfig().getType() != CollectionsConfig.Type.SINGLE) {
      throw new ConfigException(format(
          "The connector is configured to access %s, which is not supported by batch queries",
          readConfig.getNamespaceDescription()));
    }
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
   */
  @Override
  public MicroBatchStream toMicroBatchStream(final String checkpointLocation) {
    return new MongoMicroBatchStream(schema, checkpointLocation, readConfig);
  }

  /**
   * Returns the physical representation of this scan for streaming query with continuous mode.
   *
   * <p>Utilizes MongoDBs change stream functionality, the continuous streams will consist of <a
   * href="https://docs.mongodb.com/manual/reference/change-events/">change events</a>.
   *
   * <p>Note: Requires MongoDB 4.2+ To support continuing a change stream after a collection has
   * been dropped.
   */
  @Override
  public ContinuousStream toContinuousStream(final String checkpointLocation) {
    return new MongoContinuousStream(schema, checkpointLocation, readConfig);
  }
}
