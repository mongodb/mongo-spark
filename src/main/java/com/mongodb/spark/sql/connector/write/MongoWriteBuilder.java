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

package com.mongodb.spark.sql.connector.write;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

/**
 * MongoWriteBuilder builds {@link MongoBatchWrite}. Implementations can mix in some interfaces to
 * support different ways to write data to data sources.
 *
 * <p>Unless modified by a mixin interface, the {@link MongoBatchWrite} configured by this builder
 * is to append data without affecting existing data.
 */
public class MongoWriteBuilder implements WriteBuilder {
  /**
   * Returns a {@link BatchWrite} to write data to batch source. By default this method throws
   * exception, data sources must overwrite this method to provide an implementation, if the {@link
   * org.apache.spark.sql.connector.catalog.Table} that creates this write returns {@link
   * org.apache.spark.sql.connector.catalog.TableCapability#BATCH_WRITE} support in its {@link
   * org.apache.spark.sql.connector.catalog.Table#capabilities()}.
   */
  @Override
  public BatchWrite buildForBatch() {
    return WriteBuilder.super.buildForBatch();
  }

  /**
   * Returns a {@link StreamingWrite} to write data to streaming source. By default this method
   * throws exception, data sources must overwrite this method to provide an implementation, if the
   * {@link org.apache.spark.sql.connector.catalog.Table} that creates this write returns {@link
   * org.apache.spark.sql.connector.catalog.TableCapability#STREAMING_WRITE} support in its {@link
   * org.apache.spark.sql.connector.catalog.Table#capabilities()}.
   */
  @Override
  public StreamingWrite buildForStreaming() {
    return WriteBuilder.super.buildForStreaming();
  }
}
