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

import com.mongodb.spark.sql.connector.config.WriteConfig;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.jetbrains.annotations.ApiStatus;

/** MongoWriteBuilder handles the creation of batch writer or streaming writers. */
@ApiStatus.Internal
public class MongoWriteBuilder implements WriteBuilder, SupportsTruncate {
  private final LogicalWriteInfo info;
  private final WriteConfig writeConfig;

  private final boolean truncate;

  /**
   * Construct a new instance
   *
   * @param info the logical write info
   * @param writeConfig the configuration for the write
   */
  public MongoWriteBuilder(final LogicalWriteInfo info, final WriteConfig writeConfig) {
    this(info, writeConfig.withOptions(info.options()), false);
  }

  /** Returns a {@link MongoBatchWrite} to write data to batch source. */
  @SuppressWarnings("deprecation")
  @Override
  public BatchWrite buildForBatch() {
    return new MongoBatchWrite(info, writeConfig, truncate);
  }

  /** Returns a {@link MongoStreamingWrite} to write data to streaming source. */
  @Override
  @SuppressWarnings("deprecation")
  public StreamingWrite buildForStreaming() {
    return new MongoStreamingWrite(info, writeConfig, truncate);
  }

  /** @return a MongoWriteBuilder where truncate is set to true. */
  @Override
  public WriteBuilder truncate() {
    return new MongoWriteBuilder(info, writeConfig, true);
  }

  private MongoWriteBuilder(
      final LogicalWriteInfo info, final WriteConfig writeConfig, final boolean truncate) {
    this.info = info;
    this.writeConfig = writeConfig;
    this.truncate = truncate;
  }
}
