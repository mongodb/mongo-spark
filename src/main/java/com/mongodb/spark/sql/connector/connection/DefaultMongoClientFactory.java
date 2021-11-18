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

package com.mongodb.spark.sql.connector.connection;

import java.util.Objects;

import org.jetbrains.annotations.ApiStatus;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import com.mongodb.spark.sql.connector.config.MongoConfig;

/** The default MongoClientFactory implementation. */
@ApiStatus.Internal
public final class DefaultMongoClientFactory implements MongoClientFactory {
  private final MongoConfig config;

  /**
   * Create a new instance of MongoClientFactory
   *
   * @param config the MongoConfig
   */
  public DefaultMongoClientFactory(final MongoConfig config) {
    this.config = config;
  }

  /** @return create a new instance of a {@code MongoClient}. */
  @Override
  public MongoClient create() {
    return MongoClients.create(config.getConnectionString());
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DefaultMongoClientFactory that = (DefaultMongoClientFactory) o;
    return Objects.equals(config.getConnectionString(), that.config.getConnectionString());
  }

  @Override
  public int hashCode() {
    return Objects.hash(config.getConnectionString());
  }
}
