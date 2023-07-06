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

import com.mongodb.client.MongoClient;
import com.mongodb.spark.sql.connector.annotations.ThreadSafe;

/**
 * A factory for creating MongoClients
 *
 * <p>A factory should produce {@code MongoClients} with the same configuration each time {@link
 * MongoClientFactory#create()} is called.
 *
 * <p>The creation of a {@link MongoClient} is expensive (creating the connection pool and
 * authenticating connections), so {@code MongoClients} are automatically cached, using the {@code
 * MongoClientFactory} as the key.
 *
 * <p>Implementations of the factory can make use of the automatic caching by ensuring the {@code
 * MongoClientFactory.equals()} returns true if the {@code MongoClientFactory} instances configured
 * with the same relevant {@link com.mongodb.spark.sql.connector.config.MongoConfig} values and if
 * the implementation creates {@code MongoClients} with the same configuration each time {@link
 * MongoClientFactory#create()} is called. Note: Only the {@code MongoConfig} values used to create
 * the {@code MongoClient} need be compared in the {@code MongoClientFactory.equals()} method.
 *
 * <p>Note: Implementations must have either a no-args public constructor or a public constructor
 * that takes a single parameter; a {@link com.mongodb.spark.sql.connector.config.MongoConfig}
 * instance.
 */
@ThreadSafe
public interface MongoClientFactory {

  /** @return create a new instance of a {@code MongoClient}. */
  MongoClient create();
}
