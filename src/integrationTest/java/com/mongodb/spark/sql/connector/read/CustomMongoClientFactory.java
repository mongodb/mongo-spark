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

import com.mongodb.client.MongoClient;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.connection.DefaultMongoClientFactory;
import com.mongodb.spark.sql.connector.connection.MongoClientFactory;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;

public class CustomMongoClientFactory implements MongoClientFactory, Serializable {
  public static final AtomicBoolean CALLED = new AtomicBoolean(false);
  private final MongoConfig config;

  /**
   * Create a new instance of MongoClientFactory
   *
   * @param config the MongoConfig
   */
  public CustomMongoClientFactory(final MongoConfig config) {
    this.config = config;
  }

  @Override
  public MongoClient create() {
    CALLED.set(true);
    return new DefaultMongoClientFactory(config).create();
  }
}
