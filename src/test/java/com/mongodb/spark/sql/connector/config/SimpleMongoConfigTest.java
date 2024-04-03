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
package com.mongodb.spark.sql.connector.config;

import static com.mongodb.spark.sql.connector.config.MongoConfig.CONNECTION_STRING_CONFIG;
import static com.mongodb.spark.sql.connector.config.MongoConfig.DATABASE_NAME_CONFIG;
import static com.mongodb.spark.sql.connector.config.MongoConfig.PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SimpleMongoConfigTest {

  private static final Map<String, String> CONFIG_MAP = new HashMap<>();

  static {
    CONFIG_MAP.put(PREFIX + CONNECTION_STRING_CONFIG, "mongodb://localhost:27017");
    CONFIG_MAP.put(PREFIX + DATABASE_NAME_CONFIG, "db");
  }

  @Test
  void createSimpleConfig() {
    MongoConfig config = MongoConfig.createConfig(CONFIG_MAP);

    assertInstanceOf(SimpleMongoConfig.class, config);
  }

  @Test
  void configsCreateForSameMapAreEqual() {
    MongoConfig config1 = MongoConfig.createConfig(CONFIG_MAP);
    MongoConfig config2 = MongoConfig.createConfig(CONFIG_MAP);

    assertEquals(config1, config2);
  }
}
