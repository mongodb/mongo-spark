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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LazyMongoClientCacheTest {

  private String sparkPropertyValue;
  private String propertyValue;

  @BeforeEach
  void saveSystemProperties() {
    sparkPropertyValue = System.getProperty(LazyMongoClientCache.SPARK_MONGODB_KEEP_ALIVE_MS);
    propertyValue = System.getProperty(LazyMongoClientCache.MONGODB_KEEP_ALIVE_MS);
  }

  @AfterEach
  void restoreSystemProperties() {
    System.setProperty(
        LazyMongoClientCache.SPARK_MONGODB_KEEP_ALIVE_MS,
        (sparkPropertyValue != null) ? sparkPropertyValue : "");
    System.setProperty(
        LazyMongoClientCache.MONGODB_KEEP_ALIVE_MS, (propertyValue != null) ? propertyValue : "");
  }

  @Test
  void computeKeepAliveUsesDefaultIfPropertiesNotSet() {
    System.setProperty(LazyMongoClientCache.SPARK_MONGODB_KEEP_ALIVE_MS, "");
    System.setProperty(LazyMongoClientCache.MONGODB_KEEP_ALIVE_MS, "");

    assertEquals(5000, LazyMongoClientCache.computeKeepAlive(5000));
  }

  @Test
  void computeKeepAliveUsesLegacySystemProperty() {
    System.setProperty(LazyMongoClientCache.SPARK_MONGODB_KEEP_ALIVE_MS, "123");
    System.setProperty(LazyMongoClientCache.MONGODB_KEEP_ALIVE_MS, "");

    assertEquals(123, LazyMongoClientCache.computeKeepAlive(5000));
  }

  @Test
  void computeKeepAliveUsesMongoSystemProperty() {
    System.setProperty(LazyMongoClientCache.SPARK_MONGODB_KEEP_ALIVE_MS, "");
    System.setProperty(LazyMongoClientCache.MONGODB_KEEP_ALIVE_MS, "123");

    assertEquals(123, LazyMongoClientCache.computeKeepAlive(5000));
  }

  @Test
  void computeKeepAlivePrefersMongoSystemProperty() {
    System.setProperty(LazyMongoClientCache.SPARK_MONGODB_KEEP_ALIVE_MS, "456");
    System.setProperty(LazyMongoClientCache.MONGODB_KEEP_ALIVE_MS, "123");

    assertEquals(123, LazyMongoClientCache.computeKeepAlive(5000));
  }
}
