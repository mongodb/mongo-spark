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
package com.mongodb.spark.sql.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ServiceConfigurationError;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.mongodb.spark.sql.connector.mongodb.MongoSparkConnectorTestCase;

class MongoSparkConnectorTest extends MongoSparkConnectorTestCase {
  @Test
  @DisplayName("Is currently unsupported")
  void testIsUnsupportedError() {
    SparkSession sparkSession = getOrCreateSparkSession();
    ServiceConfigurationError e =
        assertThrows(
            ServiceConfigurationError.class,
            () -> sparkSession.sqlContext().read().format("mongodb").load());

    assertEquals(UnsupportedOperationException.class, e.getCause().getClass());
  }
}
