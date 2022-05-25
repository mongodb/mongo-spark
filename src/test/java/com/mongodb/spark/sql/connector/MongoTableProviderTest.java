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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

public class MongoTableProviderTest {

  @Test
  void testMongoTableProvider() {
    MongoTableProvider tableProvider = new MongoTableProvider();

    assertEquals("mongodb", tableProvider.shortName());
    assertTrue(tableProvider.supportsExternalMetadata());
    assertEquals(0, tableProvider.inferPartitioning(CaseInsensitiveStringMap.empty()).length);
  }

  @Test
  void testMongoTableProviderGetTable() {
    MongoTableProvider tableProvider = new MongoTableProvider();

    Transform[] partitioning = new Transform[0];

    Table table =
        tableProvider.getTable(new StructType(), partitioning, CaseInsensitiveStringMap.empty());
    assertEquals("MongoTable()", table.name());
    assertEquals(partitioning, table.partitioning());
    assertEquals(CaseInsensitiveStringMap.empty(), table.properties());
  }
}
