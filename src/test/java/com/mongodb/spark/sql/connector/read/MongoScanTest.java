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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.ConfigException;
import com.mongodb.spark.sql.connector.read.partitioner.Partitioner;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.connector.read.Batch;
import org.junit.jupiter.api.Test;

class MongoScanTest {
  @Test
  void toBatch() {
    assertAll(
        () -> assertThrows(ConfigException.class, () -> new MongoScan(
                createStructType(emptyList()),
                MongoConfig.readConfig(Collections.singletonMap(
                    ReadConfig.READ_PREFIX + ReadConfig.COLLECTION_NAME_CONFIG, "*")))
            .toBatch()),
        () -> assertThrows(ConfigException.class, () -> new MongoScan(
                createStructType(emptyList()),
                MongoConfig.readConfig(Collections.singletonMap(
                    ReadConfig.READ_PREFIX + ReadConfig.COLLECTION_NAME_CONFIG, "a,b")))
            .toBatch()),
        () -> assertDoesNotThrow(() -> new MongoScan(
                createStructType(emptyList()),
                MongoConfig.readConfig(Collections.singletonMap(
                    ReadConfig.READ_PREFIX + ReadConfig.COLLECTION_NAME_CONFIG, "a")))
            .toBatch()));
  }

  @Test
  void toBatchPlansInputPartitionsOnce() {
    Map<String, String> options = new HashMap<>();
    options.put(MongoConfig.PREFIX + MongoConfig.DATABASE_NAME_CONFIG, "db");
    options.put(MongoConfig.PREFIX + MongoConfig.COLLECTION_NAME_CONFIG, "coll");
    options.put(
        ReadConfig.READ_PREFIX + ReadConfig.PARTITIONER_CONFIG,
        CountingPartitioner.class.getName());
    MongoScan scan = new MongoScan(createStructType(emptyList()), MongoConfig.readConfig(options));
    CountingPartitioner.CALLS.set(0);

    Batch batch = scan.toBatch();
    assertSame(batch, scan.toBatch());
    assertSame(batch.planInputPartitions(), scan.toBatch().planInputPartitions());
    assertEquals(1, CountingPartitioner.CALLS.get());
  }

  public static class CountingPartitioner implements Partitioner {
    static final AtomicInteger CALLS = new AtomicInteger();

    @Override
    public List<MongoInputPartition> generatePartitions(final ReadConfig readConfig) {
      CALLS.incrementAndGet();
      return singletonList(
          new MongoInputPartition(0, readConfig.getAggregationPipeline(), emptyList()));
    }
  }
}
