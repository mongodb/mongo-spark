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

import static com.mongodb.spark.sql.connector.config.MongoConfig.READ_PREFIX;
import static com.mongodb.spark.sql.connector.config.ReadConfig.STREAM_MICRO_BATCH_MAX_ROWS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.mongodb.spark.sql.connector.config.CollectionsConfig;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import java.util.List;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class MongoMicroBatchStreamTest extends AbstractMongoStreamTest {

  @Override
  String collectionPrefix() {
    return "microBatch";
  }

  @Override
  Trigger getTrigger() {
    return Trigger.ProcessingTime("5 seconds");
  }

  @ParameterizedTest
  @ValueSource(strings = {"SINGLE", "MULTIPLE", "ALL"})
  void testStreamBatchMaxRows(final String collectionsConfigModeStr) {
    assumeTrue(supportsChangeStreams());

    CollectionsConfig.Type collectionsConfigType =
        CollectionsConfig.Type.valueOf(collectionsConfigModeStr);
    setTestIdentifier(computeTestIdentifier("BatchMaxRows", collectionsConfigType));

    int maxRows = 10;
    StreamingEventsListener listener = new StreamingEventsListener();

    MongoConfig mongoConfig = createMongoConfig(collectionsConfigType)
        .withOption(READ_PREFIX + STREAM_MICRO_BATCH_MAX_ROWS_CONFIG, String.valueOf(maxRows));
    testStreamingQuery(
        MEMORY,
        mongoConfig,
        DEFAULT_SCHEMA,
        null,
        listener,
        withSource("inserting 0-25", (msg, coll) -> coll.insertMany(createDocuments(0, 25))),
        withMemorySink(
            "Expected to see 25 documents",
            (msg, ds) -> assertEquals(25, ds.collectAsList().size(), msg)),
        withSource("inserting 100-125", (msg, coll) -> coll.insertMany(createDocuments(100, 125))),
        withMemorySink(
            "Expecting to see 50 documents",
            (msg, ds) -> assertEquals(50, ds.collectAsList().size(), msg)));

    List<Long> batchSizes = listener.getBatchInputRows();
    assertFalse(batchSizes.isEmpty(), "Expected at least one batch with input rows");
    for (long batchSize : batchSizes) {
      assertTrue(
          batchSize <= maxRows,
          String.format(
              "Batch had %d input rows, exceeding maxRows=%d. All batches: %s",
              batchSize, maxRows, batchSizes));
    }
  }
}
