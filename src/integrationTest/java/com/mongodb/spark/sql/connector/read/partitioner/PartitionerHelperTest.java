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

package com.mongodb.spark.sql.connector.read.partitioner;

import static com.mongodb.spark.sql.connector.config.MongoConfig.COMMENT_CONFIG;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mongodb.client.MongoCollection;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.mongodb.MongoSparkConnectorTestCase;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

public class PartitionerHelperTest extends MongoSparkConnectorTestCase {

  @Test
  void testCollStats() {
    ReadConfig readConfig = getMongoConfig().toReadConfig();
    readConfig.doWithCollection(MongoCollection::drop);
    assertDoesNotThrow(() -> PartitionerHelper.storageStats(readConfig));

    readConfig.doWithCollection(coll -> {
      coll.insertOne(new BsonDocument());
      coll.deleteOne(new BsonDocument());
    });
    assertEquals(0, PartitionerHelper.storageStats(readConfig).getNumber("size").intValue());
  }

  @Test
  void shouldLogCommentsInProfilerLogs() {
    ReadConfig readConfig = getMongoConfig().toReadConfig();
    loadSampleData(50, 1, readConfig);

    ReadConfig configWithComment = readConfig.withOption(COMMENT_CONFIG, TEST_COMMENT);
    assertCommentsInProfile(
        () -> PartitionerHelper.storageStats(configWithComment), configWithComment);
  }
}
