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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;
import java.util.HashMap;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.Test;

public class MongoOffsetTest {

  private static final String TIMESTAMP_OFFSET_JSON =
      "{\"version\": 1, \"offset\": {\"$timestamp\": {\"t\": 5000, \"i\": 0}}}";
  private static final String RESUME_TOKEN_OFFSET_JSON =
      "{\"version\": 1, \"offset\": {\"_data\": \"123\"}}";

  private static final MongoOffset DEFAULT_TIMESTAMP_OFFSET =
      new BsonTimestampOffset(new BsonTimestamp(-1));

  private static final MongoOffset TIMESTAMP_OFFSET =
      new BsonTimestampOffset(new BsonTimestamp(5000, 0));
  private static final MongoOffset RESUME_TOKEN_OFFSET =
      new ResumeTokenBasedOffset(new BsonDocument("_data", new BsonString("123")));

  @Test
  public void testGetInitialConfig() {
    ReadConfig readConfig = MongoConfig.createConfig(new HashMap<>()).toReadConfig();

    assertEquals(DEFAULT_TIMESTAMP_OFFSET, MongoOffset.getInitialOffset(readConfig));
    assertEquals(
        TIMESTAMP_OFFSET,
        MongoOffset.getInitialOffset(readConfig
            .withOption(ReadConfig.STREAMING_STARTUP_MODE_CONFIG, "timestamp")
            .withOption(
                ReadConfig.STREAMING_STARTUP_MODE_TIMESTAMP_START_AT_OPERATION_TIME_CONFIG,
                "5000")));
    assertEquals(MongoOffset.fromJson(RESUME_TOKEN_OFFSET_JSON), RESUME_TOKEN_OFFSET);
  }

  @Test
  public void testProducesTheExpectedOffsets() {
    assertEquals(MongoOffset.fromJson(TIMESTAMP_OFFSET_JSON), TIMESTAMP_OFFSET);
    assertEquals(MongoOffset.fromJson(RESUME_TOKEN_OFFSET_JSON), RESUME_TOKEN_OFFSET);

    assertEquals(TIMESTAMP_OFFSET_JSON, TIMESTAMP_OFFSET.json());
    assertEquals(RESUME_TOKEN_OFFSET_JSON, RESUME_TOKEN_OFFSET.json());
  }

  @Test
  public void testValidation() {
    MongoSparkException error =
        assertThrows(MongoSparkException.class, () -> MongoOffset.fromJson("\"version\": 1"));
    assertTrue(error.getMessage().startsWith("Invalid offset json string"));

    error = assertThrows(
        MongoSparkException.class,
        () -> MongoOffset.fromJson("{\"version\": \"1\", \"offset\": -1}"));
    assertTrue(error.getMessage().startsWith("Unsupported or missing Version"));

    error = assertThrows(
        MongoSparkException.class, () -> MongoOffset.fromJson("{\"version\": 2, \"offset\": -1}"));
    assertTrue(error.getMessage().startsWith("Unsupported or missing Version"));

    error = assertThrows(MongoSparkException.class, () -> MongoOffset.fromJson("{\"version\": 1}"));
    assertTrue(error.getMessage().startsWith("Missing offset"));

    error = assertThrows(
        MongoSparkException.class,
        () -> MongoOffset.fromJson("{\"version\": 1, \"offset\": \"123\"}"));
    assertTrue(
        error.getMessage().startsWith("Invalid offset expected a timestamp or resume token:"));
  }
}
