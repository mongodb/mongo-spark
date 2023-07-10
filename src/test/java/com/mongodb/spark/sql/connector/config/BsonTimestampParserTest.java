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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.mongodb.spark.sql.connector.exceptions.ConfigException;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.Test;

final class BsonTimestampParserTest {
  @Test
  void parse() {
    BsonTimestamp expected = new BsonTimestamp(30, 0);
    assertAll(
        () -> assertEquals(expected, parse("30")),
        () -> assertEquals(expected, parse("1970-01-01T00:00:30Z")),
        () -> assertEquals(expected, parse("{\"$timestamp\": {\"t\": 30, \"i\": 0}}")),
        () -> assertThrows(ConfigException.class, () -> parse("123.456")));
  }

  private static BsonTimestamp parse(final String v) throws ConfigException {
    return BsonTimestampParser.parse("", v, null);
  }
}
