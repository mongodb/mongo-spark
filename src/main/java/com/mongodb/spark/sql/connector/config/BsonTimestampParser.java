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
 */
package com.mongodb.spark.sql.connector.config;

import com.mongodb.spark.sql.connector.exceptions.ConfigException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.bson.BsonTimestamp;
import org.bson.json.JsonReader;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

final class BsonTimestampParser {
  static final String FORMAT_DESCRIPTION =
      "Must be either an integer number of seconds since the Epoch in the decimal format (example: 30),"
          + " or an instant in the ISO-8601 format with one second precision (example: '1970-01-01T00:00:30Z'),"
          + " or a BSON Timestamp in the canonical extended JSON (v2) format"
          + " (example: '{\"$timestamp\": {\"t\": 30, \"i\": 0}}').";

  static BsonTimestamp parse(
      final String propertyName, final String propertyValue, @Nullable final Logger logger)
      throws ConfigException {
    List<RuntimeException> exceptions = new ArrayList<>();
    try {
      return new BsonTimestamp(Integer.parseInt(propertyValue), 0);
    } catch (RuntimeException e) {
      exceptions.add(e);
    }
    try {
      Instant instant = Instant.parse(propertyValue);
      if (logger != null && instant.getNano() > 0) {
        logger.warn("Trimmed the value {} of `{}` to seconds.", propertyValue, propertyName);
      }
      return new BsonTimestamp(Math.toIntExact(instant.getEpochSecond()), 0);
    } catch (RuntimeException e) {
      exceptions.add(e);
    }
    try (JsonReader jsonReader = new JsonReader(propertyValue)) {
      return jsonReader.readTimestamp();
    } catch (RuntimeException e) {
      exceptions.add(e);
    }
    ConfigException configException =
        new ConfigException(propertyName, propertyValue, FORMAT_DESCRIPTION);
    exceptions.forEach(configException::addSuppressed);
    throw configException;
  }

  private BsonTimestampParser() {}
}
