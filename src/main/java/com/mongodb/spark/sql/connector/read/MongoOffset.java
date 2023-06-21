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

import static java.lang.String.format;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.sql.connector.read.streaming.Offset;

import org.bson.BsonDocument;
import org.bson.BsonInvalidOperationException;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonParseException;
import org.bson.json.JsonWriterSettings;

import com.mongodb.client.ChangeStreamIterable;

import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;

/** The abstract class for MongoDB change stream based offsets */
abstract class MongoOffset extends Offset implements Serializable {
  static final JsonWriterSettings EXTENDED_JSON_WRITER_SETTINGS =
      JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();
  private static final int VERSION = 1;
  private static final String JSON_TEMPLATE = format("{\"version\": %d, \"offset\": %%s}", VERSION);
  private static final Set<String> LEGACY_KEYSET =
      new HashSet<String>() {
        {
          add("_data");
        }
      };

  static BsonTimestampOffset getInitialOffset(final ReadConfig readConfig) {
    return new BsonTimestampOffset(readConfig.getStreamInitialBsonTimestamp());
  }

  static MongoOffset fromJson(final String json) {

    BsonDocument offsetDocument;
    try {
      offsetDocument = BsonDocument.parse(json);
    } catch (JsonParseException | BsonInvalidOperationException e) {
      throw new MongoSparkException(format("Invalid offset json string: `%s`.", json), e);
    }

    // Support legacy offsets
    if (offsetDocument.keySet().equals(LEGACY_KEYSET)) {
      return new ResumeTokenBasedOffset(offsetDocument);
    }

    if (!offsetDocument.containsKey("version")
        || !offsetDocument.get("version").isNumber()
        || offsetDocument.get("version").asNumber().intValue() != VERSION) {
      throw new MongoSparkException(
          format("Unsupported or missing Version: `%s`. Current Version is: %d", json, VERSION));
    }

    if (!offsetDocument.containsKey("offset")) {
      throw new MongoSparkException(format("Missing offset: `%s`.", json));
    }

    BsonValue offset = offsetDocument.get("offset");
    if (offset.isTimestamp()) {
      return new BsonTimestampOffset(offset.asTimestamp());
    } else if (offset.isDocument()) {
      return new ResumeTokenBasedOffset(offset.asDocument());
    }
    throw new MongoSparkException(
        format("Invalid offset expected a timestamp or resume token: `%s`. `%s`", offset, json));
  }

  abstract String getOffsetJsonValue();

  abstract <T> ChangeStreamIterable<T> applyToChangeStreamIterable(
      ChangeStreamIterable<T> changeStreamIterable);

  @Override
  public final String json() {
    return format(JSON_TEMPLATE, getOffsetJsonValue());
  }
}
