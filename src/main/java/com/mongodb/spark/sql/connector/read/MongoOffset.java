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

import org.apache.spark.sql.connector.read.streaming.Offset;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.json.JsonParseException;

/** The abstract class for MongoDB changestream based offsets */
abstract class MongoOffset extends Offset implements Serializable {
  private static final int VERSION = 1;
  private static final String JSON_TEMPLATE = format("{version: %s, offset: %%s}", VERSION);

  static BsonValue getOffsetBsonValue(final String json) {
    BsonDocument offsetDocument;
    try {
      offsetDocument = BsonDocument.parse(json);
    } catch (JsonParseException e) {
      throw new UnsupportedOperationException(format("Invalid offset json string: `%s`.", json), e);
    }

    if (!offsetDocument.containsKey("version")
        || !offsetDocument.get("version").isInt32()
        || offsetDocument.get("version").asInt32().getValue() != VERSION) {
      throw new UnsupportedOperationException(
          format("Unsupported or missing Version: `%s`. Current Version is: %s", json, VERSION));
    }

    if (!offsetDocument.containsKey("offset")) {
      throw new UnsupportedOperationException(format("Missing offset: `%s`.", json));
    }
    return offsetDocument.get("offset");
  }

  abstract MongoOffset fromJson(String json);

  abstract String getOffsetStringValue();

  @Override
  public String json() {
    return format(JSON_TEMPLATE, getOffsetStringValue());
  }
}
