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

package com.mongodb.spark.sql.connector.schema;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.BsonValueCodec;
import org.bson.codecs.Codec;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

/** Shared converter helper methods and statics */
public final class ConverterHelper {
  static final SchemaToExpressionEncoderFunction SCHEMA_TO_EXPRESSION_ENCODER_FUNCTION =
      new SchemaToExpressionEncoderFunction();
  static final Codec<BsonValue> BSON_VALUE_CODEC = new BsonValueCodec();

  static final JsonWriterSettings RELAXED_JSON_WRITER_SETTINGS = JsonWriterSettings.builder()
      .outputMode(JsonMode.RELAXED)
      .binaryConverter((value, writer) ->
          writer.writeString(Base64.getEncoder().encodeToString(value.getData())))
      .dateTimeConverter((value, writer) -> {
        ZonedDateTime zonedDateTime = Instant.ofEpochMilli(value).atZone(ZoneOffset.UTC);
        writer.writeString(DateTimeFormatter.ISO_DATE_TIME.format(zonedDateTime));
      })
      .decimal128Converter((value, writer) -> writer.writeString(value.toString()))
      .objectIdConverter((value, writer) -> writer.writeString(value.toHexString()))
      .symbolConverter((value, writer) -> writer.writeString(value))
      .build();

  static final JsonWriterSettings EXTENDED_JSON_WRITER_SETTINGS =
      JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

  /**
   * Converts a bson value into its extended JSON form
   *
   * @param bsonValue the bson value
   * @return the extended JSON form
   */
  public static String toJson(final BsonValue bsonValue) {
    return toJson(bsonValue, EXTENDED_JSON_WRITER_SETTINGS);
  }

  static String toJson(final BsonValue bsonValue, final JsonWriterSettings jsonWriterSettings) {
    switch (bsonValue.getBsonType()) {
      case STRING:
        return bsonValue.asString().getValue();
      case DOCUMENT:
        return bsonValue.asDocument().toJson(jsonWriterSettings);
      default:
        String value = new BsonDocument("v", bsonValue).toJson(jsonWriterSettings);
        // Strip down to just the value
        value = value.substring(6, value.length() - 1);
        // Remove unnecessary quotes of BsonValues converted to Strings.
        // Such as BsonBinary base64 string representations
        if (value.startsWith("\"") && value.endsWith("\"")) {
          value = value.substring(1, value.length() - 1);
        }
        return value;
    }
  }

  private ConverterHelper() {}
}
