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

import org.bson.BsonValue;
import org.bson.codecs.BsonValueCodec;
import org.bson.codecs.Codec;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

final class ConverterHelper {
  static final Codec<BsonValue> BSON_VALUE_CODEC = new BsonValueCodec();

  static final JsonWriterSettings RELAXED_JSON_WRITER_SETTINGS =
      JsonWriterSettings.builder()
          .outputMode(JsonMode.RELAXED)
          .binaryConverter(
              (value, writer) ->
                  writer.writeString(Base64.getEncoder().encodeToString(value.getData())))
          .dateTimeConverter(
              (value, writer) -> {
                ZonedDateTime zonedDateTime = Instant.ofEpochMilli(value).atZone(ZoneOffset.UTC);
                writer.writeString(DateTimeFormatter.ISO_DATE_TIME.format(zonedDateTime));
              })
          .decimal128Converter((value, writer) -> writer.writeString(value.toString()))
          .objectIdConverter((value, writer) -> writer.writeString(value.toHexString()))
          .symbolConverter((value, writer) -> writer.writeString(value))
          .build();

  static final JsonWriterSettings EXTENDED_JSON_WRITER_SETTINGS =
      JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

  private ConverterHelper() {}
}
