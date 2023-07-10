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

import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;

final class ResumeTokenTimestampHelper {

  /**
   * Returns the BsonTimestamp from a resume token.
   *
   * <p>Handles server based resume tokens or approximated Bson documents with a `_data` field that
   * contains a BsonTimestamp.
   *
   * <p>Server based resume tokens have the following structure:
   *
   * <p>1. It's a document containing a single field named "_data" whose value is a string
   *
   * <p>2. The string is hex-encoded
   *
   * <p>3. The first byte is the "canonical type" for a BSON timestamp, encoded as an unsigned byte.
   * It should always equal 130.
   *
   * <p>4. The next 8 bytes are the BSON timestamp representing the operation time, encoded as an
   * unsigned long value with big endian byte order (unlike BSON itself, which is little endian).
   *
   * <p>The {@link BsonTimestamp} class contains the logic for pulling out the seconds since the
   * epoch from that value. See <a href="http://bsonspec.org">http://bsonspec.org</a> for details.
   *
   * @param resumeToken the resumeToken or null
   * @return the timestamp from the resume token
   * @throws MongoSparkException if passed an invalid resumeToken
   */
  static BsonTimestamp getTimestamp(final BsonDocument resumeToken) {
    if (!resumeToken.containsKey("_data")) {
      throw new MongoSparkException("Invalid resume token, missing `_data` field");
    }

    BsonValue data = resumeToken.get("_data");
    if (data.isTimestamp()) {
      return data.asTimestamp();
    }

    if (!data.isString()) {
      throw new MongoSparkException(
          "Invalid resume token, expected string value for `_data` field, but found: "
              + data.getBsonType().name());
    }

    byte[] bytes = hexStringToBytes(data.asString().getValue());
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);

    // cast to an int then remove the sign bit to get the unsigned value
    int canonicalType = ((int) byteBuffer.get()) & 0xff;
    if (canonicalType != 130) {
      throw new MongoSparkException(
          "Invalid resume token, expected _data field canonical type to equal 130, but found: "
              + canonicalType);
    }

    long timestampAsLong = byteBuffer.asLongBuffer().get();
    return new BsonTimestamp(timestampAsLong);
  }

  static byte[] hexStringToBytes(final String hexString) {
    int len = hexString.length();
    byte[] bytes = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      bytes[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
          + Character.digit(hexString.charAt(i + 1), 16));
    }
    return bytes;
  }

  private ResumeTokenTimestampHelper() {}
}
