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

import org.bson.BsonDocument;
import org.bson.BsonValue;

final class ResumeTokenOffset extends MongoOffset {

  private static final long serialVersionUID = 1L;

  private final BsonDocument resumeToken;
  static final ResumeTokenOffset DEFAULT_OFFSET = new ResumeTokenOffset(new BsonDocument());

  ResumeTokenOffset(final BsonDocument resumeToken) {
    this.resumeToken = resumeToken;
  }

  @Override
  MongoOffset fromJson(final String json) {
    BsonValue offset = getOffsetBsonValue(json);
    if (!offset.isDocument()) {
      throw new UnsupportedOperationException(
          format("Invalid offset value (not a document): `%s`.", json));
    }
    return new ResumeTokenOffset(offset.asDocument());
  }

  @Override
  String getOffsetStringValue() {
    return resumeToken.toJson();
  }

  BsonDocument getResumeToken() {
    return resumeToken;
  }
}
