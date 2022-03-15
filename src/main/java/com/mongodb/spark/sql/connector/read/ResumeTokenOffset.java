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

import org.apache.spark.sql.connector.read.streaming.Offset;

import org.bson.BsonDocument;

import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;

/** An offset that contains a resume token from a change stream */
public final class ResumeTokenOffset extends Offset {
  static final ResumeTokenOffset INITIAL_RESUME_TOKEN_OFFSET =
      new ResumeTokenOffset(new BsonDocument());

  /**
   * Create a new instance from a json representation of a resume token
   *
   * @param json representation of a resume token
   * @return the ResumeTokenOffset
   */
  public static ResumeTokenOffset parse(final String json) {
    try {
      return new ResumeTokenOffset(BsonDocument.parse(json));
    } catch (RuntimeException ex) {
      throw new MongoSparkException(
          "Unable to parse the json string into a resume token. " + ex.getMessage(), ex);
    }
  }

  private final BsonDocument resumeToken;

  /**
   * Construct a new instance
   *
   * @param resumeToken from the change stream
   */
  public ResumeTokenOffset(final BsonDocument resumeToken) {
    this.resumeToken = resumeToken != null ? resumeToken : new BsonDocument();
  }

  /** @return the change stream resume token */
  public BsonDocument getResumeToken() {
    return resumeToken;
  }

  @Override
  public String json() {
    return resumeToken.toJson();
  }
}
