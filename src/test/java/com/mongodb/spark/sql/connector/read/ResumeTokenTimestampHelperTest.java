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

import static com.mongodb.spark.sql.connector.read.ResumeTokenTimestampHelper.getTimestamp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.Test;

public class ResumeTokenTimestampHelperTest {

  @Test
  public void testValidResumeTokenVersion1() {
    BsonDocument resumeToken =
        BsonDocument.parse("{\"_data\": \"82612F653E000000022B0229296E04\"}");

    assertEquals(new BsonTimestamp(1630496062, 2), getTimestamp(resumeToken));
  }

  @Test
  public void testValidResumeTokenVersion2() {
    BsonDocument resumeToken = BsonDocument.parse(
        "{\"_data\": \"82612E8513000000012B022C0100296E5A1004A5093ABB38FE4B9EA67F01BB1A96D812463C5F6964003C5F5F5F78000004\"}");

    assertEquals(new BsonTimestamp(1630438675, 1), getTimestamp(resumeToken));
  }

  @Test
  public void testInvalidResumeToken() {
    assertThrows(MongoSparkException.class, () -> getTimestamp(BsonDocument.parse("{}")));
    assertThrows(
        MongoSparkException.class, () -> getTimestamp(BsonDocument.parse("{\"_data\": {a: 1}}")));
    assertThrows(
        MongoSparkException.class,
        () -> getTimestamp(BsonDocument.parse("{\"_data\": \"83912F653E000000022B0229296E04\"}")));
  }
}
