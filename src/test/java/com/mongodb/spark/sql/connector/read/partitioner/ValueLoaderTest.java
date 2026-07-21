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

package com.mongodb.spark.sql.connector.read.partitioner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.jupiter.api.Test;

class ValueLoaderTest {

  @Test
  void loadsSingleLevelFieldValue() {
    ValueLoader loader = new ValueLoader("_id");
    BsonDocument document = new BsonDocument("_id", new BsonInt32(42));

    BsonValue result = loader.apply(document);

    assertEquals(new BsonInt32(42), result);
  }

  @Test
  void loadsNestedFieldValueWithDottedPath() {
    ValueLoader loader = new ValueLoader("a.b.c");
    BsonDocument document = new BsonDocument()
        .append(
            "a",
            new BsonDocument().append("b", new BsonDocument().append("c", new BsonInt32(100))));

    BsonValue result = loader.apply(document);

    assertEquals(new BsonInt32(100), result);
  }

  @Test
  void loadsNestedFieldValueWithTwoLevelPath() {
    ValueLoader loader = new ValueLoader("user.name");
    BsonDocument document = new BsonDocument()
        .append("user", new BsonDocument().append("name", new BsonString("John")));

    BsonValue result = loader.apply(document);

    assertEquals(new BsonString("John"), result);
  }

  @Test
  void fallsBackToTopLevelWhenPathDoesNotExist() {
    ValueLoader loader = new ValueLoader("nested.field");
    BsonDocument document = new BsonDocument("nested.field", new BsonInt32(50));

    BsonValue result = loader.apply(document);

    assertEquals(new BsonInt32(50), result);
  }

  @Test
  void fallsBackToTopLevelWhenIntermediatePathMissing() {
    ValueLoader loader = new ValueLoader("a.b.c");
    BsonDocument document = new BsonDocument("a.b.c", new BsonInt32(75));

    BsonValue result = loader.apply(document);

    assertEquals(new BsonInt32(75), result);
  }

  @Test
  void fallsBackToTopLevelWhenArrayEncounteredInPath() {
    ValueLoader loader = new ValueLoader("a.b.c");
    BsonDocument document =
        new BsonDocument().append("a", new BsonArray()).append("a.b.c", new BsonInt32(99));

    BsonValue result = loader.apply(document);

    assertEquals(new BsonInt32(99), result);
  }

  @Test
  void fallsBackToTopLevelWhenNonDocumentEncounteredBeforeFinalSegment() {
    ValueLoader loader = new ValueLoader("a.b.c");
    BsonDocument document = new BsonDocument()
        .append("a", new BsonString("not a document"))
        .append("a.b.c", new BsonInt32(88));

    BsonValue result = loader.apply(document);

    assertEquals(new BsonInt32(88), result);
  }

  @Test
  void returnsNullWhenFieldNotFoundAndNoFallback() {
    ValueLoader loader = new ValueLoader("missing");
    BsonDocument document = new BsonDocument("_id", new BsonInt32(1));

    BsonValue result = loader.apply(document);

    assertNull(result);
  }

  @Test
  void returnsNullWhenNestedFieldNotFoundAndNoFallback() {
    ValueLoader loader = new ValueLoader("a.b.c");
    BsonDocument document = new BsonDocument("a", new BsonDocument("b", new BsonDocument()));

    BsonValue result = loader.apply(document);

    assertNull(result);
  }

  @Test
  void handlesEmptyDocument() {
    ValueLoader loader = new ValueLoader("field");
    BsonDocument document = new BsonDocument();

    BsonValue result = loader.apply(document);

    assertNull(result);
  }

  @Test
  void handlesDottedPathWithOnlyDot() {
    ValueLoader loader = new ValueLoader(".");
    BsonDocument document = new BsonDocument(".", new BsonInt32(42));

    BsonValue result = loader.apply(document);

    assertEquals(new BsonInt32(42), result);
  }

  @Test
  void loadsDeepNestedField() {
    ValueLoader loader = new ValueLoader("a.b.c.d.e");
    BsonDocument document = new BsonDocument()
        .append(
            "a",
            new BsonDocument()
                .append(
                    "b",
                    new BsonDocument()
                        .append(
                            "c",
                            new BsonDocument()
                                .append(
                                    "d",
                                    new BsonDocument()
                                        .append("e", new BsonString("deep value"))))));

    BsonValue result = loader.apply(document);

    assertEquals(new BsonString("deep value"), result);
  }

  @Test
  void loadsValueWhenArrayIsAtFinalSegment() {
    ValueLoader loader = new ValueLoader("items");
    BsonArray arrayValue = new BsonArray();
    arrayValue.add(new BsonInt32(1));
    arrayValue.add(new BsonInt32(2));
    BsonDocument document = new BsonDocument("items", arrayValue);

    BsonValue result = loader.apply(document);

    assertEquals(arrayValue, result);
  }

  @Test
  void fallsBackWhenArrayEncounteredInIntermediatePath() {
    ValueLoader loader = new ValueLoader("a.items.value");
    BsonArray arrayValue = new BsonArray();
    arrayValue.add(new BsonInt32(1));
    BsonDocument document = new BsonDocument()
        .append("a", new BsonDocument().append("items", arrayValue))
        .append("a.items.value", new BsonInt32(42));

    BsonValue result = loader.apply(document);

    assertEquals(new BsonInt32(42), result);
  }

  @Test
  void loadsValueWithMultipleSiblingFields() {
    ValueLoader loader = new ValueLoader("data.value");
    BsonDocument document = new BsonDocument()
        .append(
            "data",
            new BsonDocument()
                .append("value", new BsonInt32(123))
                .append("other", new BsonInt32(456)))
        .append("name", new BsonString("test"));

    BsonValue result = loader.apply(document);

    assertEquals(new BsonInt32(123), result);
  }

  @Test
  void loadsValueWhenPathSegmentIsZeroLength() {
    ValueLoader loader = new ValueLoader("a..b");
    BsonDocument document = new BsonDocument("a..b", new BsonInt32(77));

    BsonValue result = loader.apply(document);

    // Should fall back to top-level lookup
    assertEquals(new BsonInt32(77), result);
  }

  @Test
  void loadsIntegerValueFromNestedPath() {
    ValueLoader loader = new ValueLoader("metadata.count");
    BsonDocument document = new BsonDocument()
        .append("metadata", new BsonDocument().append("count", new BsonInt32(999)));

    BsonValue result = loader.apply(document);

    assertEquals(new BsonInt32(999), result);
  }

  @Test
  void fallsBackToTopLevelWhenFirstSegmentNotFound() {
    ValueLoader loader = new ValueLoader("missing.field.path");
    BsonDocument document = new BsonDocument("missing.field.path", new BsonString("fallback"));

    BsonValue result = loader.apply(document);

    assertEquals(new BsonString("fallback"), result);
  }

  @Test
  void handlesSingleFieldWithoutDots() {
    ValueLoader loader = new ValueLoader("simpleField");
    BsonDocument document = new BsonDocument("simpleField", new BsonString("simple value"));

    BsonValue result = loader.apply(document);

    assertEquals(new BsonString("simple value"), result);
  }
}
