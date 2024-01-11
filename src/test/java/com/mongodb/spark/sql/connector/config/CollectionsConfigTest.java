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
package com.mongodb.spark.sql.connector.config;

import static com.mongodb.spark.sql.connector.config.CollectionsConfig.Type.ALL;
import static com.mongodb.spark.sql.connector.config.CollectionsConfig.Type.MULTIPLE;
import static com.mongodb.spark.sql.connector.config.CollectionsConfig.Type.SINGLE;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.lang.Nullable;
import com.mongodb.spark.sql.connector.config.CollectionsConfig.ParsingException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

final class CollectionsConfigTest {
  @Test
  void parse() {
    String emptyElementMessage = "Empty elements are not allowed";
    String lonelyEscapeMessage = "does not escape anything";
    String unescapableMessage = "must not be escaped";
    assertAll(
        () -> assertThrows(AssertionError.class, () -> CollectionsConfig.parse(null)),
        () -> assertThrows(AssertionError.class, () -> CollectionsConfig.parse("")),
        () -> assertParse(emptyElementMessage, ","),
        () -> assertParse(emptyElementMessage, ",,"),
        () -> assertParse(emptyElementMessage, "a,,b"),
        () -> assertParse(emptyElementMessage, "a,"),
        () -> assertParse(emptyElementMessage, ",a"),
        () -> assertParse(emptyElementMessage, ",a,b"),
        () -> assertParse(emptyElementMessage, "a,b,"),
        () -> assertParse(lonelyEscapeMessage, "\\"),
        () -> assertParse(lonelyEscapeMessage, "a\\"),
        () -> assertParse(unescapableMessage, "\\a"),
        () -> assertParse(lonelyEscapeMessage, "a,b\\"),
        () -> assertParse(unescapableMessage, "\\ab"),
        () -> assertParse(ALL, of(), "*"),
        () -> assertParse(SINGLE, of("*"), "\\*"),
        () -> assertParse(SINGLE, of("**"), "**"),
        () -> assertParse(SINGLE, of("*"), "*,*"),
        () -> assertParse(SINGLE, of("*"), "\\*,\\*"),
        () -> assertParse(MULTIPLE, of("*", "**"), "*,**"),
        () -> assertParse(SINGLE, of("\\"), "\\\\"),
        () -> assertParse(SINGLE, of(" "), " "),
        () -> assertParse(MULTIPLE, of("ab*c.d e", " ", "f,g", "\\h"), "ab*c.d e, ,f\\,g,\\\\h"));
  }

  private static void assertParse(final String expectedMessage, @Nullable final String unparsed) {
    String actualMessage = assertThrows(
            ParsingException.class, () -> CollectionsConfig.parse(unparsed))
        .getMessage();
    assertTrue(
        actualMessage.contains(expectedMessage),
        format("The message \"%s\" does not contain \"%s\"", actualMessage, expectedMessage));
  }

  private static void assertParse(
      final CollectionsConfig.Type expectedType,
      final Set<String> expectedNames,
      final String unparsed) {
    CollectionsConfig actual = CollectionsConfig.parse(unparsed);
    assertEquals(expectedType, actual.getType());
    assertEquals(expectedNames, actual.getNames());
  }

  private static Set<String> of(final String... elements) {
    return Stream.of(elements).collect(Collectors.toSet());
  }
}
