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

import static com.mongodb.spark.sql.connector.read.MongoScanBuilder.unquoteFieldName;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class MongoScanBuilderTest {

  @ParameterizedTest
  @MethodSource("fieldNameProvider")
  void unquoteFields(final String expected, final String column) {
    assertEquals(expected, unquoteFieldName(column));
  }

  private static Stream<Arguments> fieldNameProvider() {
    return Stream.of(
        Arguments.of("field-name", "field-name"),
        Arguments.of("field-name", "`field-name`"),
        Arguments.of("field`name", "`field``name`"),
        Arguments.of("sub-doc.field-name", "sub-doc.field-name"),
        Arguments.of("sub-doc.field`name", "`sub-doc`.`field``name`"),
        Arguments.of("sub`doc.field`name", "`sub``doc`.`field``name`"));
  }
}
