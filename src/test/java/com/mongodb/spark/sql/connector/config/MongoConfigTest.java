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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class MongoConfigTest {

  private static final Map<String, String> CONFIG_MAP = new HashMap<>();
  private static final Map<String, String> READ_CONFIG_MAP = new HashMap<>();
  private static final Map<String, String> WRITE_CONFIG_MAP = new HashMap<>();
  private static final Map<String, String> COMBINED_CONFIG_MAP = new HashMap<>();
  private static final Map<String, String> OPTIONS_CONFIG_MAP = new HashMap<>();

  static {
    CONFIG_MAP.put(
        MongoConfig.PREFIX + MongoConfig.CONNECTION_STRING_CONFIG, "mongodb://localhost:27017");
    CONFIG_MAP.put(MongoConfig.PREFIX + MongoConfig.DATABASE_NAME_CONFIG, "db");
    CONFIG_MAP.put(MongoConfig.PREFIX + MongoConfig.COLLECTION_NAME_CONFIG, "coll");

    READ_CONFIG_MAP.put(
        MongoConfig.READ_PREFIX + MongoConfig.CONNECTION_STRING_CONFIG, "mongodb://read:27017");
    READ_CONFIG_MAP.put(MongoConfig.READ_PREFIX + MongoConfig.DATABASE_NAME_CONFIG, "readDB");
    READ_CONFIG_MAP.put(MongoConfig.READ_PREFIX + MongoConfig.COLLECTION_NAME_CONFIG, "readColl");

    WRITE_CONFIG_MAP.put(
        MongoConfig.WRITE_PREFIX + MongoConfig.CONNECTION_STRING_CONFIG, "mongodb://write:27017");
    WRITE_CONFIG_MAP.put(MongoConfig.WRITE_PREFIX + MongoConfig.DATABASE_NAME_CONFIG, "writeDB");
    WRITE_CONFIG_MAP.put(
        MongoConfig.WRITE_PREFIX + MongoConfig.COLLECTION_NAME_CONFIG, "writeColl");

    COMBINED_CONFIG_MAP.putAll(CONFIG_MAP);
    COMBINED_CONFIG_MAP.putAll(READ_CONFIG_MAP);
    COMBINED_CONFIG_MAP.putAll(WRITE_CONFIG_MAP);

    OPTIONS_CONFIG_MAP.put(MongoConfig.PREFIX + "boolean", "true");
    OPTIONS_CONFIG_MAP.put(MongoConfig.PREFIX + "string", "string");
    OPTIONS_CONFIG_MAP.put(MongoConfig.PREFIX + "int", "1");
    OPTIONS_CONFIG_MAP.put(MongoConfig.PREFIX + "long", "1");
    OPTIONS_CONFIG_MAP.put(MongoConfig.PREFIX + "double", "1.0");
    OPTIONS_CONFIG_MAP.put(MongoConfig.PREFIX + "list", "1,2");
  }

  @Test
  void testMongoConfig() {
    MongoConfig mongoConfig = MongoConfig.createConfig(CONFIG_MAP);
    assertEquals(MongoConfig.readConfig(CONFIG_MAP), mongoConfig.toReadConfig());
    assertEquals(MongoConfig.writeConfig(CONFIG_MAP), mongoConfig.toWriteConfig());
  }

  @Test
  void testReadMongoConfig() {
    MongoConfig mongoConfig = MongoConfig.createConfig(READ_CONFIG_MAP);
    assertEquals(MongoConfig.readConfig(READ_CONFIG_MAP), mongoConfig.toReadConfig());
    assertEquals(
        MongoConfig.readConfig(READ_CONFIG_MAP), mongoConfig.toWriteConfig().toReadConfig());
    assertEquals(MongoConfig.readConfig(COMBINED_CONFIG_MAP), mongoConfig.toReadConfig());
    assertEquals(
        MongoConfig.readConfig(COMBINED_CONFIG_MAP), mongoConfig.toWriteConfig().toReadConfig());
  }

  @Test
  void testWriteMongoConfig() {
    MongoConfig mongoConfig = MongoConfig.createConfig(WRITE_CONFIG_MAP);
    assertEquals(MongoConfig.writeConfig(WRITE_CONFIG_MAP), mongoConfig.toWriteConfig());
    assertEquals(
        MongoConfig.writeConfig(WRITE_CONFIG_MAP), mongoConfig.toReadConfig().toWriteConfig());

    assertEquals(MongoConfig.writeConfig(COMBINED_CONFIG_MAP), mongoConfig.toWriteConfig());
    assertEquals(
        MongoConfig.writeConfig(COMBINED_CONFIG_MAP), mongoConfig.toReadConfig().toWriteConfig());
  }

  @Test
  void testMongoConfigOptionsParsing() {
    MongoConfig mongoConfig = MongoConfig.readConfig(OPTIONS_CONFIG_MAP);

    assertEquals(OPTIONS_CONFIG_MAP, mongoConfig.getOriginals());
    assertEquals("string", mongoConfig.get("string"));
    assertEquals("alt", mongoConfig.getOrDefault("missing", "alt"));
    assertTrue(mongoConfig.getBoolean("boolean", true));
    assertEquals(1, mongoConfig.getInt("int", 0));
    assertEquals(0, mongoConfig.getInt("missing", 0));
    assertEquals(1L, mongoConfig.getLong("long", 0L));
    assertEquals(0L, mongoConfig.getLong("missing", 0L));
    assertEquals(1.0, mongoConfig.getDouble("double", 0.0));
    assertEquals(0.0, mongoConfig.getDouble("missing", 0.0));
    assertIterableEquals(asList("1", "2"), mongoConfig.getList("list", asList("3", "4")));
    assertIterableEquals(asList("3", "4"), mongoConfig.getList("missing", asList("3", "4")));
    assertIterableEquals(
        singletonList("string"), mongoConfig.getList("string", singletonList("1")));

    // Handle overridden options
    Map<String, String> newOptions = new HashMap<>();
    newOptions.put(MongoConfig.READ_PREFIX + "string", "new string");
    newOptions.put(MongoConfig.WRITE_PREFIX + "string", "another new string");

    Map<String, String> combinedOptions = new HashMap<>(OPTIONS_CONFIG_MAP);
    combinedOptions.putAll(newOptions);

    MongoConfig newConfig = mongoConfig.withOptions(combinedOptions);
    assertEquals("new string", newConfig.get("string"));
    assertEquals("another new string", newConfig.toWriteConfig().get("string"));
  }

  @ParameterizedTest
  @MethodSource("optionsMapConfigs")
  void testConfigsAreCaseInsensitive(final MongoConfig mongoConfig) {
    assertEquals("string", mongoConfig.get("StRiNg"));
    assertEquals(1, mongoConfig.getInt("INT", 0));
    assertEquals(1L, mongoConfig.getLong("loNG", 0L));
    assertEquals(1.0, mongoConfig.getDouble("DOubLE", 0.0));

    HashMap<String, String> overrides = new HashMap<>();
    overrides.put(MongoConfig.PREFIX + "STRING", "STRING");
    overrides.put(MongoConfig.READ_PREFIX + "STRING", "READ STRING");
    overrides.put(MongoConfig.WRITE_PREFIX + "STRING", "WRITE STRING");

    MongoConfig newMongoConfig = MongoConfig.createConfig(overrides);
    assertEquals("STRING", newMongoConfig.get("StRiNg"));
    assertEquals("READ STRING", newMongoConfig.toReadConfig().get("StRiNg"));
    assertEquals("WRITE STRING", newMongoConfig.toWriteConfig().get("StRiNg"));
  }

  @Test
  void testDatabaseAndCollectionParsing() {
    Map<String, String> options = new HashMap<>(CONFIG_MAP);
    options.remove(MongoConfig.PREFIX + MongoConfig.DATABASE_NAME_CONFIG);
    options.remove(MongoConfig.PREFIX + MongoConfig.COLLECTION_NAME_CONFIG);
    options.put(
        MongoConfig.PREFIX + MongoConfig.CONNECTION_STRING_CONFIG,
        "mongodb://localhost/myDB.myColl");
    options.put(
        MongoConfig.WRITE_PREFIX + MongoConfig.CONNECTION_STRING_CONFIG,
        "mongodb://localhost/writeDB.writeColl");

    MongoConfig mongoConfig = MongoConfig.createConfig(options);
    assertEquals("myDB", mongoConfig.getDatabaseName());
    assertEquals("myColl", mongoConfig.getCollectionName());

    mongoConfig = MongoConfig.readConfig(options);
    assertEquals("myDB", mongoConfig.getDatabaseName());
    assertEquals("myColl", mongoConfig.getCollectionName());

    mongoConfig = mongoConfig.toWriteConfig();
    assertEquals("writeDB", mongoConfig.getDatabaseName());
    assertEquals("writeColl", mongoConfig.getCollectionName());

    options.put(MongoConfig.READ_PREFIX + MongoConfig.DATABASE_NAME_CONFIG, "overriddenDb");
    options.put(MongoConfig.READ_PREFIX + MongoConfig.COLLECTION_NAME_CONFIG, "overriddenColl");

    // Explicit beats implicit
    mongoConfig = mongoConfig.withOptions(options);
    assertEquals("writeDB", mongoConfig.getDatabaseName());
    assertEquals("writeColl", mongoConfig.getCollectionName());

    // Explicit beats implicit
    mongoConfig = mongoConfig.toReadConfig();
    assertEquals("overriddenDb", mongoConfig.getDatabaseName());
    assertEquals("overriddenColl", mongoConfig.getCollectionName());

    // Explicit beats implicit
    options.put(MongoConfig.WRITE_PREFIX + MongoConfig.DATABASE_NAME_CONFIG, "overriddenDb");
    options.put(MongoConfig.WRITE_PREFIX + MongoConfig.COLLECTION_NAME_CONFIG, "overriddenColl");

    mongoConfig = MongoConfig.writeConfig(options);
    assertEquals("overriddenDb", mongoConfig.getDatabaseName());
    assertEquals("overriddenColl", mongoConfig.getCollectionName());
  }

  @ParameterizedTest
  @MethodSource("optionsMapConfigs")
  void testMongoConfigOptionsParsingErrors(final MongoConfig mongoConfig) {
    assertThrows(IllegalArgumentException.class, () -> mongoConfig.getBoolean("string", true));
    assertThrows(IllegalArgumentException.class, () -> mongoConfig.getBoolean("string", true));
    assertThrows(NumberFormatException.class, () -> mongoConfig.getInt("string", 1));
    assertThrows(NumberFormatException.class, () -> mongoConfig.getLong("string", 1L));
    assertThrows(NumberFormatException.class, () -> mongoConfig.getDouble("string", 1.0));
  }

  @Test
  void testConnectionStringErrors() {
    Map<String, String> options = new HashMap<>(OPTIONS_CONFIG_MAP);
    options.put(MongoConfig.PREFIX + MongoConfig.CONNECTION_STRING_CONFIG, "invalid");
    assertThrows(IllegalArgumentException.class, () -> MongoConfig.createConfig(options));
    assertThrows(IllegalArgumentException.class, () -> MongoConfig.readConfig(options));
    assertThrows(IllegalArgumentException.class, () -> MongoConfig.writeConfig(options));
  }

  @ParameterizedTest
  @MethodSource("optionsMapConfigs")
  void testErrorScenarios(final MongoConfig mongoConfig) {
    assertThrows(IllegalArgumentException.class, () -> mongoConfig.getBoolean("string", true));
    assertThrows(NumberFormatException.class, () -> mongoConfig.getInt("string", 1));
    assertThrows(NumberFormatException.class, () -> mongoConfig.getLong("string", 1L));
    assertThrows(NumberFormatException.class, () -> mongoConfig.getDouble("string", 1.0));
  }

  private static Stream<MongoConfig> optionsMapConfigs() {
    return Stream.of(
        MongoConfig.createConfig(OPTIONS_CONFIG_MAP),
        MongoConfig.readConfig(OPTIONS_CONFIG_MAP),
        MongoConfig.writeConfig(OPTIONS_CONFIG_MAP));
  }
}
