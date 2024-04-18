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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.WriteConcern;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.spark.sql.connector.exceptions.ConfigException;
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
  void testMongoConfigWriteConcern() {
    WriteConfig writeConfig = MongoConfig.createConfig(CONFIG_MAP)
        .withOption(WriteConfig.WRITE_PREFIX + WriteConfig.WRITE_CONCERN_W_CONFIG, "1")
        .toWriteConfig();
    assertEquals(1, writeConfig.getWriteConcern().getW());

    writeConfig =
        writeConfig.withOption(WriteConfig.WRITE_PREFIX + WriteConfig.WRITE_CONCERN_W_CONFIG, "2");
    assertEquals(2, writeConfig.getWriteConcern().getW());

    writeConfig =
        writeConfig.withOption(WriteConfig.WRITE_PREFIX + WriteConfig.WRITE_CONCERN_W_CONFIG, "3");
    assertEquals(3, writeConfig.getWriteConcern().getW());

    writeConfig = writeConfig.withOption(
        WriteConfig.WRITE_PREFIX + WriteConfig.WRITE_CONCERN_W_CONFIG, "majority");
    assertEquals(WriteConcern.MAJORITY, writeConfig.getWriteConcern());

    writeConfig = writeConfig.withOption(
        WriteConfig.WRITE_PREFIX + WriteConfig.WRITE_CONCERN_W_CONFIG, "region1");
    assertEquals(new WriteConcern("region1"), writeConfig.getWriteConcern());
  }

  @Test
  void testWriteConfigConvertJson() {
    WriteConfig writeConfig = MongoConfig.createConfig(CONFIG_MAP).toWriteConfig();
    assertEquals(writeConfig.convertJson(), WriteConfig.ConvertJson.FALSE);
    assertEquals(
        writeConfig.withOption("convertJson", "False").convertJson(),
        WriteConfig.ConvertJson.FALSE);
    assertEquals(
        writeConfig.withOption("convertJson", "True").convertJson(), WriteConfig.ConvertJson.ANY);
    assertEquals(
        writeConfig.withOption("convertJson", "Any").convertJson(), WriteConfig.ConvertJson.ANY);
    assertEquals(
        writeConfig.withOption("convertJson", "objectOrArrayOnly").convertJson(),
        WriteConfig.ConvertJson.OBJECT_OR_ARRAY_ONLY);
    assertEquals(
        writeConfig.withOption("convertJson", "OBJECT_OR_ARRAY_ONLY").convertJson(),
        WriteConfig.ConvertJson.OBJECT_OR_ARRAY_ONLY);
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

    MongoConfig newConfig = mongoConfig.withOption("a", "a").withOptions(combinedOptions);
    assertEquals("a", newConfig.get("a"));
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
    ReadConfig readConfig = mongoConfig.toReadConfig();
    assertEquals("myDB", readConfig.getDatabaseName());
    assertEquals("myColl", readConfig.getCollectionName());

    WriteConfig writeConfig = mongoConfig.toWriteConfig();
    assertEquals("writeDB", writeConfig.getDatabaseName());
    assertEquals("writeColl", writeConfig.getCollectionName());

    options.put(MongoConfig.READ_PREFIX + MongoConfig.DATABASE_NAME_CONFIG, "overriddenReadDb");
    options.put(MongoConfig.READ_PREFIX + MongoConfig.COLLECTION_NAME_CONFIG, "overriddenReadColl");
    options.put(MongoConfig.WRITE_PREFIX + MongoConfig.DATABASE_NAME_CONFIG, "overriddenWriteDb");
    options.put(
        MongoConfig.WRITE_PREFIX + MongoConfig.COLLECTION_NAME_CONFIG, "overriddenWriteColl");

    // Explicit beats implicit
    readConfig = MongoConfig.readConfig(options);
    assertEquals("overriddenReadDb", readConfig.getDatabaseName());
    assertEquals("overriddenReadColl", readConfig.getCollectionName());

    // Explicit beats implicit
    writeConfig = MongoConfig.writeConfig(options);
    assertEquals("overriddenWriteDb", writeConfig.getDatabaseName());
    assertEquals("overriddenWriteColl", writeConfig.getCollectionName());

    // CollectionsConfig.Mode.MULTIPLE
    options.put(
        MongoConfig.READ_PREFIX + MongoConfig.COLLECTION_NAME_CONFIG, "readCollA,readCollB");
    options.put(
        MongoConfig.WRITE_PREFIX + MongoConfig.COLLECTION_NAME_CONFIG, "writeCollA,writeCollB");
    assertEquals(
        CollectionsConfig.parse("readCollA,readCollB"),
        MongoConfig.readConfig(options).getCollectionsConfig());
    assertThrows(ConfigException.class, () -> MongoConfig.readConfig(options).getCollectionName());
    assertThrows(
        ConfigException.class, () -> MongoConfig.writeConfig(options).getCollectionsConfig());
    assertThrows(ConfigException.class, () -> MongoConfig.writeConfig(options).getCollectionName());

    // CollectionsConfig.Mode.ALL
    options.put(MongoConfig.READ_PREFIX + MongoConfig.COLLECTION_NAME_CONFIG, "*");
    options.put(MongoConfig.WRITE_PREFIX + MongoConfig.COLLECTION_NAME_CONFIG, "*");
    assertEquals(
        CollectionsConfig.parse("*"), MongoConfig.readConfig(options).getCollectionsConfig());
    assertThrows(ConfigException.class, () -> MongoConfig.readConfig(options).getCollectionName());
    assertThrows(
        ConfigException.class, () -> MongoConfig.writeConfig(options).getCollectionsConfig());
    assertThrows(ConfigException.class, () -> MongoConfig.writeConfig(options).getCollectionName());
  }

  @ParameterizedTest
  @MethodSource("optionsMapConfigs")
  void testMongoConfigOptionsParsingErrors(final MongoConfig mongoConfig) {
    assertThrows(ConfigException.class, () -> mongoConfig.getBoolean("string", true));
    assertThrows(ConfigException.class, () -> mongoConfig.getBoolean("string", true));
    assertThrows(ConfigException.class, () -> mongoConfig.getInt("string", 1));
    assertThrows(ConfigException.class, () -> mongoConfig.getLong("string", 1L));
    assertThrows(ConfigException.class, () -> mongoConfig.getDouble("string", 1.0));
  }

  @Test
  void testConnectionStringErrors() {
    Map<String, String> options = new HashMap<>(OPTIONS_CONFIG_MAP);
    options.put(MongoConfig.PREFIX + MongoConfig.CONNECTION_STRING_CONFIG, "invalid");
    assertThrows(ConfigException.class, () -> MongoConfig.readConfig(options));
    assertThrows(ConfigException.class, () -> MongoConfig.writeConfig(options));

    MongoConfig simpleConfig = assertDoesNotThrow(() -> MongoConfig.createConfig(options));
    assertThrows(ConfigException.class, simpleConfig::toReadConfig);
    assertThrows(ConfigException.class, simpleConfig::toWriteConfig);
  }

  @Test
  void testReadConfigStreamFullDocument() {
    ReadConfig readConfig = MongoConfig.readConfig(CONFIG_MAP);
    assertEquals(readConfig.getStreamFullDocument(), FullDocument.DEFAULT);

    readConfig =
        readConfig.withOption(ReadConfig.STREAM_LOOKUP_FULL_DOCUMENT_CONFIG, "updateLookup");
    assertEquals(readConfig.getStreamFullDocument(), FullDocument.UPDATE_LOOKUP);

    readConfig = readConfig.withOption(ReadConfig.STREAM_LOOKUP_FULL_DOCUMENT_CONFIG, "INVALID");
    assertThrows(ConfigException.class, readConfig::getStreamFullDocument);

    readConfig = readConfig.withOption(ReadConfig.STREAM_PUBLISH_FULL_DOCUMENT_ONLY_CONFIG, "true");
    assertEquals(readConfig.getStreamFullDocument(), FullDocument.UPDATE_LOOKUP);
  }

  @Test
  void testMongoConfigStringHidesConnectionString() {
    MongoConfig mongoConfig = MongoConfig.createConfig(CONFIG_MAP);
    assertFalse(mongoConfig.toString().contains("mongodb://"));
    assertFalse(mongoConfig.toReadConfig().toString().contains("mongodb://"));
    assertFalse(mongoConfig.toWriteConfig().toString().contains("mongodb://"));
  }

  @ParameterizedTest
  @MethodSource("optionsMapConfigs")
  void testErrorScenarios(final MongoConfig mongoConfig) {
    assertThrows(ConfigException.class, () -> mongoConfig.getBoolean("string", true));
    assertThrows(ConfigException.class, () -> mongoConfig.getInt("string", 1));
    assertThrows(ConfigException.class, () -> mongoConfig.getLong("string", 1L));
    assertThrows(ConfigException.class, () -> mongoConfig.getDouble("string", 1.0));
  }

  private static Stream<MongoConfig> optionsMapConfigs() {
    MongoConfig simpleMongoConfig = MongoConfig.createConfig(OPTIONS_CONFIG_MAP);
    return Stream.of(simpleMongoConfig.toReadConfig(), simpleMongoConfig.toWriteConfig());
  }
}
