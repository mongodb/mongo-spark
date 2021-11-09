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

import static java.util.stream.Collectors.toList;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.jetbrains.annotations.ApiStatus;

import com.mongodb.ConnectionString;
import com.mongodb.MongoNamespace;

import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.connection.DefaultMongoClientFactory;

/**
 * The MongoConfig interface.
 *
 * <p>Provides MongoDB specific configuration.
 */
public interface MongoConfig extends Serializable {

  /**
   * Create a Mongo Configuration that does not yet have a determined read or write use case
   *
   * @param options the configuration options
   * @see
   *     com.mongodb.spark.sql.connector.MongoTableProvider#getTable(org.apache.spark.sql.types.StructType,
   *     org.apache.spark.sql.connector.expressions.Transform[], Map)
   * @return the configuration
   */
  @ApiStatus.Internal
  static MongoConfig createConfig(final Map<String, String> options) {
    return new UnknownUseCaseMongoConfig(options);
  }

  /**
   * Create a Read Configuration
   *
   * @param options the configuration options
   * @return the read configuration
   */
  static ReadConfig readConfig(final Map<String, String> options) {
    return new ReadConfig(options);
  }

  /**
   * Create a Write Configuration
   *
   * @param options the configuration options
   * @return the write configuration
   */
  static WriteConfig writeConfig(final Map<String, String> options) {
    return new WriteConfig(options);
  }

  /**
   * The prefix for all general Spark MongoDB configurations.
   *
   * <p>For example the {@link MongoConfig#CONNECTION_STRING_CONFIG} should be defined as: "{@code
   * spark.mongodb.connection.uri}".
   *
   * <p>{@value}
   */
  String PREFIX = "spark.mongodb.";

  /**
   * The prefix for specific input (write) based configurations.
   *
   * <p>Overrides any configurations that just use the {@link MongoConfig#PREFIX}. For example to
   * override the {@link MongoConfig#CONNECTION_STRING_CONFIG} just for inputting data into MongoDB:
   * "{@code spark.mongodb.input.connection.uri}".
   *
   * <p>{@value}
   */
  String WRITE_PREFIX = PREFIX + "write.";

  /**
   * The prefix for specific output (read) based configurations.
   *
   * <p>Overrides any configurations that just use the {@link MongoConfig#PREFIX}. For example to
   * override the {@link MongoConfig#CONNECTION_STRING_CONFIG} just for outputting data from
   * MongoDB: "{@code spark.mongodb.output.connection.uri}".
   *
   * <p>{@value}
   */
  String READ_PREFIX = PREFIX + "read.";

  /**
   * The MongoClientFactory configuration key
   *
   * <p>The default implementation uses the {@link MongoConfig#CONNECTION_STRING_CONFIG} as the
   * connection string.
   *
   * <p>Custom implementations are allowed and must implement the {@link
   * com.mongodb.spark.sql.connector.connection.MongoClientFactory} interface.
   *
   * <p>{@value}
   */
  String CLIENT_FACTORY_CONFIG = "mongoClientFactory";
  /**
   * The default MongoClientFactory configuration value
   *
   * <p>Requires the {@link MongoConfig#CONNECTION_STRING_CONFIG} for configuring the resulting
   * {@link com.mongodb.client.MongoClient}
   */
  String CLIENT_FACTORY_DEFAULT = DefaultMongoClientFactory.class.getName();

  /**
   * The connection string configuration key
   *
   * <p>{@value}
   */
  String CONNECTION_STRING_CONFIG = "connection.uri";
  /**
   * The default connection string configuration value
   *
   * <p>{@value}
   */
  String CONNECTION_STRING_DEFAULT = "mongodb://localhost:27017/";

  /**
   * The database name config
   *
   * <p>{@value}
   */
  String DATABASE_NAME_CONFIG = "database";

  /**
   * The collection name config
   *
   * <p>{@value}
   */
  String COLLECTION_NAME_CONFIG = "collection";

  /** @return the options for this MongoConfig instance */
  Map<String, String> getOptions();

  /**
   * Return a {@link MongoConfig} instance with the extra options applied.
   *
   * <p>Existing configurations may be overwritten by the new options.
   *
   * @param options the context specific options.
   * @return a new MongoConfig
   */
  MongoConfig withOptions(Map<String, String> options);

  /** @return the original options for this MongoConfig instance */
  Map<String, String> getOriginals();

  /** @return the read config */
  ReadConfig toReadConfig();

  /** @return the write config */
  WriteConfig toWriteConfig();

  /** @return the namespace related to this config */
  MongoNamespace getNamespace();

  /** @return the connection string */
  default ConnectionString getConnectionString() {
    return new ConnectionString(getOrDefault(CONNECTION_STRING_CONFIG, CONNECTION_STRING_DEFAULT));
  }

  /** @return the database name to use for this configuration */
  default String getDatabaseName() {
    return Assertions.validateState(
        get(DATABASE_NAME_CONFIG),
        Objects::nonNull,
        () -> "Missing configuration for: " + DATABASE_NAME_CONFIG);
  }

  /** @return the collection name to use for this configuration */
  default String getCollectionName() {
    return Assertions.validateState(
        get(COLLECTION_NAME_CONFIG),
        Objects::nonNull,
        () -> "Missing configuration for: " + COLLECTION_NAME_CONFIG);
  }

  /**
   * Returns {@code true} if this map contains a mapping for the specified key.
   *
   * @param key key whose presence in this map is to be tested
   * @return {@code true} if this map contains a mapping for the specified key
   */
  default boolean containsKey(final String key) {
    return getOptions().containsKey(key);
  }

  /**
   * Returns the value to which the specified key is mapped
   *
   * @param key the key whose associated value is to be returned. The key match is case-insensitive.
   * @return the value to which the specified key is mapped or null.
   */
  default String get(final String key) {
    return getOptions().get(key.toLowerCase(Locale.ROOT));
  }

  /**
   * Returns the value to which the specified key is mapped, or {@code defaultValue} if this config
   * contains no mapping for the key.
   *
   * <p>Note: The key match is case-insensitive.
   *
   * @param key the key whose associated value is to be returned
   * @param defaultValue the default mapping for the config
   * @return the value to which the specified key is mapped, or {@code defaultValue} if this config
   *     contains no mapping for the key or the mapping returns null. The key match is
   *     case-insensitive.
   * @throws ClassCastException if the key is of an inappropriate type for this map
   */
  default String getOrDefault(final String key, final String defaultValue) {
    return getOptions().getOrDefault(key.toLowerCase(Locale.ROOT), defaultValue);
  }

  /**
   * Returns the boolean value to which the specified key is mapped, or {@code defaultValue} if
   * there is no mapping for the key.
   *
   * @param key the key whose associated value is to be returned
   * @param defaultValue the default mapping for the config
   * @return the boolean value to which the specified key is mapped, or {@code defaultValue} if
   *     there is no mapping for the key. The key match is case-insensitive.
   * @throws IllegalArgumentException if the specified key cannot be converted into a valid boolean
   */
  default boolean getBoolean(final String key, final boolean defaultValue) {
    String value = get(key);
    // We can't use `Boolean.parseBoolean` here, as it returns false for invalid strings.
    if (value == null) {
      return defaultValue;
    } else if (value.equalsIgnoreCase("true")) {
      return true;
    } else if (value.equalsIgnoreCase("false")) {
      return false;
    } else {
      throw new IllegalArgumentException(value + " is not a boolean string.");
    }
  }

  /**
   * Returns the int value to which the specified key is mapped, or {@code defaultValue} if there is
   * no mapping for the key.
   *
   * @param key the key whose associated value is to be returned
   * @param defaultValue the default mapping for the config
   * @return the integer value to which the specified key is mapped, or {@code defaultValue} if
   *     there is no mapping for the key. The key match is case-insensitive.
   * @throws NumberFormatException if the specified key cannot be converted into a valid int
   */
  default int getInt(final String key, final int defaultValue) {
    String value = get(key);
    return value == null ? defaultValue : Integer.parseInt(value);
  }

  /**
   * Returns the long value to which the specified key is mapped, or {@code defaultValue} if there
   * is no mapping for the key.
   *
   * @param key the key whose associated value is to be returned
   * @param defaultValue the default mapping for the config
   * @return the long value to which the specified key is mapped, or {@code defaultValue} if there
   *     is no mapping for the key. The key match is case-insensitive.
   * @throws NumberFormatException if the specified key cannot be converted into a valid long
   */
  default long getLong(final String key, final long defaultValue) {
    String value = get(key);
    return value == null ? defaultValue : Long.parseLong(value);
  }

  /**
   * Returns the double value to which the specified key is mapped, or {@code defaultValue} if there
   * is no mapping for the key.
   *
   * @param key the key whose associated value is to be returned
   * @param defaultValue the default mapping for the config
   * @return the double value to which the specified key is mapped, or {@code defaultValue} if there
   *     is no mapping for the key. The key match is case-insensitive.
   * @throws NumberFormatException if the specified key cannot be converted into a valid double
   */
  default double getDouble(final String key, final double defaultValue) {
    String value = get(key);
    return value == null ? defaultValue : Double.parseDouble(value);
  }

  /**
   * Returns a list of strings from a comma delimited string to which the specified key is mapped,
   * or {@code defaultValue} if there is no mapping for the key.
   *
   * @param key the key whose associated value is to be returned
   * @param defaultValue the default mapping for the config
   * @return a list of strings to which the specified key is mapped, or {@code defaultValue} if
   *     there is no mapping for the key. The key match is case-insensitive.
   */
  default List<String> getList(final String key, final List<String> defaultValue) {
    String value = get(key);
    return value == null
        ? defaultValue
        : Arrays.stream(value.split(",")).map(String::trim).collect(toList());
  }
}
