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

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import com.mongodb.ConnectionString;
import com.mongodb.MongoNamespace;
import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.connection.DefaultMongoClientFactory;
import com.mongodb.spark.sql.connector.exceptions.ConfigException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.bson.BsonString;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

/**
 * The MongoConfig interface.
 *
 * <p>Provides MongoDB specific configuration.
 */
public interface MongoConfig extends Serializable {

  /**
   * Create a Mongo Configuration that does not yet have a fixed use case
   *
   * <p>Can be used to create partial configurations or a MongoConfig which will later be turned
   * into a {@code ReadConfig} or {@code WriteConfig}
   *
   * @param options the configuration options
   * @see
   *     com.mongodb.spark.sql.connector.MongoTableProvider#getTable(org.apache.spark.sql.types.StructType,
   *     org.apache.spark.sql.connector.expressions.Transform[], Map)
   * @return a simple configuration
   */
  @ApiStatus.Internal
  static MongoConfig createConfig(final Map<String, String> options) {
    return new SimpleMongoConfig(options);
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

  // This documentation links to `WriteBuilder` instead of `Write`
  // because `Write` was added in spark-catalyst 3.2.0, and we must support 3.1.2.
  /**
   * A configuration of the set of collections for {@linkplain WriteBuilder writing} to / {@linkplain Scan scanning} from.
   * When configuring a {@linkplain WriteBuilder write}, only a single collection name is supported.
   * When configuring a {@linkplain Scan scan},
   * the following {@linkplain CollectionsConfig.Type configuration types} are supported:
   * <ul>
   *     <li>
   *     A {@linkplain CollectionsConfig.Type#SINGLE single} collection name.</li>
   *     <li>
   *     {@linkplain CollectionsConfig.Type#MULTIPLE Multiple} collection names separated with comma ({@code ','}).
   *     For example, {@code "collectionA,collectionB"}.
   *     Note how the values are separated only with comma, and there is no space ({@code ' '}) accompanying it.
   *     Specifying a space makes it part of a collection name:
   *     {@code "collectionA, collectionB"}---collections {@code "collectionA"} and {@code " collectionB"}.</li>
   *     <li>
   *     {@linkplain CollectionsConfig.Type#ALL All} collections in the {@linkplain #getDatabaseName() database},
   *     in which case one must specify a string consisting of a single asterisk ({@code '*'}).</li>
   * </ul>
   * Note that if a collection name contains comma ({@code ','}), reverse solidus ({@code '\'}), or starts with asterisk ({@code '*'}),
   * such a character must be escaped with reverse solidus ({@code '\'}). Examples:
   * <ul>
   *     <li>
   *     {@code "mass\, kg"}---a single collection named {@code "mass, kg"}.</li>
   *     <li>
   *     {@code "\*"}---a single collection named {@code "*"}.</li>
   *     <li>
   *     {@code "\\"}---a single collection named {@code "\"}.
   *     Note that if the value is specified as a string literal in Java code, then each reverse solidus has to be further escaped,
   *     leading to having to specify {@code "\\\\"}.</li>
   * </ul>
   *
   * <p>{@value}
   */
  String COLLECTION_NAME_CONFIG = "collection";

  /**
   * Add a comment to mongodb operations
   *
   * <p>Allows debugging and profiling queries from the connector.
   *
   * <p>{@value}
   *
   * <p>See: <a
   * href="https://www.mongodb.com/docs/current/reference/operator/query/comment/">$comment</a>
   *
   * <p>Note: Requires MongoDB 4.6+
   */
  String COMMENT_CONFIG = "comment";

  /** @return the options for this MongoConfig instance */
  Map<String, String> getOptions();

  /**
   * Return a {@link MongoConfig} instance with the extra options applied.
   *
   * <p>Existing configurations may be overwritten by the new options.
   *
   * @param key the key to add
   * @param value the value to add
   * @return a new MongoConfig
   */
  MongoConfig withOption(String key, String value);

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

  /** @return the connection string */
  default ConnectionString getConnectionString() {
    return new ConnectionString(getOrDefault(CONNECTION_STRING_CONFIG, CONNECTION_STRING_DEFAULT));
  }

  /**
   * @return the namespace related to this config
   * @throws ConfigException
   * If either {@linkplain CollectionsConfig.Type#MULTIPLE multiple} or {@linkplain CollectionsConfig.Type#ALL all}
   * collections are {@linkplain ReadConfig#getCollectionsConfig() configured} to be {@linkplain Scan scanned}.
   */
  default MongoNamespace getNamespace() {
    return new MongoNamespace(getDatabaseName(), getCollectionName());
  }

  /** @return the database name to use for this configuration */
  default String getDatabaseName() {
    throw new UnsupportedOperationException(
        "Unspecialized MongoConfig. Use `mongoConfig.toReadConfig()` "
            + "or `mongoConfig.toWriteConfig()` to specialize");
  }

  /** @return the collection name to use for this configuration */
  default String getCollectionName() {
    throw new UnsupportedOperationException(
        "Unspecialized MongoConfig. Use `mongoConfig.toReadConfig()` "
            + "or `mongoConfig.toWriteConfig()` to specialize");
  }

  /** @return the read config */
  default ReadConfig toReadConfig() {
    if (this instanceof ReadConfig) {
      return (ReadConfig) this;
    }
    return readConfig(getOriginals());
  }

  /** @return the write config */
  default WriteConfig toWriteConfig() {
    if (this instanceof WriteConfig) {
      return (WriteConfig) this;
    }
    return writeConfig(getOriginals());
  }

  /**
   * Gets all configurations starting with a prefix.
   *
   * <p>Note: The prefix will be removed from the keys in the resulting configuration.
   *
   * @param prefix for the configuration options that should be returned
   * @return the configuration options that started with the prefix
   */
  default MongoConfig subConfiguration(final String prefix) {
    Assertions.ensureState(
        () -> prefix.endsWith("."),
        () -> format("Invalid configuration prefix `%s`, it must end with a '.'", prefix));
    return MongoConfig.createConfig(getOptions().entrySet().stream()
        .filter(
            e -> e.getKey().toLowerCase(Locale.ROOT).startsWith(prefix.toLowerCase(Locale.ROOT)))
        .collect(
            Collectors.toMap(e -> e.getKey().substring(prefix.length()), Map.Entry::getValue)));
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
      throw new ConfigException(value + " is not a boolean string.");
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
    return value == null
        ? defaultValue
        : Assertions.validateConfig(
            () -> Integer.parseInt(value),
            () -> format("%s did not contain a valid int, got: %s", key, value));
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
    return value == null
        ? defaultValue
        : Assertions.validateConfig(
            () -> Long.parseLong(value),
            () -> format("%s did not contain a valid long, got: %s", key, value));
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
    return value == null
        ? defaultValue
        : Assertions.validateConfig(
            () -> Double.parseDouble(value),
            () -> format("%s did not contain a valid double, got: %s", key, value));
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

  /** @return the comment to be associated with an operation or null if not set */
  @Nullable
  default BsonString getComment() {
    return containsKey(COMMENT_CONFIG) ? new BsonString(get(COMMENT_CONFIG)) : null;
  }
}
