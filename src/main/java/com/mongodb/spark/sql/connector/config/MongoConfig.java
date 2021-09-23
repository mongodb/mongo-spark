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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.TestOnly;

import com.mongodb.ConnectionString;

import com.mongodb.spark.sql.connector.connection.DefaultMongoClientFactory;
import com.mongodb.spark.sql.connector.connection.MongoClientFactory;

/**
 * The MongoConfig class
 *
 * <p>Provides MongoDB specific configuration. Wraps the {@link CaseInsensitiveStringMap} options
 * provided by Spark.
 */
public final class MongoConfig implements Serializable {

  static final long serialVersionUID = 1L;

  /**
   * The prefix for all general Spark MongoDB configurations.
   *
   * <p>For example the {@link MongoConfig#MONGO_CONNECTION_STRING_CONFIG} should be defined as:
   * "{@code spark.mongodb.connection.uri}".
   */
  public static final String MONGO_PREFIX = "spark.mongodb.";

  /**
   * The prefix for specific input based configurations.
   *
   * <p>Overrides any configurations that just use the {@link MongoConfig#MONGO_PREFIX}. For example
   * to override the {@link MongoConfig#MONGO_CONNECTION_STRING_CONFIG} just for inputting data into
   * MongoDB: "{@code spark.mongodb.input.connection.uri}".
   */
  public static final String MONGO_INPUT_PREFIX = "spark.mongodb.input.";

  /**
   * The prefix for specific output based configurations.
   *
   * <p>Overrides any configurations that just use the {@link MongoConfig#MONGO_PREFIX}. For example
   * to override the {@link MongoConfig#MONGO_CONNECTION_STRING_CONFIG} just for outputting data
   * from MongoDB: "{@code spark.mongodb.output.connection.uri}".
   */
  public static final String MONGO_OUTPUT_PREFIX = "spark.mongodb.output.";

  /**
   * The MongoClientFactory configuration
   *
   * <p>The default implementation uses the {@link MongoConfig#MONGO_CONNECTION_STRING_CONFIG} as
   * the connection string.
   *
   * <p>Custom implementations are allowed and must implement the {@link
   * com.mongodb.spark.sql.connector.connection.MongoClientFactory} interface.
   */
  public static final String MONGO_CLIENT_FACTORY_CONFIG = "mongoClientFactory";
  /**
   * The default MongoClientFactory configuration
   *
   * <p>Requires the {@link MongoConfig#MONGO_CONNECTION_STRING_CONFIG} for configuring the
   * resulting {@link com.mongodb.client.MongoClient}
   */
  public static final String MONGO_CLIENT_FACTORY_DEFAULT =
      DefaultMongoClientFactory.class.getName();

  /** The connection string configuration */
  public static final String MONGO_CONNECTION_STRING_CONFIG = "connection.uri";
  /** The default connection string configuration */
  public static final String MONGO_CONNECTION_STRING_DEFAULT = "mongodb://localhost:27017/";

  /** The current usage mode for the configuration. */
  private enum UsageMode {
    INPUT,
    OUTPUT
  }

  /**
   * Creates a new {@code MongoConfig} for inputting data.
   *
   * @param options the user provided options
   * @return the MongoConfig.
   */
  public static MongoConfig createInputConfig(final CaseInsensitiveStringMap options) {
    return new MongoConfig(options, UsageMode.INPUT);
  }

  /**
   * Creates a new {@code MongoConfig} for outputting data.
   *
   * @param options the user provided options
   * @return the MongoConfig.
   */
  public static MongoConfig createOutputConfig(final CaseInsensitiveStringMap options) {
    return new MongoConfig(options, UsageMode.OUTPUT);
  }

  private final CaseInsensitiveStringMap originals;
  private final CaseInsensitiveStringMap values;
  private final UsageMode usageMode;
  private transient MongoClientFactory mongoClientFactory;

  private MongoConfig(final CaseInsensitiveStringMap originals, final UsageMode usageMode) {
    this.originals = originals;
    this.usageMode = usageMode;

    String ignorePrefix = usageMode == UsageMode.INPUT ? MONGO_OUTPUT_PREFIX : MONGO_INPUT_PREFIX;
    String overridePrefix = usageMode == UsageMode.INPUT ? MONGO_INPUT_PREFIX : MONGO_OUTPUT_PREFIX;

    Map<String, String> usageSpecificSettings = new HashMap<>();
    List<String> defaults = new ArrayList<>();
    List<String> overrides = new ArrayList<>();

    originals.keySet().stream()
        .filter(k -> k.startsWith(MONGO_PREFIX))
        .forEach(
            k -> {
              if (k.startsWith(overridePrefix)) {
                overrides.add(k);
              } else if (!k.startsWith(ignorePrefix)) {
                defaults.add(k);
              }
            });

    defaults.forEach(
        k -> usageSpecificSettings.put(k.substring(MONGO_PREFIX.length()), originals.get(k)));
    overrides.forEach(
        k -> usageSpecificSettings.put(k.substring(overridePrefix.length()), originals.get(k)));

    this.values = new CaseInsensitiveStringMap(usageSpecificSettings);
  }

  /**
   * Convert the configuration for use when inputting data
   *
   * @return the MongoConfig to use.
   */
  public MongoConfig toInputMongoConfig() {
    if (this.usageMode == UsageMode.INPUT) {
      return this;
    }
    return new MongoConfig(originals, UsageMode.INPUT);
  }

  /**
   * Convert the configuration for use when outputting data
   *
   * @return the MongoConfig to use.
   */
  public MongoConfig toOutputMongoConfig() {
    if (this.usageMode == UsageMode.OUTPUT) {
      return this;
    }
    return new MongoConfig(originals, UsageMode.OUTPUT);
  }

  /** @return the {@link MongoClientFactory} to use. */
  @ApiStatus.Internal
  public MongoClientFactory getMongoClientFactory() {
    if (mongoClientFactory == null) {
      String mongoClientFactoryName =
          values.getOrDefault(MONGO_CLIENT_FACTORY_CONFIG, MONGO_CLIENT_FACTORY_DEFAULT);
      mongoClientFactory =
          ClassHelper.createInstance(
              MONGO_CLIENT_FACTORY_CONFIG, mongoClientFactoryName, MongoClientFactory.class);
      mongoClientFactory.configure(this);
    }
    return mongoClientFactory;
  }

  /** @return the connection string */
  public ConnectionString getConnectionString() {
    return new ConnectionString(
        getOrDefault(MONGO_CONNECTION_STRING_CONFIG, MONGO_CONNECTION_STRING_DEFAULT));
  }

  /**
   * Returns the value to which the specified key is mapped
   *
   * @param key the key whose associated value is to be returned
   * @return the value to which the specified key is mapped, or {@code defaultValue} if this config
   *     contains no mapping for the key
   * @throws NullPointerException if the specified key is null and this map does not permit null
   *     keys
   */
  public String get(final String key) {
    return values.get(key);
  }

  /**
   * Returns the value to which the specified key is mapped, or {@code defaultValue} if this config
   * contains no mapping for the key.
   *
   * @param key the key whose associated value is to be returned
   * @param defaultValue the default mapping for the config
   * @return the value to which the specified key is mapped, or {@code defaultValue} if this config
   *     contains no mapping for the key
   * @throws ClassCastException if the key is of an inappropriate type for this map
   * @throws NullPointerException if the specified key is null and this map does not permit null
   *     keys
   */
  public String getOrDefault(final Object key, final String defaultValue) {
    return values.getOrDefault(key, defaultValue);
  }

  /**
   * @param key the key whose associated value is to be returned
   * @param defaultValue the default mapping for the config
   * @return the boolean value to which the specified key is mapped, or defaultValue if there is no
   *     mapping for the key. The key match is case-insensitive.
   */
  public boolean getBoolean(final String key, final boolean defaultValue) {
    return values.getBoolean(key, defaultValue);
  }

  /**
   * @param key the key whose associated value is to be returned
   * @param defaultValue the default mapping for the config
   * @return the integer value to which the specified key is mapped, or defaultValue if there is no
   *     mapping for the key. The key match is case-insensitive.
   */
  public int getInt(final String key, final int defaultValue) {
    return values.getInt(key, defaultValue);
  }

  /**
   * @param key the key whose associated value is to be returned
   * @param defaultValue the default mapping for the config
   * @return the long value to which the specified key is mapped, or defaultValue if there is no
   *     mapping for the key. The key match is case-insensitive.
   */
  public long getLong(final String key, final long defaultValue) {
    return values.getLong(key, defaultValue);
  }

  /**
   * @param key the key whose associated value is to be returned
   * @param defaultValue the default mapping for the config
   * @return the double value to which the specified key is mapped, or defaultValue if there is no
   *     mapping for the key. The key match is case-insensitive.
   */
  public double getDouble(final String key, final double defaultValue) {
    return values.getDouble(key, defaultValue);
  }

  @Override
  public String toString() {
    return "MongoConfig{" + "usageMode=" + usageMode + '}';
  }

  @TestOnly
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final MongoConfig that = (MongoConfig) o;
    return Objects.equals(originals, that.originals) && usageMode == that.usageMode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(originals, usageMode);
  }
}
