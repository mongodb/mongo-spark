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
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.jetbrains.annotations.TestOnly;

import org.bson.BsonDocument;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;

import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.connection.LazyMongoClientCache;
import com.mongodb.spark.sql.connector.connection.MongoClientFactory;

import scala.Tuple2;

/**
 * The MongoConfig abstract base class
 *
 * <p>The {@link MongoConfig} instance will also include the {@link org.apache.spark.SparkConf} from
 * the active spark session.
 *
 * <p>Any configuration defined in the {@code originals} will overwrite any spark configuration.
 *
 * <p>Any usage specific configuration will overwrite any default scope configuration.
 */
abstract class AbstractMongoConfig implements MongoConfig {

  /** The current usage mode for the configuration. */
  enum UsageMode {
    READ,
    WRITE
  }

  private final Map<String, String> originals;
  private final Map<String, String> options;
  private final UsageMode usageMode;
  private transient MongoClientFactory mongoClientFactory;

  /**
   * Constructs the instance
   *
   * @param originals the original configuration
   * @param usageMode the usage mode (read|write)
   */
  AbstractMongoConfig(final Map<String, String> originals, final UsageMode usageMode) {
    this.originals = unmodifiableMap(originals);
    this.usageMode = usageMode;

    Map<String, String> configOptions = new HashMap<>();
    if (SparkSession.getActiveSession().isDefined()) {
      Map<String, String> configMap =
          Arrays.stream(SparkSession.active().sparkContext().getConf().getAll())
              .collect(toMap(Tuple2::_1, Tuple2::_2));
      configOptions = createUsageOptions(configMap, usageMode);
    }

    configOptions.putAll(createUsageOptions(originals, usageMode));
    this.options = unmodifiableMap(configOptions);
  }

  @Override
  public Map<String, String> getOriginals() {
    return originals;
  }

  @Override
  public Map<String, String> getOptions() {
    return options;
  }

  @Override
  public String getDatabaseName() {
    return Assertions.validateConfig(
        get(DATABASE_NAME_CONFIG),
        Objects::nonNull,
        () -> "Missing configuration for: " + DATABASE_NAME_CONFIG);
  }

  @Override
  public String getCollectionName() {
    return Assertions.validateConfig(
        get(COLLECTION_NAME_CONFIG),
        Objects::nonNull,
        () -> "Missing configuration for: " + COLLECTION_NAME_CONFIG);
  }

  /**
   * Returns a MongoClient
   *
   * <p>Once the {@link MongoClient} is no longer required, it MUST be closed by calling {@code
   * mongoClient.close()}.
   *
   * @return the MongoClient from the cache or create a new one using the {@code
   *     MongoClientFactory}.
   */
  public MongoClient getMongoClient() {
    return LazyMongoClientCache.getMongoClient(getMongoClientFactory());
  }

  /**
   * Runs a function against a {@code MongoClient}
   *
   * @param function the function that is passed the {@code MongoClient}
   * @param <T> The return type
   * @return the result of the function
   */
  public <T> T withClient(final Function<MongoClient, T> function) {
    try (MongoClient client = getMongoClient()) {
      return function.apply(client);
    }
  }

  /**
   * Loans a {@link MongoClient} to the user, does not return a result.
   *
   * @param consumer the consumer of the {@code MongoClient}
   */
  public void doWithClient(final Consumer<MongoClient> consumer) {
    withClient(
        client -> {
          consumer.accept(client);
          return null;
        });
  }

  /**
   * Runs a function against a {@code MongoCollection}
   *
   * @param function the function that is passed the {@code MongoCollection}
   * @param <T> The return type
   * @return the result of the function
   */
  public <T> T withCollection(final Function<MongoCollection<BsonDocument>, T> function) {
    try (MongoClient client = getMongoClient()) {
      return function.apply(
          client
              .getDatabase(getDatabaseName())
              .getCollection(getCollectionName(), BsonDocument.class));
    }
  }

  /**
   * Loans a {@link MongoCollection} to the user, does not return a result.
   *
   * @param consumer the consumer of the {@code MongoCollection<BsonDocument>}
   */
  public void doWithCollection(final Consumer<MongoCollection<BsonDocument>> consumer) {
    withCollection(
        collection -> {
          consumer.accept(collection);
          return null;
        });
  }

  @Override
  public String toString() {
    String cleanedOptions =
        options.entrySet().stream()
            .map(
                e -> {
                  String value = e.getValue();
                  if (e.getKey().contains(CONNECTION_STRING_CONFIG)) {
                    value = "<hidden>";
                  }
                  return e.getKey() + "=" + value;
                })
            .collect(Collectors.joining(", "));
    return "MongoConfig{options=" + cleanedOptions + ", usageMode=" + usageMode + '}';
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
    final AbstractMongoConfig that = (AbstractMongoConfig) o;
    return Objects.equals(options, that.options) && usageMode == that.usageMode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(options, usageMode);
  }

  Map<String, String> withOverrides(final String context, final Map<String, String> overrides) {
    Map<String, String> newOptions = new HashMap<>(originals);
    overrides.forEach(
        (k, v) -> {
          if (!k.startsWith(context)) {
            newOptions.put(context + k, v);
          } else {
            newOptions.put(k, v);
          }
        });
    newOptions.putAll(overrides);
    return newOptions;
  }

  /** @return the {@link MongoClientFactory} for this configuration. */
  private MongoClientFactory getMongoClientFactory() {
    if (mongoClientFactory == null) {
      String mongoClientFactoryName =
          getOptions().getOrDefault(CLIENT_FACTORY_CONFIG, CLIENT_FACTORY_DEFAULT);
      mongoClientFactory =
          ClassHelper.createInstance(
              CLIENT_FACTORY_CONFIG, mongoClientFactoryName, MongoClientFactory.class, this);
    }
    return mongoClientFactory;
  }

  /**
   * Gets the configuration options for the current {@link UsageMode}.
   *
   * <p>Configures any default scoped configs: eg. {@code spark.mongodb.database}
   *
   * <p>Configures any usage specific configs: eg.{@code spark.mongodb.write.database}
   *
   * @param options the case insensitive options
   * @param usageMode the current usage mode
   * @return the configuration options for the usage mode
   */
  private static Map<String, String> createUsageOptions(
      final Map<String, String> options, final UsageMode usageMode) {

    CaseInsensitiveStringMap caseInsensitiveOptions = new CaseInsensitiveStringMap(options);

    String overridePrefix;
    String ignorePrefix;
    switch (usageMode) {
      case READ:
        overridePrefix = READ_PREFIX;
        ignorePrefix = WRITE_PREFIX;
        break;
      case WRITE:
        overridePrefix = WRITE_PREFIX;
        ignorePrefix = READ_PREFIX;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported usage mode");
    }

    List<String> defaults = new ArrayList<>();
    List<String> overrides = new ArrayList<>();
    options.keySet().stream()
        .map(k -> k.toLowerCase(Locale.ROOT))
        .filter(k -> k.startsWith(PREFIX))
        .forEach(
            k -> {
              if (k.startsWith(overridePrefix)) {
                overrides.add(k);
              } else if (!k.startsWith(ignorePrefix)) {
                defaults.add(k);
              }
            });

    Map<String, String> usageSpecificOptions = new HashMap<>();
    // Add any globally scoped options
    addConnectionStringDatabaseAndCollection(PREFIX, caseInsensitiveOptions, usageSpecificOptions);

    defaults.forEach(
        k -> usageSpecificOptions.put(k.substring(PREFIX.length()), caseInsensitiveOptions.get(k)));

    // Add usage specifically scoped options
    addConnectionStringDatabaseAndCollection(
        overridePrefix, caseInsensitiveOptions, usageSpecificOptions);
    overrides.forEach(
        k ->
            usageSpecificOptions.put(
                k.substring(overridePrefix.length()), caseInsensitiveOptions.get(k)));
    return usageSpecificOptions;
  }

  /**
   * Sets any {@link ConnectionString} defined database and collection values for the current scope.
   *
   * @param prefix the current configuration prefix. eg. {@code spark.mongodb.} for the global
   *     scope, {@code spark.mongodb.read.} for the read scope and {@code spark.mongodb.write.} for
   *     the write scope
   * @param options the configuration options
   * @param usageSpecificOptions the usage specific configuration options
   */
  private static void addConnectionStringDatabaseAndCollection(
      final String prefix,
      final CaseInsensitiveStringMap options,
      final Map<String, String> usageSpecificOptions) {
    if (options.containsKey(prefix + MongoConfig.CONNECTION_STRING_CONFIG)) {
      String rawConnectionString = options.get(prefix + MongoConfig.CONNECTION_STRING_CONFIG);
      ConnectionString connectionString =
          Assertions.validateConfig(
              () -> new ConnectionString(rawConnectionString),
              () -> format("Invalid connection string: '%s'", rawConnectionString));
      String databaseName = connectionString.getDatabase();
      if (databaseName != null) {
        usageSpecificOptions.put(DATABASE_NAME_CONFIG, databaseName);
      }
      String collectionName = connectionString.getCollection();
      if (collectionName != null) {
        usageSpecificOptions.put(COLLECTION_NAME_CONFIG, collectionName);
      }
    }
  }
}
