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

import static com.mongodb.assertions.Assertions.assertNotNull;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toMap;

import com.mongodb.ConnectionString;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.connection.LazyMongoClientCache;
import com.mongodb.spark.sql.connector.connection.MongoClientFactory;
import com.mongodb.spark.sql.connector.exceptions.ConfigException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.bson.BsonDocument;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.TestOnly;
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
  private transient CaseInsensitiveStringMap caseInsensitiveOptions;
  private transient CollectionsConfig collectionsConfig;

  /**
   * Constructs the instance
   *
   * @param originals the original configuration
   * @param usageMode the usage mode (read|write)
   */
  AbstractMongoConfig(final Map<String, String> originals, final UsageMode usageMode) {
    this.originals = unmodifiableMap(originals);
    this.usageMode = usageMode;

    Map<String, String> configOptions = SparkSession.getActiveSession()
        .map(s -> Arrays.stream(s.sparkContext().getConf().getAll())
            .collect(toMap(Tuple2::_1, Tuple2::_2)))
        .map(m -> createUsageOptions(m, usageMode))
        .getOrElse(HashMap::new);
    configOptions.putAll(createUsageOptions(originals, usageMode));
    this.options = unmodifiableMap(configOptions);
  }

  @Override
  public Map<String, String> getOriginals() {
    return originals;
  }

  @Override
  public Map<String, String> getOptions() {
    if (caseInsensitiveOptions == null) {
      caseInsensitiveOptions = new CaseInsensitiveStringMap(options);
    }
    return caseInsensitiveOptions;
  }

  @Override
  public String getDatabaseName() {
    return Assertions.validateConfig(
        get(DATABASE_NAME_CONFIG),
        Objects::nonNull,
        () -> "Missing configuration for: " + DATABASE_NAME_CONFIG);
  }

  /**
   * @throws ConfigException
   * If either {@linkplain CollectionsConfig.Type#MULTIPLE multiple} or {@linkplain CollectionsConfig.Type#ALL all}
   * collections are {@linkplain ReadConfig#getCollectionsConfig() configured} to be {@linkplain Scan scanned}.
   */
  @Override
  public String getCollectionName() {
    CollectionsConfig collectionsConfig = getCollectionsConfig();
    CollectionsConfig.Type type = collectionsConfig.getType();
    if (type == CollectionsConfig.Type.SINGLE) {
      return collectionsConfig.getName();
    } else {
      throw new ConfigException(format(
          "The connector is configured to access %s, which is not supported in the current context",
          getNamespaceDescription()));
    }
  }

  /**
   * @return The namespace description, which is equal to the {@linkplain MongoNamespace#toString() string representation}
   * of {@link #getNamespace()} when a {@linkplain CollectionsConfig.Type#SINGLE single}
   * collection is {@linkplain ReadConfig#getCollectionsConfig() configured} to be {@linkplain Scan scanned}.
   * Unlike {@link #getNamespace()}, this method works even if
   * {@linkplain CollectionsConfig.Type#MULTIPLE multiple} or {@linkplain CollectionsConfig.Type#ALL all}
   * collections are configured to be scanned.
   */
  @ApiStatus.Internal
  public String getNamespaceDescription() {
    return new MongoNamespace(
            getDatabaseName(), getCollectionsConfig().getPartialNamespaceDescription())
        .toString();
  }

  /**
   * @return The {@link CollectionsConfig}.
   * @see #getCollectionName()
   */
  @ApiStatus.Internal
  public CollectionsConfig getCollectionsConfig() {
    if (collectionsConfig == null) {
      collectionsConfig = parseAndValidateCollectionsConfig();
    }
    return assertNotNull(collectionsConfig);
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
    withClient(client -> {
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
   * @throws ConfigException
   * If either {@linkplain CollectionsConfig.Type#MULTIPLE multiple} or {@linkplain CollectionsConfig.Type#ALL all}
   * collections are {@linkplain ReadConfig#getCollectionsConfig() configured} to be {@linkplain Scan scanned}.
   */
  public <T> T withCollection(final Function<MongoCollection<BsonDocument>, T> function) {
    try (MongoClient client = getMongoClient()) {
      return function.apply(client
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
    withCollection(collection -> {
      consumer.accept(collection);
      return null;
    });
  }

  @Override
  public String toString() {
    String cleanedOptions = getOptions().entrySet().stream()
        .map(e -> {
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
    return Objects.equals(getOptions(), that.getOptions()) && usageMode == that.usageMode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getOptions(), usageMode);
  }

  Map<String, String> withOverrides(final String context, final Map<String, String> overrides) {
    Map<String, String> newOptions = new HashMap<>(originals);
    overrides.forEach((k, v) -> {
      if (!k.startsWith(context)) {
        newOptions.put(context + k, v);
      } else {
        newOptions.put(k, v);
      }
    });
    newOptions.putAll(overrides);
    return newOptions;
  }

  CollectionsConfig parseAndValidateCollectionsConfig() {
    return parseCollectionsConfig();
  }

  /** @return the {@link MongoClientFactory} for this configuration. */
  private MongoClientFactory getMongoClientFactory() {
    if (mongoClientFactory == null) {
      String mongoClientFactoryName =
          getOptions().getOrDefault(CLIENT_FACTORY_CONFIG, CLIENT_FACTORY_DEFAULT);
      mongoClientFactory = ClassHelper.createInstance(
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

    CaseInsensitiveStringMap localCaseInsensitiveOptions = new CaseInsensitiveStringMap(options);

    List<String> defaults = new ArrayList<>();
    List<String> overrides = new ArrayList<>();
    localCaseInsensitiveOptions.keySet().stream()
        .filter(k -> k.startsWith(PREFIX))
        .forEach(k -> {
          if (k.startsWith(overridePrefix)) {
            overrides.add(k);
          } else if (!k.startsWith(ignorePrefix)) {
            defaults.add(k);
          }
        });

    Map<String, String> usageSpecificOptions = new HashMap<>();
    // Add any globally scoped options
    addConnectionStringDatabaseAndCollection(
        PREFIX, localCaseInsensitiveOptions, usageSpecificOptions);

    defaults.forEach(k ->
        usageSpecificOptions.put(k.substring(PREFIX.length()), localCaseInsensitiveOptions.get(k)));

    // Add usage specifically scoped options
    addConnectionStringDatabaseAndCollection(
        overridePrefix, localCaseInsensitiveOptions, usageSpecificOptions);
    overrides.forEach(k -> usageSpecificOptions.put(
        k.substring(overridePrefix.length()), localCaseInsensitiveOptions.get(k)));
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
      ConnectionString connectionString = Assertions.validateConfig(
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

  private String getRawCollectionName() {
    return Assertions.validateConfig(
        get(COLLECTION_NAME_CONFIG),
        v -> v != null && !v.isEmpty(),
        () -> "Missing configuration for: " + COLLECTION_NAME_CONFIG);
  }

  private CollectionsConfig parseCollectionsConfig() throws ConfigException {
    try {
      return CollectionsConfig.parse(getRawCollectionName());
    } catch (CollectionsConfig.ParsingException e) {
      throw new ConfigException(e);
    }
  }
}
