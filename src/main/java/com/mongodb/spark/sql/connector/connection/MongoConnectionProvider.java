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

package com.mongodb.spark.sql.connector.connection;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.TestOnly;

import org.bson.BsonDocument;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import com.mongodb.spark.sql.connector.config.MongoConfig;

/**
 * The Mongo Connection Provider
 *
 * <p>Provides loan methods for using a {@link MongoClient}, {@link MongoDatabase} or {@link
 * MongoCollection}. The underlying {@code MongoClient} is cached using the {@link
 * MongoClientCache}.
 */
@ApiStatus.Internal
public final class MongoConnectionProvider implements Serializable {

  static final long serialVersionUID = 1L;
  private static final String SYSTEM_MONGO_CACHE_KEEP_ALIVE_MS_PROPERTY =
      "spark.mongodb.keep_alive_ms";
  private static MongoClientCache mongoClientCache;
  private final MongoConfig mongoConfig;

  /**
   * Constructs a new instance.
   *
   * @param mongoConfig the configuration to use.
   */
  public MongoConnectionProvider(final MongoConfig mongoConfig) {
    this.mongoConfig = mongoConfig;
  }

  /**
   * Loans a {@link MongoClient} to the user, does not return a result.
   *
   * @param consumer the consumer of the {@code MongoClient}
   */
  public void doWithClient(final Consumer<MongoClient> consumer) {
    MongoClient client = getMongoClientCache().acquire(mongoConfig.getMongoClientFactory());
    try {
      consumer.accept(client);
    } finally {
      getMongoClientCache().release(client);
    }
  }

  /**
   * Loans a {@link MongoDatabase} to the user, does not return a result.
   *
   * @param databaseName the database name to use.
   * @param consumer the consumer of the {@code MongoDatabase}
   */
  public void doWithDatabase(final String databaseName, final Consumer<MongoDatabase> consumer) {
    doWithClient(client -> consumer.accept(client.getDatabase(databaseName)));
  }

  /**
   * Loans a {@link MongoCollection} to the user, does not return a result.
   *
   * @param databaseName the database name to use.
   * @param collectionName the database name to use.
   * @param consumer the consumer of the {@code MongoCollection<BsonDocument>}
   */
  public void doWithCollection(
      final String databaseName,
      final String collectionName,
      final Consumer<MongoCollection<BsonDocument>> consumer) {
    doWithDatabase(
        databaseName,
        mongoDatabase ->
            consumer.accept(mongoDatabase.getCollection(collectionName, BsonDocument.class)));
  }

  /**
   * Loans a {@link MongoClient} to the user.
   *
   * @param function the function that is passed the {@code MongoClient}
   * @param <T> The return type
   * @return the result of the function
   */
  public <T> T withClient(final Function<MongoClient, T> function) {
    MongoClient client = getMongoClientCache().acquire(mongoConfig.getMongoClientFactory());
    try {
      return function.apply(client);
    } finally {
      getMongoClientCache().release(client);
    }
  }
  /**
   * Loans a {@link MongoDatabase} to the user.
   *
   * @param databaseName the database name to use.
   * @param function the function that is passed the {@code MongoDatabase}
   * @param <T> The return type
   * @return the result of the function
   */
  public <T> T withDatabase(final String databaseName, final Function<MongoDatabase, T> function) {
    return withClient(client -> function.apply(client.getDatabase(databaseName)));
  }

  /**
   * Loans a {@link MongoCollection} to the user.
   *
   * @param databaseName the database name to use.
   * @param collectionName the database name to use.
   * @param function the function that is passed the {@code MongoCollection<BsonDocument>}
   * @param <T> The return type
   * @return the result of the function
   */
  public <T> T withCollection(
      final String databaseName,
      final String collectionName,
      final Function<MongoCollection<BsonDocument>, T> function) {
    return withDatabase(
        databaseName,
        mongoDatabase ->
            function.apply(mongoDatabase.getCollection(collectionName, BsonDocument.class)));
  }

  /** @return memoize's and returns the {@link MongoClientCache} */
  private static synchronized MongoClientCache getMongoClientCache() {
    if (mongoClientCache == null) {
      int keepAliveMS = 5000;
      try {
        keepAliveMS =
            Integer.parseInt(System.getProperty(SYSTEM_MONGO_CACHE_KEEP_ALIVE_MS_PROPERTY, "5000"));
      } catch (NumberFormatException e) {
        // ignore and use default
      }
      mongoClientCache = new MongoClientCache(keepAliveMS);
      Runtime.getRuntime().addShutdownHook(new ShutdownHook());
    }
    return mongoClientCache;
  }

  /** The shutdown hook that shuts down the {@link MongoClientCache} */
  static class ShutdownHook extends Thread {
    @Override
    public void run() {
      synchronized (MongoConnectionProvider.class) {
        if (mongoClientCache != null) {
          mongoClientCache.shutdown();
          mongoClientCache = null;
        }
      }
    }
  }

  @Override
  public String toString() {
    return "MongoConnectionProvider{" + "mongoConfig=" + mongoConfig + '}';
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
    final MongoConnectionProvider that = (MongoConnectionProvider) o;
    return Objects.equals(mongoConfig, that.mongoConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mongoConfig);
  }
}
