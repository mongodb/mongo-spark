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

import com.mongodb.ClientSessionOptions;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.spark.sql.connector.annotations.ThreadSafe;
import com.mongodb.spark.sql.connector.assertions.Assertions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * A simple cache that handles the acquisition and release of {@code MongoClients}.
 *
 * <p>Handles the creation and closing of {@code MongoClients}. When a {@link MongoClient} is no
 * longer used there is a grace period allowing the {@code MongoClient} to be reused. As Spark
 * Executors can operate on multiple tasks this reduces the cost creating a MongoClient and
 * connections.
 */
@ThreadSafe
final class MongoClientCache {
  private final Map<MongoClientFactory, CachedMongoClient> cache = new HashMap<>();
  private final long keepAliveNanos;
  private final long initialCleanUpDelayMS;
  private final long cleanUpDelayMS;
  private ScheduledExecutorService scheduler;

  static final int INITIAL_CLEANUP_DELAY_MS = 1000;
  static final int CLEANUP_DELAY_MS = 200;

  /**
   * Constructs a new instance.
   *
   * @param keepAliveMS the keep alive duration to use for unused {@code MongoClients}.
   */
  MongoClientCache(final long keepAliveMS) {
    this(keepAliveMS, INITIAL_CLEANUP_DELAY_MS, CLEANUP_DELAY_MS);
  }

  /**
   * Constructs a new instance.
   *
   * @param keepAliveMS the keep alive duration to use for unused {@code MongoClients}.
   */
  @VisibleForTesting
  MongoClientCache(
      final long keepAliveMS, final long initialCleanUpDelayMS, final long cleanUpDelayMS) {
    this.keepAliveNanos = TimeUnit.NANOSECONDS.convert(keepAliveMS, TimeUnit.MILLISECONDS);
    this.initialCleanUpDelayMS = initialCleanUpDelayMS;
    this.cleanUpDelayMS = cleanUpDelayMS;
  }

  /**
   * Acquires a new MongoClient
   *
   * <p>Once the {@link MongoClient} is no longer required, it should be closed by calling {@code
   * mongoClient.close()}.
   *
   * @param mongoClientFactory the factory to use for creating the MongoClient
   * @return the MongoClient from the cache or create a new one using the {@code
   *     MongoClientFactory}.
   */
  synchronized MongoClient acquire(final MongoClientFactory mongoClientFactory) {
    ensureScheduler();
    return cache
        .computeIfAbsent(
            mongoClientFactory,
            factory -> new CachedMongoClient(this, factory.create(), keepAliveNanos))
        .acquire();
  }

  synchronized void shutdown() {
    if (scheduler != null) {
      scheduler.shutdownNow();
      cache.values().forEach(CachedMongoClient::shutdownClose);
      cache.clear();
      scheduler = null;
    }
  }

  private synchronized void checkClientCache() {
    long currentNanos = System.nanoTime();
    cache.entrySet().removeIf(e -> e.getValue().shouldBeRemoved(currentNanos));
    if (cache.entrySet().isEmpty()) {
      shutdown();
    }
  }

  private synchronized void ensureScheduler() {
    if (scheduler == null) {
      scheduler = Executors.newScheduledThreadPool(1);
      scheduler.scheduleWithFixedDelay(
          this::checkClientCache, initialCleanUpDelayMS, cleanUpDelayMS, TimeUnit.MILLISECONDS);
    }
  }

  private static final class CachedMongoClient implements MongoClient {
    private final MongoClientCache cache;
    private final MongoClient wrapped;
    private final long keepAliveNanos;
    private long releasedNanos;
    private int referenceCount;

    private CachedMongoClient(
        final MongoClientCache cache, final MongoClient wrapped, final long keepAliveNanos) {
      this.cache = cache;
      this.wrapped = wrapped;
      this.keepAliveNanos = keepAliveNanos;
      this.releasedNanos = System.nanoTime();
      this.referenceCount = 0;
    }

    private CachedMongoClient acquire() {
      referenceCount += 1;
      return this;
    }

    private void shutdownClose() {
      referenceCount = 0;
      wrapped.close();
    }

    @Override
    public void close() {
      synchronized (cache) {
        cache.ensureScheduler();
        Assertions.ensureState(
            () -> referenceCount > 0, () -> "MongoClient reference count cannot be below zero");
        releasedNanos = System.nanoTime();
        referenceCount -= 1;
      }
    }

    @Override
    public MongoDatabase getDatabase(final String databaseName) {
      return wrapped.getDatabase(databaseName);
    }

    @Override
    public ClientSession startSession() {
      return wrapped.startSession();
    }

    @Override
    public ClientSession startSession(final ClientSessionOptions options) {
      return wrapped.startSession(options);
    }

    @Override
    public MongoIterable<String> listDatabaseNames() {
      return wrapped.listDatabaseNames();
    }

    @Override
    public MongoIterable<String> listDatabaseNames(final ClientSession clientSession) {
      return wrapped.listDatabaseNames(clientSession);
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases() {
      return wrapped.listDatabases();
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases(final ClientSession clientSession) {
      return wrapped.listDatabases(clientSession);
    }

    @Override
    public <TResult> ListDatabasesIterable<TResult> listDatabases(
        final Class<TResult> tResultClass) {
      return wrapped.listDatabases(tResultClass);
    }

    @Override
    public <TResult> ListDatabasesIterable<TResult> listDatabases(
        final ClientSession clientSession, final Class<TResult> tResultClass) {
      return wrapped.listDatabases(clientSession, tResultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch() {
      return wrapped.watch();
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final Class<TResult> tResultClass) {
      return wrapped.watch(tResultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(final List<? extends Bson> pipeline) {
      return wrapped.watch(pipeline);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(
        final List<? extends Bson> pipeline, final Class<TResult> tResultClass) {
      return wrapped.watch(pipeline, tResultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(final ClientSession clientSession) {
      return wrapped.watch(clientSession);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(
        final ClientSession clientSession, final Class<TResult> tResultClass) {
      return wrapped.watch(clientSession, tResultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(
        final ClientSession clientSession, final List<? extends Bson> pipeline) {
      return wrapped.watch(clientSession, pipeline);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(
        final ClientSession clientSession,
        final List<? extends Bson> pipeline,
        final Class<TResult> tResultClass) {
      return wrapped.watch(clientSession, pipeline, tResultClass);
    }

    @Override
    public ClusterDescription getClusterDescription() {
      return wrapped.getClusterDescription();
    }

    private boolean shouldBeRemoved(final long currentNanos) {
      if (referenceCount == 0 && currentNanos - releasedNanos > keepAliveNanos) {
        try {
          wrapped.close();
        } catch (RuntimeException e) {
          // ignore
        }
        return true;
      }
      return false;
    }
  }
}
