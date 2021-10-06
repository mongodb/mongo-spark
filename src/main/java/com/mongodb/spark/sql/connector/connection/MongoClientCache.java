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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;

import com.mongodb.spark.sql.connector.assertions.Assertions;

/**
 * A simple cache that handles the acquisition and release of {@code MongoClients}.
 *
 * <p>Handles the creation and closing of {@code MongoClients}. When a {@link MongoClient} is no
 * longer used there is a grace period allowing the {@code MongoClient} to be reused. As Spark
 * Executors can operate on multiple tasks this reduces the cost creating a MongoClient and
 * connections.
 */
final class MongoClientCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoClientCache.class);
  private final List<MongoClientCacheItem> cacheList = new ArrayList<>();
  private final long keepAliveNanos;
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private boolean isAvailable;

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
    scheduler.scheduleWithFixedDelay(
        this::checkClientCache, initialCleanUpDelayMS, cleanUpDelayMS, TimeUnit.MILLISECONDS);
    isAvailable = true;
  }

  /**
   * Acquires a new MongoClient
   *
   * @param mongoClientFactory the factory to use for creating the MongoClient
   * @return the MongoClient from the cache or create a new one using the {@code
   *     MongoClientFactory}.
   */
  synchronized MongoClient acquire(final MongoClientFactory mongoClientFactory) {
    assertIsAvailable();
    return cacheList.stream()
        .filter(c -> Objects.equals(c.mongoClientFactory, mongoClientFactory))
        .findFirst()
        .orElseGet(
            () -> {
              MongoClientCacheItem mongoClientCacheItem =
                  new MongoClientCacheItem(mongoClientFactory, keepAliveNanos);
              cacheList.add(mongoClientCacheItem);
              return mongoClientCacheItem;
            })
        .acquire();
  }

  /**
   * Releases the MongoClient
   *
   * @param mongoClient to release
   */
  synchronized void release(final MongoClient mongoClient) {
    assertIsAvailable();
    cacheList.stream()
        .filter(c -> c.mongoClient == mongoClient)
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("MongoClient not in the cache."))
        .release();
  }

  synchronized void shutdown() {
    if (isAvailable) {
      scheduler.shutdownNow();
      cacheList.forEach(
          item -> {
            try {
              item.mongoClient.close();
            } catch (RuntimeException e) {
              LOGGER.info("Error when shutting down client: {}", e.getMessage());
            }
          });
      cacheList.clear();
      isAvailable = false;
    }
  }

  private synchronized void checkClientCache() {
    cacheList.removeIf(cacheItem -> cacheItem.shouldBeRemoved(System.nanoTime()));
  }

  private void assertIsAvailable() {
    Assertions.ensureState(
        () -> isAvailable, "The MongoClientCache has been shutdown and is no longer available");
  }

  private static final class MongoClientCacheItem {
    private final MongoClientFactory mongoClientFactory;
    private final long keepAliveNanos;
    private final MongoClient mongoClient;
    private boolean acquired;
    private long releasedNanos;
    private int referenceCount;

    private MongoClientCacheItem(
        final MongoClientFactory mongoClientFactory, final long keepAliveNanos) {
      this.mongoClientFactory = mongoClientFactory;
      this.mongoClient = mongoClientFactory.create();
      this.keepAliveNanos = keepAliveNanos;
      this.releasedNanos = System.nanoTime();
      this.referenceCount = 0;
    }

    private MongoClient acquire() {
      referenceCount += 1;
      acquired = true;
      return mongoClient;
    }

    private void release() {
      Assertions.ensureState(
          () -> referenceCount > 0, "MongoClient reference count cannot be below zero");
      releasedNanos = System.nanoTime();
      referenceCount -= 1;
    }

    private boolean shouldBeRemoved(final long currentNanos) {
      if (acquired && referenceCount == 0 && currentNanos - releasedNanos > keepAliveNanos) {
        try {
          mongoClient.close();
        } catch (RuntimeException e) {
          // ignore
        }
        return true;
      }
      return false;
    }
  }
}
