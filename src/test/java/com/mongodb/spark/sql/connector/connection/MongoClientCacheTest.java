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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.mongodb.client.MongoClient;

@ExtendWith(MockitoExtension.class)
public class MongoClientCacheTest {

  @Mock private MongoClientFactory mongoClientFactory;
  @Mock private MongoClient mongoClient;

  @Test
  void testNormalUsecase() {
    MongoClientCache mongoClientCache = new MongoClientCache(0, 0, 100);
    when(mongoClientFactory.create()).thenReturn(mongoClient);

    // Create clients
    MongoClient client1 = mongoClientCache.acquire(mongoClientFactory);
    MongoClient client2 = mongoClientCache.acquire(mongoClientFactory);

    // Ensure only a single client was created
    verify(mongoClientFactory, times(1)).create();

    // Ensure release clients eventually closes the client
    mongoClientCache.release(client1);
    mongoClientCache.release(client2);
    sleep(200);
    verify(mongoClient, times(1)).close();
  }

  @Test
  void testKeepAliveReuseOfClient() {
    MongoClientCache mongoClientCache = new MongoClientCache(500, 0, 200);
    when(mongoClientFactory.create()).thenReturn(mongoClient);

    // Create clients
    MongoClient client1 = mongoClientCache.acquire(mongoClientFactory);
    MongoClient client2 = mongoClientCache.acquire(mongoClientFactory);

    // Ensure only a single client was created
    verify(mongoClientFactory, times(1)).create();

    // Ensure release clients eventually closes the client
    mongoClientCache.release(client1);
    mongoClientCache.release(client2);
    sleep(250);
    verify(mongoClient, times(0)).close();

    // Ensure acquiring a new MongoClient within the clean
    MongoClient client3 = mongoClientCache.acquire(mongoClientFactory);
    verify(mongoClientFactory, times(1)).create();
    verify(mongoClient, times(0)).close();

    // Ensure release clients eventually closes the client
    mongoClientCache.release(client3);
    sleep(1000);
    verify(mongoClient, times(1)).close();
  }

  @Test
  void testShutdown() {
    MongoClientCache mongoClientCache = new MongoClientCache(500, 0, 200);
    when(mongoClientFactory.create()).thenReturn(mongoClient);

    // Create clients
    mongoClientCache.acquire(mongoClientFactory);
    mongoClientCache.acquire(mongoClientFactory);
    mongoClientCache.acquire(mongoClientFactory);

    verify(mongoClientFactory, times(1)).create();

    // shutdown
    mongoClientCache.shutdown();
    verify(mongoClient, times(1)).close();

    // Verify behaviour after shutdown
    assertThrows(IllegalStateException.class, () -> mongoClientCache.acquire(mongoClientFactory));
    assertThrows(IllegalStateException.class, () -> mongoClientCache.release(mongoClient));
    assertDoesNotThrow(mongoClientCache::shutdown);
  }

  private void sleep(final long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
