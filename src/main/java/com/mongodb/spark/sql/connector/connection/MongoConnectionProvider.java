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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.jetbrains.annotations.ApiStatus;

import org.bson.BsonDocument;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/** Temporary code to be rewritten as part of SPARK-308 */
@ApiStatus.Experimental
public class MongoConnectionProvider {
  // CHECKSTYLE:OFF:JavadocMethod
  private final Map<String, String> options;

  public MongoConnectionProvider(final Map<String, String> options) {
    this.options = new HashMap<>(options);
  }

  public void doWithClient(final Consumer<MongoClient> consumer) {
    String connectionURI = options.getOrDefault("connection.uri", "mongodb://localhost:27017/");
    try (MongoClient client = MongoClients.create(connectionURI)) {
      consumer.accept(client);
    }
  }

  public void doWithDatabase(final String databaseName, final Consumer<MongoDatabase> consumer) {
    doWithClient(client -> consumer.accept(client.getDatabase(databaseName)));
  }

  public void doWithCollection(
      final String databaseName,
      final String collectionName,
      final Consumer<MongoCollection<BsonDocument>> consumer) {
    doWithDatabase(
        databaseName,
        mongoDatabase ->
            consumer.accept(mongoDatabase.getCollection(collectionName, BsonDocument.class)));
  }

  public <T> T withClient(final Function<MongoClient, T> function) {
    String connectionURI = options.getOrDefault("connection.uri", "mongodb://localhost:27017/");
    try (MongoClient client = MongoClients.create(connectionURI)) {
      return function.apply(client);
    }
  }

  public <T> T withDatabase(final String databaseName, final Function<MongoDatabase, T> function) {
    return withClient(client -> function.apply(client.getDatabase(databaseName)));
  }

  public <T> T withCollection(
      final String databaseName,
      final String collectionName,
      final Function<MongoCollection<BsonDocument>, T> function) {
    return withDatabase(
        databaseName,
        mongoDatabase ->
            function.apply(mongoDatabase.getCollection(collectionName, BsonDocument.class)));
  }
  // CHECKSTYLE.ON:JavadocMethod
}
