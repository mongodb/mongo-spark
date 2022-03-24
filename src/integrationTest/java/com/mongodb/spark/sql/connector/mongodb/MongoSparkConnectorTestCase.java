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
 */
package com.mongodb.spark.sql.connector.mongodb;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ClusterType;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.bson.BsonDocument;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.opentest4j.AssertionFailedError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.fail;

@MongoDBOnline()
public class MongoSparkConnectorTestCase {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoSparkConnectorTestCase.class);

  @RegisterExtension
  public static final MongoSparkConnectorHelper MONGODB = new MongoSparkConnectorHelper();

  public String getDatabaseName() {
    return MONGODB.getDatabaseName();
  }

  public String getCollectionName() {
    return MONGODB.getCollectionName();
  }

  public MongoDatabase getDatabase() {
    return MONGODB.getDatabase();
  }

  public MongoCollection<BsonDocument> getCollection() {
    return getCollection(getCollectionName());
  }

  public MongoCollection<BsonDocument> getCollection(final String collectionName) {
    return getDatabase().getCollection(collectionName, BsonDocument.class);
  }

  public boolean supportsChangeStreams() {
    ClusterType clusterType = MONGODB.getMongoClient().getClusterDescription().getType();
    int counter = 0;
    while (clusterType == ClusterType.UNKNOWN && counter < 30) {
      sleep(1000, "Interrupted when checking change stream support");
      clusterType = MONGODB.getMongoClient().getClusterDescription().getType();
      counter++;
    }
    return clusterType == ClusterType.SHARDED || clusterType == ClusterType.REPLICA_SET;
  }

  public boolean isSharded() {
    return MONGODB.isSharded();
  }

  public void shardCollection(final MongoNamespace mongoNamespace, final String shardKeyJson) {
    MONGODB.shardCollection(mongoNamespace, shardKeyJson);
  }

  public CaseInsensitiveStringMap getConnectionProviderOptions() {
    Map<String, String> options = new HashMap<>();
    options.put(
        MongoConfig.PREFIX + MongoConfig.CONNECTION_STRING_CONFIG,
        MONGODB.getConnectionString().toString());
    return new CaseInsensitiveStringMap(options);
  }

  public MongoConfig getMongoConfig() {
    Map<String, String> options = new HashMap<>();
    options.put(
        MongoConfig.PREFIX + MongoConfig.CONNECTION_STRING_CONFIG,
        MONGODB.getConnectionString().toString());
    options.put(MongoConfig.PREFIX + MongoConfig.DATABASE_NAME_CONFIG, getDatabaseName());
    options.put(MongoConfig.PREFIX + MongoConfig.COLLECTION_NAME_CONFIG, getCollectionName());
    return MongoConfig.createConfig(options);
  }

  /**
   * Creates sample data
   *
   * @param numberOfDocuments the total number of documents to create
   * @param sizeInMB the total size of the documents
   * @param config the config used for the database and collection
   */
  public void loadSampleData(
      final int numberOfDocuments, final int sizeInMB, final MongoConfig config) {
    MONGODB.loadSampleData(numberOfDocuments, sizeInMB, config);
  }

  public SparkConf getSparkConf() {
    return MONGODB.getSparkConf();
  }

  public SparkSession getOrCreateSparkSession() {
    return getOrCreateSparkSession(getSparkConf());
  }

  public SparkSession getOrCreateSparkSession(final SparkConf sparkConfig) {
    return SparkSession.builder().sparkContext(getOrCreateSparkContext(sparkConfig)).getOrCreate();
  }

  public SparkContext getOrCreateSparkContext(final SparkConf sparkConfig) {
    return MONGODB.getOrCreateSparkContext(sparkConfig);
  }

  public void retryAssertion(final Runnable assertion) {
    retryAssertion(assertion, 10, 2000);
  }

  public void retryAssertion(final Runnable assertion, final int retries, final long timeoutMs) {
    int counter = 0;
    boolean hasError = true;
    AssertionFailedError exception = null;
    while (counter < retries && hasError) {
      try {
        counter++;
        assertion.run();
        hasError = false;
      } catch (AssertionFailedError e) {
        LOGGER.info("Failed assertion on attempt: {}", counter);
        exception = e;
        sleep(timeoutMs, "Interrupted when retrying assertion.");
      }
    }
    if (hasError && exception != null) {
      throw exception;
    }
  }

  private void sleep(final long timeoutMs, final String message) {
    try {
      Thread.sleep(timeoutMs);
    } catch (InterruptedException interruptedException) {
      if (message != null) {
        fail(message);
      }
    }
  }
}
