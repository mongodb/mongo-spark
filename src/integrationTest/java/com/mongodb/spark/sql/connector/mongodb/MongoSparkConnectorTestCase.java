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

import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.opentest4j.AssertionFailedError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ClusterType;

import com.mongodb.spark.sql.connector.config.MongoConfig;

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
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        fail("Interrupted when checking change stream support");
      }
      clusterType = MONGODB.getMongoClient().getClusterDescription().getType();
      counter++;
    }
    return clusterType == ClusterType.SHARDED || clusterType == ClusterType.REPLICA_SET;
  }

  public CaseInsensitiveStringMap getConnectionProviderOptions() {
    Map<String, String> options = new HashMap<>();
    options.put(
        MongoConfig.PREFIX + MongoConfig.CONNECTION_STRING_CONFIG,
        MONGODB.getConnectionString().toString());
    return new CaseInsensitiveStringMap(options);
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
    retryAssertion(assertion, 5, 2000);
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
        LOGGER.debug("Failed assertion on attempt: {}", counter);
        exception = e;
        try {
          Thread.sleep(timeoutMs);
        } catch (InterruptedException interruptedException) {
          fail("Interrupted when retrying assertion.");
        }
      }
    }
    if (hasError && exception != null) {
      throw exception;
    }
  }
}
