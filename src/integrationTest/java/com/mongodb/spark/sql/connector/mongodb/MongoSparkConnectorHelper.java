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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import com.mongodb.spark.sql.connector.config.MongoConfig;

public class MongoSparkConnectorHelper
    implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback {
  private static final String DEFAULT_URI = "mongodb://localhost:27017";
  public static final String URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";
  public static final String DEFAULT_DATABASE_NAME = "MongoSparkConnectorTest";
  public static final String DEFAULT_COLLECTION_NAME = "coll";

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoSparkConnectorHelper.class);

  private SparkContext sparkContext;
  private ConnectionString connectionString;
  private MongoClient mongoClient;
  private Boolean online;

  public MongoSparkConnectorHelper() {}

  public MongoClient getMongoClient() {
    if (mongoClient == null) {
      mongoClient = MongoClients.create(getConnectionString());
    }
    return mongoClient;
  }

  @Override
  public void beforeAll(final ExtensionContext context) {
    getOrCreateSparkContext(getSparkConf());
  }

  @Override
  public void beforeEach(final ExtensionContext context) {
    if (mongoClient != null) {
      getDatabase().drop();
    }
  }

  @Override
  public void afterEach(final ExtensionContext context) {
    if (mongoClient != null) {
      getDatabase().drop();
    }
  }

  @Override
  public void afterAll(final ExtensionContext context) {
    if (mongoClient != null) {
      mongoClient.close();
      mongoClient = null;
    }
    resetSparkContext();
  }

  public String getDatabaseName() {
    String databaseName = getConnectionString().getDatabase();
    return databaseName != null ? databaseName : DEFAULT_DATABASE_NAME;
  }

  public MongoDatabase getDatabase() {
    return getMongoClient().getDatabase(getDatabaseName());
  }

  public String getCollectionName() {
    String collectionName = getConnectionString().getCollection();
    return collectionName != null ? collectionName : DEFAULT_COLLECTION_NAME;
  }

  public ConnectionString getConnectionString() {
    if (connectionString == null) {
      String mongoURIProperty = System.getProperty(URI_SYSTEM_PROPERTY_NAME);
      String mongoURIString =
          mongoURIProperty == null || mongoURIProperty.isEmpty() ? DEFAULT_URI : mongoURIProperty;
      connectionString = new ConnectionString(mongoURIString);
      LOGGER.info("Connecting to: '{}'", connectionString);
    }
    return connectionString;
  }

  public SparkConf getSparkConf() {
    return new SparkConf()
        .setMaster("local")
        .setAppName("MongoSparkConnector")
        .set("spark.driver.allowMultipleContexts", "false")
        .set("spark.sql.allowMultipleContexts", "false")
        .set("spark.sql.streaming.checkpointLocation", getTempDirectory())
        .set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .set("spark.app.id", "MongoSparkConnector")
        .set(
            MongoConfig.PREFIX + MongoConfig.CONNECTION_STRING_CONFIG,
            getConnectionString().getConnectionString())
        .set(MongoConfig.PREFIX + MongoConfig.DATABASE_NAME_CONFIG, getDatabaseName())
        .set(MongoConfig.PREFIX + MongoConfig.COLLECTION_NAME_CONFIG, getCollectionName());
  }

  public SparkContext getOrCreateSparkContext(final SparkConf sparkConfig) {
    return getOrCreateSparkContext(sparkConfig, false);
  }

  public synchronized SparkContext getOrCreateSparkContext(
      final SparkConf sparkConfig, final boolean customConf) {
    boolean createSparkContext = sparkContext == null || customConf;
    if (createSparkContext) {
      resetSparkContext();
      sparkContext = new SparkContext(sparkConfig);
    }
    return sparkContext;
  }

  synchronized void resetSparkContext() {
    if (sparkContext != null) {
      sparkContext.stop();
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
    }
    sparkContext = null;
  }

  public boolean isOnline() {
    if (online == null) {
      try {
        getMongoClient().getDatabase("admin").runCommand(BsonDocument.parse("{isMaster: 1}"));
        online = true;
      } catch (Exception e) {
        online = false;
      }
    }
    return online;
  }

  private String getTempDirectory() {
    try {
      File tmpDirectory = Files.createTempDirectory("mongo-spark-connector").toFile();
      tmpDirectory.deleteOnExit();
      return tmpDirectory.getAbsolutePath();
    } catch (IOException e) {
      throw new UnsupportedOperationException("Could not create a temp directory", e);
    }
  }
}
