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
import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

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
  private Integer maxWireVersion;
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

  public MongoCollection<Document> getCollection() {
    return getDatabase().getCollection(getCollectionName());
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

  public boolean isOnline() {
    if (online == null) {
      try {
        isMaster();
        online = true;
      } catch (Exception e) {
        online = false;
      }
    }
    return online;
  }

  private Document isMaster() {
    return getMongoClient().getDatabase("admin").runCommand(BsonDocument.parse("{isMaster: 1}"));
  }

  public boolean isReplicaSetOrSharded() {
    Document isMaster = isMaster();
    return isMaster.containsKey("setName") || isMaster.get("msg", "").equals("isdbgrid");
  }

  private static final int THREE_DOT_SIX_WIRE_VERSION = 6;
  private static final int FOUR_DOT_ZERO_WIRE_VERSION = 7;
  private static final int FOUR_DOT_TWO_WIRE_VERSION = 8;
  public static final int FOUR_DOT_FOUR_WIRE_VERSION = 9;

  public boolean isGreaterThanThreeDotSix() {
    return getMaxWireVersion() > THREE_DOT_SIX_WIRE_VERSION;
  }

  public boolean isGreaterThanFourDotZero() {
    return getMaxWireVersion() > FOUR_DOT_ZERO_WIRE_VERSION;
  }

  public boolean isGreaterThanFourDotTwo() {
    return getMaxWireVersion() > FOUR_DOT_TWO_WIRE_VERSION;
  }

  public boolean isGreaterThanFourDotFour() {
    return getMaxWireVersion() > FOUR_DOT_FOUR_WIRE_VERSION;
  }

  public int getMaxWireVersion() {
    if (maxWireVersion == null) {
      maxWireVersion = isMaster().get("maxWireVersion", 0);
    }
    return maxWireVersion;
  }

  public SparkConf getSparkConf() {
    return new SparkConf()
        .setMaster("local")
        .setAppName("MongoSparkConnector")
        .set("spark.driver.allowMultipleContexts", "false")
        .set("spark.sql.allowMultipleContexts", "false")
        .set("spark.app.id", "MongoSparkConnector")
        .set("spark.mongodb.output.uri", getConnectionString().getConnectionString())
        .set("spark.mongodb.output.database", getDatabaseName())
        .set("spark.mongodb.output.collection", getCollectionName());
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
}
