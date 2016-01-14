/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.spark.api.java;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoDBDefaults;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;

import java.io.Serializable;

import static org.junit.Assume.assumeTrue;

public abstract class RequiresMongoDB implements Serializable {

    private transient SparkContext sc;

    private static final MongoDBDefaults mongoDBDefaults = new MongoDBDefaults();

    public String getMongoClientURI() {
        return mongoDBDefaults.getMongoClientURI();
    }

    public MongoClient getMongoClient() {
        return mongoDBDefaults.getMongoClient();
    }

    public MongoDatabase getDatabase() {
        return getMongoClient().getDatabase(mongoDBDefaults.DATABASE_NAME());
    }

    public MongoCollection<Document> getCollection() {
        return getDatabase().getCollection(getCollectionName());
    }

    public SparkConf getSparkConf() {
        return getSparkConf(getCollectionName());
    }

    public SparkConf getSparkConf(final String collectionName) {
        return mongoDBDefaults.getSparkConf(collectionName);
    }

    public SparkContext getSparkContext() {
        if (sc != null) {
            sc.stop();
        }
        sc = new SparkContext(getSparkConf());
        return sc;
    }

    public SparkContext getSparkContext(final String collectionName) {
        if (sc != null) {
            sc.stop();
        }
        sc = new SparkContext(getSparkConf(collectionName));
        return sc;
    }

    public String getCollectionName() {
        return this.getClass().getName();
    }

    @Before
    public void setUp() {
        assumeTrue(mongoDBDefaults.isMongoDBOnline());
        mongoDBDefaults.dropDB();
        sc = null;
    }

    @After
    public void tearDown() {
        mongoDBDefaults.dropDB();
        if (sc != null) {
            sc.stop();
            sc = null;
        }
    }
}
