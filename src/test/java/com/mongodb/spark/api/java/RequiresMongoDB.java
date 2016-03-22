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
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;

import java.io.Serializable;

import static org.junit.Assume.assumeTrue;

public abstract class RequiresMongoDB implements Serializable {

    private transient JavaSparkContext jsc;

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

    public JavaSparkContext getJavaSparkContext() {
        if (jsc != null) {
            jsc.stop();
        }
        jsc = new JavaSparkContext(new SparkContext(getSparkConf()));
        return jsc;
    }

    public JavaSparkContext getJavaSparkContext(final String collectionName) {
        if (jsc != null) {
            jsc.stop();
        }
        jsc = new JavaSparkContext(new SparkContext(getSparkConf(collectionName)));
        return jsc;
    }

    public String getDatabaseName() {
        return mongoDBDefaults.DATABASE_NAME();
    }

    public String getCollectionName() {
        return this.getClass().getName();
    }

    @Before
    public void setUp() {
        assumeTrue(mongoDBDefaults.isMongoDBOnline());
        mongoDBDefaults.dropDB();
        jsc = null;
    }

    @After
    public void tearDown() {
        mongoDBDefaults.dropDB();
        if (jsc != null) {
            jsc.stop();
            jsc = null;
        }
    }
}
