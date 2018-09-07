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

package com.mongodb.spark;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.sql.types.BsonCompatibility;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;

import static org.junit.Assume.assumeTrue;

public abstract class JavaRequiresMongoDB implements Serializable {

    private static final TestHelper testHelper = new TestHelper();
    private static boolean customConf;

    public static String getMongoClientURI() {
        return testHelper.getMongoClientURI();
    }

    public static MongoClient getMongoClient() {
        return testHelper.getMongoClient();
    }

    public static MongoDatabase getDatabase() {
        return getMongoClient().getDatabase(testHelper.DATABASE_NAME());
    }

    public static MongoCollection<Document> getCollection() {
        return getDatabase().getCollection(getCollectionName());
    }

    public static SparkConf getSparkConf() {
        return getSparkConf(getCollectionName());
    }

    public static SparkConf getSparkConf(final String collectionName) {
        return testHelper.getSparkConf(collectionName);
    }

    public static SparkSession getSparkSession() {
        return SparkSession.builder().config(getSparkConf()).getOrCreate();
    }

    public static String getDatabaseName() {
        return testHelper.DATABASE_NAME();
    }

    public static String getCollectionName() {
        return MethodHandles.lookup().lookupClass().getSimpleName();
    }

    public static JavaSparkContext getJavaSparkContext() {
        return new JavaSparkContext(TestHelper.getOrCreateSparkContext(getSparkConf(), false));
    }

    public static JavaSparkContext getJavaSparkContext(final SparkConf sparkConf) {
        return new JavaSparkContext(TestHelper.getOrCreateSparkContext(sparkConf, true));
    }

    public StructType ObjectIdStruct() {
        return BsonCompatibility.ObjectId$.MODULE$.structType();
    }

    public static Function<String, Document> JsonToDocument = new Function<String, Document>() {
        @Override
        public Document call(final String json) throws Exception {
            return Document.parse(json);
        }
    };

    @Before
    public void setUp() {
        assumeTrue(testHelper.isMongoDBOnline());
        testHelper.dropDB();
    }

    @After
    public void tearDown() {
        testHelper.dropDB();
    }

    @AfterClass
    public static void afterClass() {
        TestHelper.resetSparkContext();
        testHelper.dropDB();
    }
}
