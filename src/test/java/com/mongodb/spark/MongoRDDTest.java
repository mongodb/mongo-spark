/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

package com.mongodb.spark;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.apache.spark.SparkContext;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;

public class MongoRDDTest {
    private String root = "mongodb://";
    private String host = "localhost:27017";
    private String credentials = "test:password@";
    private String database = "test";
    private String collection = "rdd";

    private MongoClientURI uri =
        new MongoClientURI(root + credentials + host + "/" + database + "." + collection);
    private MongoClient client = new MongoClient(uri);

    private String master = "local";
    private String appName = "testApp";

    private SparkContext sc;
    private int partitions = 2;

    private String key = "a";
    private List<Document> documents = Arrays.asList(new Document(key, 0), new Document(key, 1), new Document(key, 2));
    private BsonDocument query = new BsonDocument(key, new BsonInt32(0));
    private List<BsonDocument> pipeline = singletonList(new BsonDocument("$project", new BsonDocument(key, new BsonInt32(1))));

    @Before
    public void setUp() {
        client.getDatabase(uri.getDatabase()).getCollection(uri.getCollection()).drop();
        client.getDatabase(uri.getDatabase()).getCollection(uri.getCollection()).insertMany(documents);
        sc = new SparkContext(master, appName);
    }

    @After
    public void tearDown() {
        sc.stop();
        sc = null;
    }

    @Test
    public void shouldMakeMongoRDDWithPartitionsAndQuery() {
        MongoRDD<Document> mongoRdd = new MongoRDD<>(sc, uri, Document.class, partitions, query);

        Assert.assertEquals(1, mongoRdd.count());
        Assert.assertEquals(documents.get(0), mongoRdd.first());
        Assert.assertEquals(1, mongoRdd.getPartitions().length); // TODO: check actual num partitions once partitioning is implemented
    }

    @Test
    public void shouldMakeMongoRDDWithPartitionsAndAggregation() {
        MongoRDD<Document> mongoRdd = new MongoRDD<>(sc, uri, Document.class, partitions, pipeline);

        Assert.assertEquals(documents.size(), mongoRdd.count());
        Assert.assertEquals(documents.get(0), mongoRdd.first());
        Assert.assertEquals(1, mongoRdd.getPartitions().length); // TODO: check actual num partitions once partitioning is implemented
    }

    @Test
    public void shouldMakeMongoRDDWithPartitions() {
        MongoRDD<Document> mongoRdd = new MongoRDD<>(sc, uri, Document.class, partitions);

        Assert.assertEquals(documents.size(), mongoRdd.count());
        Assert.assertEquals(documents.get(0), mongoRdd.first());
        Assert.assertEquals(1, mongoRdd.getPartitions().length); // TODO: check actual num partitions once partitioning is implemented
    }

    @Test
    public void shouldMakeMongoRDDWithQuery() {
        MongoRDD<Document> mongoRdd = new MongoRDD<>(sc, uri, Document.class, query);

        Assert.assertEquals(1, mongoRdd.count());
        Assert.assertEquals(documents.get(0), mongoRdd.first());
        Assert.assertEquals(sc.defaultParallelism(), mongoRdd.getPartitions().length);
    }

    @Test
    public void shouldMakeMongoRDD() {
        MongoRDD<Document> mongoRdd = new MongoRDD<>(sc, uri, Document.class);

        Assert.assertEquals(documents.size(), mongoRdd.count());
        Assert.assertEquals(documents.get(0), mongoRdd.first());
        Assert.assertEquals(sc.defaultParallelism(), mongoRdd.getPartitions().length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailNullClazz() {
        new MongoRDD<>(sc, uri, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailNullURI() {
        new MongoRDD<>(sc, null, Document.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailNullURIDatabase() {
        new MongoRDD<>(sc, new MongoClientURI(root + credentials + host), Document.class).collect();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailNullURICollection() {
        new MongoRDD<>(sc, new MongoClientURI(root + credentials + host + "/" + database), Document.class).collect();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailNonnegativePartitions() {
        new MongoRDD<>(sc, uri, Document.class, 0);
    }
}
