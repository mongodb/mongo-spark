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

import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MongoRDDTest {
    private String master = "local";
    private String appName = "testApp";

    private MongoSparkContext msc;

    private String host = "localhost:27017";
    private String database = "test";
    private String collection = "rdd";

    private MongoClientURI uri =
        new MongoClientURI("mongodb://test:password@" + host + "/" + database + "." + collection);
    private MongoClient client = new MongoClient(uri);

    private List<Document> documents = Arrays.asList(new Document("key", 0), new Document("key", 1), new Document("key", 2));

    @Before
    public void setUp() {
        client.getDatabase(uri.getDatabase()).getCollection(uri.getCollection()).drop();
        client.getDatabase(uri.getDatabase()).getCollection(uri.getCollection()).insertMany(documents);
    }

    @After
    public void tearDown() {
        msc.stop();
        msc = null;
    }

    @Test
    public void shouldMakeMongoRDD() {
        SparkContext sc = new SparkContext(master, appName);
        msc = new MongoSparkContext(sc, uri);

        MongoRDD mongoRdd = new MongoRDD(sc, "key", uri.getURI(), database, collection);

        Assert.assertEquals(documents.size(), mongoRdd.count());
        Assert.assertEquals(documents.get(0), mongoRdd.first());
        Assert.assertEquals(1, mongoRdd.getPartitions().length);
    }
}
