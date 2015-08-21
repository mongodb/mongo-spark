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
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

/*
 * These tests assume a single mongod running on localhost:30000
 * with authentication available for a user with name 'test' and password 'password'
 * for the database 'test'
 */
public class MongoRDDTest {
    private String database = "spark_test";
    private String collection = "test";

    private String uri = "mongodb://spark_test:password@localhost:30000/spark_test";

    private String master = "local";
    private String appName = "testApp";

    private SparkConf sparkConf = new SparkConf().setMaster(master)
            .setAppName(appName);
    private SparkContext sc;
    private int partitions = 1;

    private MongoClientProvider clientProvider = new MongoSparkClientProvider(uri);
    private MongoCollectionProvider<Document> collectionProvider =
            new MongoSparkCollectionProvider<>(Document.class, clientProvider, database, collection);

    private String key = "a";
    private List<Document> documents = asList(new Document(key, 0), new Document(key, 1), new Document(key, 2));
    private List<Bson> pipeline = singletonList(new BsonDocument("$match", new BsonDocument(key, new BsonInt32(0))));

    @Before
    public void setUp() {
        MongoClient client = new MongoClient(new MongoClientURI(uri));
        client.getDatabase(database).getCollection(collection).drop();
        client.getDatabase(database).getCollection(collection).insertMany(documents);
        client.getDatabase(database).getCollection(collection).createIndex(new Document(key, 1));
        client.close();
        sc = new SparkContext(sparkConf);
    }

    @After
    public void tearDown() {
        sc.stop();
        sc = null;
    }

    @Test
    public void shouldMakeMongoRDDWithPartitionsAndAggregation() {
        MongoRDD<Document> mongoRdd = new MongoRDD<>(sc, collectionProvider, Document.class, key, partitions, pipeline);

        assertEquals(1, mongoRdd.count());
        assertEquals(documents.get(0), mongoRdd.first());
        assertEquals(partitions, mongoRdd.getPartitions().length);
    }

    @Test
    public void shouldMakeMongoRDDWithPartitions() {
        MongoRDD<Document> mongoRdd = new MongoRDD<>(sc, collectionProvider, Document.class, key, partitions);

        assertEquals(documents.size(), mongoRdd.count());
        assertEquals(documents.get(0), mongoRdd.first());
        assertEquals(partitions, mongoRdd.getPartitions().length);
    }


    @Test
    public void shouldMakeMongoRDDWithAggregation() {
        MongoRDD<Document> mongoRdd = new MongoRDD<>(sc, collectionProvider, Document.class, key, pipeline);

        assertEquals(1, mongoRdd.count());
        assertEquals(documents.get(0), mongoRdd.first());
        assertEquals(sc.defaultParallelism(), mongoRdd.getPartitions().length);
    }

    @Test
    public void shouldMakeMongoRDD() {
        MongoRDD<Document> mongoRdd = new MongoRDD<>(sc, collectionProvider, Document.class, key);

        assertEquals(documents.size(), mongoRdd.count());
        assertEquals(documents.get(0), mongoRdd.first());
        assertEquals(sc.defaultParallelism(), mongoRdd.getPartitions().length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailNullClazz() {
        new MongoRDD<>(sc, collectionProvider, null, key);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailNullProvider() {
        new MongoRDD<>(sc, null, Document.class, key);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailNullSplitKey() {
        new MongoRDD<>(sc, collectionProvider, Document.class, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailLessThanOnePartitions() {
        new MongoRDD<>(sc, collectionProvider, Document.class, key, 0);
    }
}
