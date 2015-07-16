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
import com.mongodb.spark.MongoWriter.WriteMode;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.reflect.ClassTag$;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class MongoRDDTest {
    private String root = "mongodb://";
    private String host = "localhost:27017";
    private String username = "test";
    private String password = "password";
    private String database = "test";
    private String collection = "rdd";

    private String uri = root + username + ":" + password + "@" + host + "/" + database + "." + collection;

    private String master = "local";
    private String appName = "testApp";

    private SparkConf sparkConf = new SparkConf().setMaster(master)
                                                 .setAppName(appName);
    private SparkContext sc;
    private int partitions = 1;

    private MongoClientFactory clientFactory = new MongoSparkClientFactory(uri);
    private MongoCollectionFactory<Document> collectionFactory =
            new MongoSparkCollectionFactory<>(Document.class, clientFactory, database, collection);

    private String key = "a";
    private List<Document> documents = asList(new Document(key, 0), new Document(key, 1), new Document(key, 2));
    private Bson query = new BsonDocument(key, new BsonInt32(0));
    private List<Bson> pipeline = singletonList(new BsonDocument("$project", new BsonDocument(key, new BsonInt32(1))));

    @Before
    public void setUp() {
        MongoClient client = new MongoClient(new MongoClientURI(uri));
        client.getDatabase(database).getCollection(collection).drop();
        client.getDatabase(database).getCollection(collection).insertMany(documents);
        client.close();
        sc = new SparkContext(sparkConf);
    }

    @After
    public void tearDown() {
        sc.stop();
        sc = null;
    }

    @Test
    public void shouldMakeMongoRDDWithPartitionsAndQuery() {
        MongoRDD<Document> mongoRdd = new MongoRDD<>(sc, collectionFactory, Document.class, partitions, query);

        assertEquals(1, mongoRdd.count());
        assertEquals(documents.get(0), mongoRdd.first());
        assertEquals(partitions, mongoRdd.getPartitions().length);
    }

    @Test
    public void shouldMakeMongoRDDWithPartitionsAndAggregation() {
        MongoRDD<Document> mongoRdd = new MongoRDD<>(sc, collectionFactory, Document.class, partitions, pipeline);

        assertEquals(documents.size(), mongoRdd.count());
        assertEquals(documents.get(0), mongoRdd.first());
        assertEquals(partitions, mongoRdd.getPartitions().length);
    }

    @Test
    public void shouldMakeMongoRDDWithPartitions() {
        MongoRDD<Document> mongoRdd = new MongoRDD<>(sc, collectionFactory, Document.class, partitions);

        assertEquals(documents.size(), mongoRdd.count());
        assertEquals(documents.get(0), mongoRdd.first());
        assertEquals(partitions, mongoRdd.getPartitions().length);
    }

    @Test
    public void shouldMakeMongoRDDWithQuery() {
        MongoRDD<Document> mongoRdd = new MongoRDD<>(sc, collectionFactory, Document.class, query);

        assertEquals(1, mongoRdd.count());
        assertEquals(documents.get(0), mongoRdd.first());
        assertEquals(sc.defaultParallelism(), mongoRdd.getPartitions().length);
    }

    @Test
    public void shouldMakeMongoRDDWithAggregation() {
        MongoRDD<Document> mongoRdd = new MongoRDD<>(sc, collectionFactory, Document.class, pipeline);

        assertEquals(documents.size(), mongoRdd.count());
        assertEquals(documents.get(0), mongoRdd.first());
        assertEquals(sc.defaultParallelism(), mongoRdd.getPartitions().length);
    }

    @Test
    public void shouldMakeMongoRDD() {
        MongoRDD<Document> mongoRdd = new MongoRDD<>(sc, collectionFactory, Document.class);

        assertEquals(documents.size(), mongoRdd.count());
        assertEquals(documents.get(0), mongoRdd.first());
        assertEquals(sc.defaultParallelism(), mongoRdd.getPartitions().length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailNullClazz() {
        new MongoRDD<>(sc, collectionFactory, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailNullFactory() {
        new MongoRDD<>(sc, null, Document.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailLessThanOnePartitions() {
        new MongoRDD<>(sc, collectionFactory, Document.class, 0);
    }

    @Test
    public void shouldWriteToMongoSimple() {
        RDD<Document> mongoRdd = new MongoRDD<>(sc, collectionFactory, Document.class)
                                     .map(new GetSingleKeyValueDocument(key), ClassTag$.MODULE$.apply(Document.class));

        MongoRDD.toMongoCollection(mongoRdd, collectionFactory, WriteMode.SIMPLE);

        assertEquals(2 * documents.size(), collectionFactory.getCollection().count());
    }

    @Test
    public void shouldWriteToMongoBulkUnordered() {
        RDD<Document> mongoRdd = new MongoRDD<>(sc, collectionFactory, Document.class)
                                     .map(new GetSingleKeyValueDocument(key), ClassTag$.MODULE$.apply(Document.class));

        MongoRDD.toMongoCollection(mongoRdd, collectionFactory, WriteMode.BULK_UNORDERED);

        assertEquals(2 * documents.size(), collectionFactory.getCollection().count());
    }

    @Test
    public void shouldWriteToMongoBulkOrdered() {
        RDD<Document> mongoRdd = new MongoRDD<>(sc, collectionFactory, Document.class)
                                     .map(new GetSingleKeyValueDocument(key), ClassTag$.MODULE$.apply(Document.class));

        MongoRDD.toMongoCollection(mongoRdd, collectionFactory, WriteMode.BULK_ORDERED);

        assertEquals(2 * documents.size(), collectionFactory.getCollection().count());
    }
}

// essentially removes _id from each document - JavaRDD can use lambdas instead of serializable abstract functions
class GetSingleKeyValueDocument extends SerializableAbstractFunction1<Document, Document> {
    private String key;

    public GetSingleKeyValueDocument(final String key) {
        this.key = key;
    }

    @Override
    public Document apply(final Document document) {
        return new Document(key, document.get(key));
    }
}
