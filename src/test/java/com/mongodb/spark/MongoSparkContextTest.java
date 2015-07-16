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
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoClientURI;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class MongoSparkContextTest {
    private String username = "test";
    private String password = "password";
    private String host = "localhost:27017";
    private String database = "test";
    private String collection = "test";
    private String uri = "mongodb://" + username + ":" + password + "@" + host + "/" + database + "." + collection;

    private String master = "local";
    private String appName = "testApp";
    private String sparkHome = "path/to/spark";
    private String jarFile = "test.jar";

    private SparkConf sparkConf = new SparkConf().setMaster(master)
                                                 .setAppName(appName);
    private MongoSparkContext msc;
    private int partitions = 1;

    private MongoClientFactory clientFactory = new MongoSparkClientFactory(uri);
    private MongoCollectionFactory<Document> collectionFactory =
            new MongoSparkCollectionFactory<>(Document.class, clientFactory, database, collection);

    private String key = "a";
    private List<Document> documents = Arrays.asList(new Document(key, 0), new Document(key, 1), new Document(key, 2));
    private Bson query = new BsonDocument(key, new BsonInt32(0));
    private List<Bson> pipeline = singletonList(new BsonDocument("$project", new BsonDocument(key, new BsonInt32(1))));

    @Before
    public void setUp() {
        MongoClient client = new MongoClient(new MongoClientURI(uri));
        client.getDatabase(database).getCollection(collection).drop();
        client.getDatabase(database).getCollection(collection).insertMany(documents);
        client.close();
    }

    @After
    public void tearDown() {
        msc.stop();
        msc = null;
    }

    @Test
    public void shouldConstructMSCWithSparkConf() {
        msc = new MongoSparkContext(sparkConf);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionFactory);

        assertEquals(documents.size(), rdd.count());
    }

    @Test
    public void shouldConstructMSCWithSparkContext() {
        msc = new MongoSparkContext(new SparkContext(sparkConf));

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionFactory);

        assertEquals(documents.size(), rdd.count());
    }

    @Test
    public void shouldConstructMSCWithSparkConfSparkHome() {
        msc = new MongoSparkContext(sparkConf, sparkHome);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionFactory);

        assertEquals(documents.size(), rdd.count());
    }

    @Test
    public void shouldConstructMSCWithSparkConfSparkHomeJarFile() {
        msc = new MongoSparkContext(sparkConf, sparkHome, jarFile);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionFactory);

        assertEquals(documents.size(), rdd.count());
    }

    @Test
    public void shouldConstructMSCWithSparkConfSparkHomeJars() {
        msc = new MongoSparkContext(sparkConf, sparkHome, new String[] {jarFile});

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionFactory);

        assertEquals(documents.size(), rdd.count());
    }

    @Test
    public void shouldParallelizeWithPartitions() {
        msc = new MongoSparkContext(sparkConf);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionFactory, partitions);

        assertEquals(documents.size(), rdd.count());
        assertEquals(partitions, rdd.partitions().size());
    }

    @Test
    public void shouldParallelizeWithQuery() {
        msc = new MongoSparkContext(sparkConf);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionFactory, query);

        assertEquals(1, rdd.count());
        assertEquals(msc.sc().defaultParallelism(), rdd.partitions().size());
    }

    @Test
    public void shouldParallelizeWithPartitionsAndQuery() {
        msc = new MongoSparkContext(sparkConf);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionFactory, partitions, query);

        assertEquals(1, rdd.count());
        assertEquals(partitions, rdd.partitions().size());
    }

    @Test
    public void shouldParallelizeWithPipeline() {
        msc = new MongoSparkContext(sparkConf);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionFactory, pipeline);

        assertEquals(documents.size(), rdd.count());
        assertEquals(msc.sc().defaultParallelism(), rdd.partitions().size());
    }

    @Test
    public void shouldParallelizeWithPartitionsAndPipeline() {
        msc = new MongoSparkContext(sparkConf);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionFactory, partitions, pipeline);

        assertEquals(documents.size(), rdd.count());
        assertEquals(partitions, rdd.partitions().size());
    }

    @Test
    public void shouldParallelizeWithDefaultParallelism() {
        msc = new MongoSparkContext(sparkConf);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionFactory);

        assertEquals(documents.size(), rdd.count());
        assertEquals(msc.sc().defaultParallelism(), rdd.partitions().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailNonnegativePartitions() {
        msc = new MongoSparkContext(sparkConf);

        msc.parallelize(Document.class, collectionFactory, 0);
    }

    /*
     * Example of how one would use a client options builder initializer
     */
    @Test
    public void shouldSerializeSupplierAndInitializeBuilder() {
        msc = new MongoSparkContext(sparkConf);

        MongoCollectionFactory<Document> collectionFactory =
                new MongoSparkCollectionFactory<>(
                        Document.class,
                        new MongoSparkClientFactory(
                                uri,
                                new MongoSparkClientOptionsBuilderInitializer((SerializableSupplier<Builder>) () -> {
                                    Builder builder = new Builder();
                                    builder.maxConnectionLifeTime(100);
                                    return builder;
                                })
                        ),
                        database,
                        collection);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionFactory);

        assertEquals(documents.size(), rdd.count());
    }

    /*
     * Example of how one would write to a mongo collection
     */
    @Test
    public void shouldWriteToMongo() {
        msc = new MongoSparkContext(sparkConf);

        String keyCopy = key;
        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionFactory).map(doc -> new Document(keyCopy, doc.get(keyCopy)));

        MongoSparkContext.toMongoCollection(rdd, collectionFactory, MongoWriter.WriteMode.SIMPLE);

        assertEquals(2 * documents.size(), collectionFactory.getCollection().count());
    }
}
