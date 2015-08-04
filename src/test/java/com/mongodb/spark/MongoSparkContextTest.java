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

import static com.mongodb.MongoClientOptions.Builder;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

/*
 * These tests assume a single mongod running on localhost:30000
 * with authentication available for a user with name 'test' and password 'password'
 * for the database 'test'
 */
public class MongoSparkContextTest {
    private String database = "spark_test";
    private String collection = "test";
    private String uri = "mongodb://spark_test:password@localhost:30000/spark_test";

    private String master = "local";
    private String appName = "testApp";
    private String sparkHome = "path/to/spark";
    private String jarFile = "test.jar";

    private SparkConf sparkConf = new SparkConf().setMaster(master)
                                                 .setAppName(appName);
    private MongoSparkContext msc;
    private int partitions = 1;

    private MongoClientProvider clientProvider = new MongoSparkClientProvider(uri);
    private MongoCollectionProvider<Document> collectionProvider =
            new MongoSparkCollectionProvider<>(Document.class, clientProvider, database, collection);

    private String key = "a";
    private List<Document> documents = Arrays.asList(new Document(key, 0), new Document(key, 1), new Document(key, 2));
    private List<Bson> pipeline = singletonList(new BsonDocument("$match", new BsonDocument(key, new BsonInt32(0))));

    @Before
    public void setUp() {
        MongoClient client = new MongoClient(new MongoClientURI(uri));
        client.getDatabase(database).getCollection(collection).drop();
        client.getDatabase(database).getCollection(collection).insertMany(documents);
        client.getDatabase(database).getCollection(collection).createIndex(new Document(key, 1));
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

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionProvider, key);

        assertEquals(documents.size(), rdd.count());
        assertEquals(documents, rdd.collect());
    }

    @Test
    public void shouldConstructMSCWithSparkContext() {
        msc = new MongoSparkContext(new SparkContext(sparkConf));

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionProvider, key);

        assertEquals(documents.size(), rdd.count());
        assertEquals(documents, rdd.collect());
    }

    @Test
    public void shouldConstructMSCWithSparkConfSparkHome() {
        msc = new MongoSparkContext(sparkConf, sparkHome);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionProvider, key);

        assertEquals(documents.size(), rdd.count());
        assertEquals(documents, rdd.collect());
    }

    @Test
    public void shouldConstructMSCWithSparkConfSparkHomeJarFile() {
        msc = new MongoSparkContext(sparkConf, sparkHome, jarFile);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionProvider, key);

        assertEquals(documents.size(), rdd.count());
        assertEquals(documents, rdd.collect());
    }

    @Test
    public void shouldConstructMSCWithSparkConfSparkHomeJars() {
        msc = new MongoSparkContext(sparkConf, sparkHome, new String[] {jarFile});

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionProvider, key);

        assertEquals(documents.size(), rdd.count());
        assertEquals(documents, rdd.collect());
    }

    @Test
    public void shouldParallelizeWithPartitions() {
        msc = new MongoSparkContext(sparkConf);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionProvider, key, partitions);

        assertEquals(documents.size(), rdd.count());
        assertEquals(partitions, rdd.partitions().size());
        assertEquals(documents, rdd.collect());
    }

    @Test
    public void shouldParallelizeWithPipeline() {
        msc = new MongoSparkContext(sparkConf);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionProvider, key, pipeline);

        assertEquals(1, rdd.count());
        assertEquals(msc.sc().defaultParallelism(), rdd.partitions().size());
        assertEquals(singletonList(documents.get(0)), rdd.collect());
    }

    @Test
    public void shouldParallelizeWithPartitionsAndPipeline() {
        msc = new MongoSparkContext(sparkConf);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionProvider, key, partitions, pipeline);

        assertEquals(1, rdd.count());
        assertEquals(partitions, rdd.partitions().size());
        assertEquals(singletonList(documents.get(0)), rdd.collect());
    }

    @Test
    public void shouldParallelizeWithDefaultParallelism() {
        msc = new MongoSparkContext(sparkConf);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionProvider, key);

        assertEquals(documents.size(), rdd.count());
        assertEquals(msc.sc().defaultParallelism(), rdd.partitions().size());
        assertEquals(documents, rdd.collect());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailNonnegativePartitions() {
        msc = new MongoSparkContext(sparkConf);

        msc.parallelize(Document.class, collectionProvider, key, 0);
    }

    /*
     * Example of how one would use a client options builder initializer
     */
    @Test
    public void shouldSerializeSupplierAndInitializeBuilder() {
        msc = new MongoSparkContext(sparkConf);

        MongoCollectionProvider<Document> collectionProvider =
                        new MongoSparkCollectionProvider<>(
                        Document.class,
                        new MongoSparkClientProvider(uri,
                                new MongoSparkClientOptionsBuilderInitializer(() -> new Builder().maxConnectionLifeTime(100))
                        ),
                        database,
                        collection);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionProvider, key);

        assertEquals(documents.size(), rdd.count());
        assertEquals(documents, rdd.collect());
    }
}
