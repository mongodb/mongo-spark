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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;

public class MongoSparkContextTest {
    private String username = "test";
    private String password = "password";
    private String host = "localhost:27017";
    private String database = "test";
    private String collection = "test";
    private MongoClientURI uri =
        new MongoClientURI("mongodb://" + username + ":" + password + "@" + host + "/" + database + "." + collection);

    private String master = "local";
    private String appName = "testApp";
    private String sparkHome = "path/to/spark";
    private String jarFile = "test.jar";

    private SparkConf sparkConf = new SparkConf().setMaster(master)
                                                 .setAppName(appName)
                                                 .set("spark.mongo.auth.userName", username)
                                                 .set("spark.mongo.auth.password", password)
                                                 .set("spark.mongo.auth.source", database)
                                                 .set("spark.mongo.hosts", host);
    private MongoSparkContext msc;
    private int partitions = 1;

    private String key = "a";
    private List<Document> documents = Arrays.asList(new Document(key, 0), new Document(key, 1), new Document(key, 2));
    private Bson query = new BsonDocument(key, new BsonInt32(0));
    private List<Bson> pipeline = singletonList(new BsonDocument("$project", new BsonDocument(key, new BsonInt32(1))));

    @Before
    public void setUp() {
        MongoClient client = new MongoClient(uri);
        client.getDatabase(uri.getDatabase()).getCollection(uri.getCollection()).drop();
        client.getDatabase(uri.getDatabase()).getCollection(uri.getCollection()).insertMany(documents);
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

        JavaRDD<Document> rdd = msc.parallelize(Document.class, database, collection);

        Assert.assertEquals(documents.size(), rdd.count());
    }

    @Test
    public void shouldConstructMSCWithSparkContext() {
        msc = new MongoSparkContext(new SparkContext(sparkConf));

        JavaRDD<Document> rdd = msc.parallelize(Document.class, database, collection);

        Assert.assertEquals(documents.size(), rdd.count());
    }

    @Test
    public void shouldConstructMSCWithSparkConfSparkHome() {
        msc = new MongoSparkContext(sparkConf, sparkHome);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, database, collection);

        Assert.assertEquals(documents.size(), rdd.count());
    }

    @Test
    public void shouldConstructMSCWithSparkConfSparkHomeJarFile() {
        msc = new MongoSparkContext(sparkConf, sparkHome, jarFile);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, database, collection);

        Assert.assertEquals(documents.size(), rdd.count());
    }

    @Test
    public void shouldConstructMSCWithSparkConfSparkHomeJars() {
        msc = new MongoSparkContext(sparkConf, sparkHome, new String[] {jarFile});

        JavaRDD<Document> rdd = msc.parallelize(Document.class, database, collection);

        Assert.assertEquals(documents.size(), rdd.count());
    }

    @Test
    public void shouldParallelizeWithPartitions() {
        msc = new MongoSparkContext(sparkConf);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, database, collection, partitions);

        Assert.assertEquals(documents.size(), rdd.count());
        Assert.assertEquals(partitions, rdd.partitions().size());
    }

    @Test
    public void shouldParallelizeWithQuery() {
        msc = new MongoSparkContext(sparkConf);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, database, collection, query);

        Assert.assertEquals(1, rdd.count());
        Assert.assertEquals(msc.sc().defaultParallelism(), rdd.partitions().size());
    }

    @Test
    public void shouldParallelizeWithPartitionsAndQuery() {
        msc = new MongoSparkContext(sparkConf);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, database, collection, partitions, query);

        Assert.assertEquals(1, rdd.count());
        Assert.assertEquals(partitions, rdd.partitions().size());
    }

    @Test
    public void shouldParallelizeWithPipeline() {
        msc = new MongoSparkContext(sparkConf);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, database, collection, pipeline);

        Assert.assertEquals(documents.size(), rdd.count());
        Assert.assertEquals(msc.sc().defaultParallelism(), rdd.partitions().size());
    }

    @Test
    public void shouldParallelizeWithPartitionsAndPipeline() {
        msc = new MongoSparkContext(sparkConf);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, database, collection, partitions, pipeline);

        Assert.assertEquals(documents.size(), rdd.count());
        Assert.assertEquals(partitions, rdd.partitions().size());
    }

    @Test
    public void shouldParallelizeWithDefaultParallelism() {
        msc = new MongoSparkContext(sparkConf);

        JavaRDD<Document> rdd = msc.parallelize(Document.class, database, collection);

        Assert.assertEquals(documents.size(), rdd.count());
        Assert.assertEquals(msc.sc().defaultParallelism(), rdd.partitions().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailNonnegativePartitions() {
        msc = new MongoSparkContext(sparkConf);

        msc.parallelize(Document.class, database, collection, 0);
    }

    @Test
    public void testFactoriesAndCleanup() {
        msc = new MongoSparkContext(new SparkContext(sparkConf));

        JavaRDD<Document> rdd = msc.parallelize(Document.class, database, collection);

        Assert.assertEquals(documents.size(), rdd.count());
    }
}
