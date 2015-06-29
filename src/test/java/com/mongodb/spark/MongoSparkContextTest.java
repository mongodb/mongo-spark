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
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;

import org.bson.Document;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MongoSparkContextTest {
    private String master = "local";
    private String appName = "testApp";
    private String sparkHome = "path/to/spark";
    private String jarFile = "test.jar";

    private String host = "localhost:27017";
    private String database = "test";
    private String collection = "test";

    private SparkConf sparkConf = new SparkConf().setMaster(master).setAppName(appName);
    private SparkContext sc;

    private MongoClientURI clientUri = new MongoClientURI("mongodb://test:password@" + host + "/" + database);

    private MongoCredential credential =
            MongoCredential.createCredential(database, collection, new char[] {'p', 'a', 's', 's', 'w', 'o', 'r', 'd'});
    private List<MongoCredential> credentials = Collections.singletonList(credential);
    private List<String> hosts = Collections.singletonList(host);
    private MongoClientOptions options = new MongoClientOptions.Builder().build();

    private MongoClient mongoClient = new MongoClient(new ServerAddress(host), credentials, options);

    private List<Document> documents = Arrays.asList(new Document("a", 1), new Document("b", 2), new Document("c", 3));

    private MongoSparkContext msc;

    @Before
    public void setUp() {
        mongoClient.getDatabase(database).getCollection(collection).drop();
        mongoClient.getDatabase(database).getCollection(collection).insertMany(documents);
    }

    @After
    public void tearDown() {
        msc.stop();
        msc = null;
    }

    @Test
    public void shouldConstructMSCWithSparkConf() {
        msc = new MongoSparkContext(sparkConf);

        JavaRDD<Document> rdd = msc.parallelize(database, collection);

        Assert.assertEquals(documents.size(), rdd.collect().size());
        Assert.assertEquals(master, msc.sc().master());
        Assert.assertEquals(appName, msc.sc().appName());
    }

    @Test
    public void shouldConstructMSCWithSparkContext() {
        sc = new SparkContext(sparkConf);
        msc = new MongoSparkContext(sc);

        JavaRDD<Document> rdd = msc.parallelize(database, collection);

        Assert.assertEquals(documents.size(), rdd.collect().size());
        Assert.assertEquals(master, msc.sc().master());
        Assert.assertEquals(appName, msc.sc().appName());
    }

    @Test
    public void shouldConstructMSCWithSparkContextCredentials() {
        sc = new SparkContext(sparkConf);
        msc = new MongoSparkContext(sc, credentials);

        JavaRDD<Document> rdd = msc.parallelize(database, collection);

        Assert.assertEquals(documents.size(), rdd.collect().size());
        Assert.assertEquals(master, msc.sc().master());
        Assert.assertEquals(appName, msc.sc().appName());
    }

    @Test
    public void shouldConstructMSCWithSparkContextCredentialsHosts() {
        sc = new SparkContext(sparkConf);
        msc = new MongoSparkContext(sc, credentials, hosts);

        JavaRDD<Document> rdd = msc.parallelize(database, collection);

        Assert.assertEquals(documents.size(), rdd.collect().size());
        Assert.assertEquals(master, msc.sc().master());
        Assert.assertEquals(appName, msc.sc().appName());
    }

    @Test
    public void shouldConstructMSCWithSparkContextCredentialsHostsOptions() {
        sc = new SparkContext(sparkConf);
        msc = new MongoSparkContext(sc, credentials, hosts, options);

        JavaRDD<Document> rdd = msc.parallelize(database, collection);

        Assert.assertEquals(documents.size(), rdd.collect().size());
        Assert.assertEquals(master, msc.sc().master());
        Assert.assertEquals(appName, msc.sc().appName());
    }

    @Test
    public void shouldConstructMSCWithSparkConfCredentialsHostsOptions() {
        msc = new MongoSparkContext(sparkConf, credentials, hosts, options);

        JavaRDD<Document> rdd = msc.parallelize(database, collection);

        Assert.assertEquals(documents.size(), rdd.collect().size());
        Assert.assertEquals(master, msc.sc().master());
        Assert.assertEquals(appName, msc.sc().appName());
    }

    @Test
    public void shouldConstructMSCWithMasterAppNameCredentialsHostsOptions() {
        msc = new MongoSparkContext(master, appName, credentials, hosts, options);

        JavaRDD<Document> rdd = msc.parallelize(database, collection);

        Assert.assertEquals(documents.size(), rdd.collect().size());
        Assert.assertEquals(master, msc.sc().master());
        Assert.assertEquals(appName, msc.sc().appName());
    }

    @Test
    public void shouldConstructMSCWithMasterAppNameConfCredentialsHostsOptions() {
        msc = new MongoSparkContext(master, appName, sparkConf, credentials, hosts, options);

        JavaRDD<Document> rdd = msc.parallelize(database, collection);

        Assert.assertEquals(documents.size(), rdd.collect().size());
        Assert.assertEquals(master, msc.sc().master());
        Assert.assertEquals(appName, msc.sc().appName());
    }

    @Test
    public void shouldConstructMSCWithMasterAppNameSparkHomeJarFileCredentialsHostsOptions() {
        msc = new MongoSparkContext(master, appName, sparkHome, jarFile, credentials, hosts, options);

        JavaRDD<Document> rdd = msc.parallelize(database, collection);

        Assert.assertEquals(documents.size(), rdd.collect().size());
        Assert.assertEquals(master, msc.sc().master());
        Assert.assertEquals(appName, msc.sc().appName());
    }

    @Test
    public void shouldConstructMSCWithMasterAppNameSparkHomeJarsCredentialsHostsOptions() {
        msc = new MongoSparkContext(master, appName, sparkHome, new String[] {jarFile}, credentials, hosts, options);

        JavaRDD<Document> rdd = msc.parallelize(database, collection);

        Assert.assertEquals(documents.size(), rdd.collect().size());
        Assert.assertEquals(master, msc.sc().master());
        Assert.assertEquals(appName, msc.sc().appName());
    }

    @Test
    public void shouldConstructMSCWithSparkContextURI() {
        sc = new SparkContext(sparkConf);
        msc = new MongoSparkContext(sc, clientUri);

        JavaRDD<Document> rdd = msc.parallelize(database, collection);

        Assert.assertEquals(documents.size(), rdd.collect().size());
        Assert.assertEquals(master, msc.sc().master());
        Assert.assertEquals(appName, msc.sc().appName());
    }

    @Test
    public void shouldConstructMSCWithSparkConfURI() {
        msc = new MongoSparkContext(sparkConf, clientUri);

        JavaRDD<Document> rdd = msc.parallelize(database, collection);

        Assert.assertEquals(documents.size(), rdd.collect().size());
        Assert.assertEquals(master, msc.sc().master());
        Assert.assertEquals(appName, msc.sc().appName());
    }
}
