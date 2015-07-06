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

import java.io.Serializable;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

class DocComp implements Serializable, Comparator {
    @Override
    public int compare(final Object d1, final Object d2) {
        return Integer.compare((Integer) (((Document) d2).get("a")), (Integer) (((Document) d1).get("a")));
    }
}

public class MongoJavaRDDTest {
    private MongoSparkContext msc;

    private String host = "localhost:27017";
    private String database = "test";
    private String collection = "rdd";

    private MongoClientURI uri =
            new MongoClientURI("mongodb://test:password@" + host + "/" + database + "." + collection);
    private MongoClient client = new MongoClient(uri);

    private String key = "a";
    private List<Document> documents = Arrays.asList(new Document(key, 0), new Document(key, 2), new Document(key, 1));

    private BsonDocument query = new BsonDocument("b", new BsonInt32(0));

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
        SparkContext sc = new SparkContext("local", "app");
        msc = new MongoSparkContext(sc, uri);

        MongoJavaRDD mongoRdd = msc.parallelize(1);

        Assert.assertEquals(documents.size(), mongoRdd.collect().size());
        Assert.assertEquals(mongoRdd.take(1).get(0), mongoRdd.first());
    }

    @Test
    public void shouldMakeMappedJavaRDD() {
        SparkContext sc = new SparkContext("local", "app");
        msc = new MongoSparkContext(sc, uri);

        MongoJavaRDD mongoRdd = msc.parallelize(1);

        String queryKey = key;
        JavaPairRDD<String, Integer> rdd = mongoRdd.mapToPair(doc -> new Tuple2<>(queryKey, (Integer) ((Document) doc).get(queryKey) * 2));

        JavaRDD<?> jrdd = rdd.map(tuple -> new Document((String) ((Tuple2) tuple)._1(), ((Tuple2) tuple)._2()));
        Assert.assertEquals(Arrays.asList(new Document(key, 0), new Document(key, 4), new Document(key, 2)), jrdd.collect());
    }

    @Test
    public void shouldMakeMongoJavaRDD() {
        SparkContext sc = new SparkContext("local", "app");
        msc = new MongoSparkContext(sc, uri);

        MongoJavaRDD rdd = msc.parallelize(1);

        String queryKey = key;
        JavaRDD jrdd = rdd.map(doc -> new Tuple2<>(queryKey, new Document(queryKey, (Integer) ((Document) doc).get(queryKey) * 2)));

        JavaPairRDD<String, Document> jprdd = JavaPairRDD.fromJavaRDD(jrdd);

        Assert.assertEquals(new Tuple2<>(key, new Document(key, 6)),
                               jprdd.reduceByKey((a, b) -> new Document(queryKey, (Integer) a.get(queryKey) + (Integer) b.get(queryKey)))
                                   .collect().get(0));
    }

    @Test
    public void shouldTakeOrdered() {
        SparkContext sc = new SparkContext("local", "app");
        msc = new MongoSparkContext(sc, uri);

        MongoJavaRDD rdd = msc.parallelize(1);

        List<Document> results = rdd.takeOrdered(3, new DocComp());
        results.forEach(doc -> doc.remove("_id"));

        Assert.assertEquals(Arrays.asList(new Document(key, 2), new Document(key, 1), new Document(key, 0)), results);
    }

    @Test
    public void shouldQuery() {
        client.getDatabase(uri.getDatabase()).getCollection(uri.getCollection()).insertOne(new Document(key, 3).append("b", 0));

        SparkContext sc = new SparkContext("local", "app");
        msc = new MongoSparkContext(sc, uri);

        MongoJavaRDD rdd = msc.parallelize(query);

        Assert.assertEquals(1, rdd.count());
    }
}
