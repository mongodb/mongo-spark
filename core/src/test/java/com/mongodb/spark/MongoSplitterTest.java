/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

import com.mongodb.client.model.Filters;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.spark.SplitterHelper.splitsToBounds;
import static org.junit.Assert.assertEquals;

/*
 * These tests assume:
 *     There is a standalone mongod running on localhost:27017, with db 'spark_test'
 *     There is a sharded cluster running with a mongos on localhost:27018, with db 'spark_test'
 * Modify the URIs as necessary.
 */
public class MongoSplitterTest {
    private MongoSparkContext msc;

    private String standaloneUri = "mongodb://localhost:27017";
    private String shardedUri = "mongodb://localhost:27018";

    @Before
    public void setUp() {
        msc = new MongoSparkContext(new SparkConf().setAppName("MongoSplitterTest").setMaster("local"));
    }

    @After
    public void tearDown() {
        msc.stop();
        msc = null;
    }

    @Test
    public void shouldSplitStandalone() {
        MongoClientProvider clientProvider = new MongoSparkClientProvider(standaloneUri);
        MongoCollectionProvider<Document> collectionProvider =
                new MongoSparkCollectionProvider<>(Document.class, clientProvider, "spark_test", "test");

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionProvider, "a");

        assertEquals(clientProvider.getClient()
                                   .getDatabase("spark_test")
                                   .runCommand(new Document("splitVector", "spark_test.test")
                                                    .append("keyPattern", new Document("a", 1))
                                                    .append("maxChunkSize", 64))
                                   .get("splitKeys", List.class)
                                   .size() + 1,
                     rdd.partitions().size());
    }

    @Test
    public void shouldSplitSharded() {
        MongoClientProvider clientProvider = new MongoSparkClientProvider(shardedUri);
        MongoCollectionProvider<Document> collectionProvider =
                new MongoSparkCollectionProvider<>(Document.class, clientProvider, "spark_test", "test");

        JavaRDD<Document> rdd = msc.parallelize(Document.class, collectionProvider, "a");

        assertEquals(clientProvider.getClient()
                                   .getDatabase("config")
                                   .getCollection("chunks")
                                   .find()
                                   .filter(Filters.eq("ns", "spark_test.test"))
                                   .into(new ArrayList<>())
                                   .size(),
                     rdd.partitions().size());
    }

    @Test
    public void shouldGenerateBoundsFromSplits() {
        String key = "a";
        Document lower = new Document(key, 1);
        Document upper = new Document(key, 2);

        Document expectedBounds = new Document(key, new Document("$gte", lower.get(key))
                                                         .append("$lt", upper.get(key)));

        assertEquals(expectedBounds, splitsToBounds(lower, upper, key));
    }
}
