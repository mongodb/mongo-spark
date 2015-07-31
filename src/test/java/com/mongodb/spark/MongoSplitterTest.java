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
import org.apache.spark.broadcast.Broadcast;
import org.bson.Document;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/*
 * These tests assume:
 *     There is a standalone mongod running on localhost:30000, with namespace test.foo
 *     There is a sharded cluster running with a mongos on localhost:27017, with namespace test.foo
 * Modify the URIs as necessary.
 */
public class MongoSplitterTest {
    @Test
    public void shouldSplitStandalone() {
        MongoSparkContext msc = new MongoSparkContext(new SparkConf().setAppName("MongoSplitterTest").setMaster("local"));
        MongoClientFactory clientFactory = new MongoSparkClientFactory("mongodb://localhost:30000");
        Broadcast<MongoCollectionFactory<Document>> broadcastFactory =
                msc.broadcast(new MongoSparkCollectionFactory<>(Document.class, clientFactory, "test", "part"));

        JavaRDD<Document> rdd = msc.parallelize(Document.class, broadcastFactory, "a");

        assertEquals(((List) (clientFactory.getClient()
                                           .getDatabase("test")
                                           .runCommand(new Document("splitVector", "test.part")
                                                            .append("keyPattern", new Document("a", 1))
                                                            .append("maxChunkSize", 64))
                                           .get("splitKeys"))).size() + 1,
                     rdd.partitions().size());

        msc.stop();
    }

    @Test
    public void shouldSplitSharded() {
        MongoSparkContext msc = new MongoSparkContext(new SparkConf().setAppName("MongoSplitterTest").setMaster("local"));
        MongoClientFactory clientFactory = new MongoSparkClientFactory("mongodb://localhost:27017");
        Broadcast<MongoCollectionFactory<Document>> broadcastFactory =
                msc.broadcast(new MongoSparkCollectionFactory<>(Document.class, clientFactory, "test", "foo"));

        JavaRDD<Document> rdd = msc.parallelize(Document.class, broadcastFactory, "a");

        assertEquals(clientFactory.getClient()
                                  .getDatabase("config")
                                  .getCollection("chunks")
                                  .find()
                                  .filter(Filters.eq("ns", "test.foo"))
                                  .into(new ArrayList<>())
                                  .size(),
                     rdd.partitions().size());

        msc.stop();
    }
}
