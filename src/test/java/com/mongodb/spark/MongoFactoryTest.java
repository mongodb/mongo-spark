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

import org.bson.Document;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/*
 * These tests assume a single mongod running on localhost:30000
 */
public class MongoFactoryTest {
    @Test
    public void testFactories() {
        MongoClientFactory clientFactory = new MongoSparkClientFactory("mongodb://localhost:30000/");
        MongoCollectionFactory<Document> collectionFactory =
                new MongoSparkCollectionFactory<>(Document.class, clientFactory, "test", "test");

        collectionFactory.getCollection().drop();
        collectionFactory.getCollection().insertOne(new Document("test", "test"));

        assertEquals(1, collectionFactory.getCollection().count());

        try {
            clientFactory.close();
        } catch (IOException e) {
            fail();
        }
    }
}
