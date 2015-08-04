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
public class MongoProviderTest {
    @Test
    public void testProviders() {
        MongoClientProvider clientProvider = new MongoSparkClientProvider("mongodb://localhost:30000/");
        MongoCollectionProvider<Document> collectionProvider =
                new MongoSparkCollectionProvider<>(Document.class, clientProvider, "spark_test", "test");

        collectionProvider.getCollection().drop();
        collectionProvider.getCollection().insertOne(new Document("test", "test"));

        assertEquals(1, collectionProvider.getCollection().count());

        try {
            clientProvider.close();
        } catch (IOException e) {
            fail();
        }
    }
}
