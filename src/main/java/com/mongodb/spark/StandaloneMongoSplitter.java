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

import com.mongodb.client.MongoCollection;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.spark.SplitterHelper.splitsToBounds;

/**
 * A splitter for standalone mongo.
 */
class StandaloneMongoSplitter {
    private MongoCollectionFactory factory;
    private String key;
    private int maxChunkSize;

    /**
     * Constructs a new instance
     *
     * @param factory a mongo collection factory
     * @param key the minimal prefix key of the index to be used for splitting
     * @param maxChunkSize the max size (in MB) for each partition
     * @param <T> the type of documents in the collection
     */
    <T> StandaloneMongoSplitter(final MongoCollectionFactory<T> factory, final String key, final int maxChunkSize) {
        this.factory = notNull("factory", factory);
        this.key = notNull("key", key);
        this.maxChunkSize = maxChunkSize;
    }

    /**
     * Get the split keys for a non-sharded collection.
     *
     * @return the split bounds as documents
     */
    List<Document> getSplitBounds() {
        MongoCollection collection = this.factory.getCollection();

        String ns = collection.getNamespace().getFullName();
        Document keyPattern = new Document(this.key, 1);
        Document splitVectorCommand = new Document("splitVector", ns)
                                           .append("keyPattern", keyPattern)
                                           .append("maxChunkSize", this.maxChunkSize);
        Document result = this.factory.getDatabase().runCommand(splitVectorCommand, Document.class);

        if (result.get("ok").equals(1.0)) {
            List<Document> splitKeys = (List<Document>) result.get("splitKeys");

            List<Document> splitBounds = new ArrayList<>(splitKeys.size() + 1);

            if (splitKeys.size() == 0) {
                // no splits were calculated - this means that the RDD partition may be massive
                splitBounds.add(
                        splitsToBounds(new Document(this.key, new BsonMinKey()), new Document(this.key, new BsonMaxKey()), this.key));
            } else {
                splitBounds.add(splitsToBounds(new Document(this.key, new BsonMinKey()), splitKeys.get(0), this.key));

                for (int i = 0; i < splitKeys.size() - 1; i++) {
                    splitBounds.add(splitsToBounds(splitKeys.get(i), splitKeys.get(i + 1), this.key));
                }

                splitBounds.add(splitsToBounds(splitKeys.get(splitKeys.size() - 1), new Document(this.key, new BsonMaxKey()), this.key));
            }

            return splitBounds;
        } else {
            throw new SplitException("Could not calculate standalone splits. Server errmsg: " + result.get("errmsg"));
        }
    }
}
