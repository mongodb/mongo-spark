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

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A splitter for standalone mongo.
 */
public class StandaloneMongoSplitter implements MongoSplitter {
    private MongoCollectionFactory factory;
    private Bson keyPattern;
    private int maxChunkSize;

    /**
     * Constructs a new instance
     *
     * @param factory a mongo collection factory
     * @param keyPattern the keys of the index to be used for splitting
     * @param maxChunkSize the max size (in MB) for each partition
     * @param <T> the type of documents in the collection
     */
    public <T> StandaloneMongoSplitter(final MongoCollectionFactory<T> factory, final Bson keyPattern, final int maxChunkSize) {
        this.factory = factory;
        this.keyPattern = keyPattern == null ? new Document("_id", 1) : keyPattern;
        this.maxChunkSize = maxChunkSize;
    }

    @Override
    public List<Bson> getSplitBounds() {
        Set<String> keys;
        if (this.keyPattern instanceof Document) {
            keys = ((Document) this.keyPattern).keySet();
        }
        else if (this.keyPattern instanceof BsonDocument) {
            keys = ((BsonDocument) this.keyPattern).keySet();
        }
        else {
            throw new SplitException("keyPattern of class " + this.keyPattern.getClass() + " is not supported.");
        }
        if (keys.size() > 1) {
            throw new SplitException("keyPattern must specify a single index key. keyPattern provided: " + this.keyPattern);
        }

        String namespace = this.factory.getCollection().getNamespace().getFullName();
        Document splitVectorCommand = new Document("splitVector", namespace)
                                           .append("keyPattern", this.keyPattern)
                                           .append("maxChunkSize", this.maxChunkSize);
        Document result = this.factory.getDatabase().runCommand(splitVectorCommand, Document.class);

        if (result.get("ok").equals(1.0)) {
            List<Document> splitKeys = (List<Document>) result.get("splitKeys");

            List<Bson> splitBounds = new ArrayList<>(splitKeys.size() + 1);

            if (splitKeys.size() == 0) {
                // no splits were calculated - this means that the RDD partition may be massive
                splitBounds.add(MongoSplitter.keysToBounds(null, null, keys));
            }
            else {
                splitBounds.add(MongoSplitter.keysToBounds(null, splitKeys.get(0), keys));

                for (int i = 0; i < splitKeys.size() - 1; i++) {
                    splitBounds.add(MongoSplitter.keysToBounds(splitKeys.get(i), splitKeys.get(i + 1), keys));
                }

                splitBounds.add(MongoSplitter.keysToBounds(splitKeys.get(splitKeys.size() - 1), null, keys));
            }

            return splitBounds;
        } else {
            throw new SplitException("Could not calculate standalone splits. Server errmsg: " + result.get("errmsg"));
        }
    }
}
