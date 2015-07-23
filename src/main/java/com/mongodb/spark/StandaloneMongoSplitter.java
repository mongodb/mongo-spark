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

import com.mongodb.MongoException;
import org.bson.BsonDocument;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
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

    /**
     * Constructs a new instance
     *
     * @param factory a mongo collection factory
     * @param keyPattern the keys of the index to be used for splitting
     * @param <T> the type of documents in the collection
     */
    public <T> StandaloneMongoSplitter(final MongoCollectionFactory<T> factory, final Bson keyPattern) {
        this.factory = factory;
        this.keyPattern = keyPattern == null ? new Document("_id", 1) : keyPattern;
    }

    // TODO: more robust - what if not ok?
    @Override
    public List<Bson> getSplitBounds(final int maxChunkSize) {
        String namespace = this.factory.getCollection().getNamespace().getFullName();
        Document splitVectorCommand = new Document("splitVector", namespace)
                                           .append("keyPattern", this.keyPattern)
                                           .append("maxChunkSize", maxChunkSize)
                                           .append("force", false);
        Document result;
        try {
            result = this.factory.getDatabase().runCommand(splitVectorCommand, Document.class);
        } catch (final MongoException e) {
            // TODO: handle
            return null;
        }

        Set<String> keys;
        if (this.keyPattern instanceof Document) {
            keys = ((Document) this.keyPattern).keySet();
        }
        else if (this.keyPattern instanceof BsonDocument) {
            keys = ((BsonDocument) this.keyPattern).keySet();
        }
        else {
            // TODO: unsupported
            return null;
        }

        List<Document> splitKeys;
        List<Bson> splitBounds;

        if (result.get("ok").equals(1.0)) {
            splitKeys = (List<Document>) result.get("splitKeys");

            // account for max/minKey
            splitBounds = new ArrayList<>(splitKeys.size() + 1);

            if (splitKeys.size() == 0) {
                splitBounds.add(this.keysToBounds(null, null, keys));
            }
            else {
                // create first $minKey -> first split key
                splitBounds.add(this.keysToBounds(null, splitKeys.get(0), keys));

                // create interior bounds
                for (int i = 0; i < splitKeys.size() - 1; i++) {
                    splitBounds.add(this.keysToBounds(splitKeys.get(i), splitKeys.get(i + 1), keys));
                }

                // create last bounds - last split key -> $maxKey
                splitBounds.add(this.keysToBounds(splitKeys.get(splitKeys.size() - 1), null, keys));
            }
        } else {
            // TODO: handle
            splitBounds = null;
        }

        return splitBounds;
    }

    /**
     * Helper function to translate splitVector key:value pairs into partition boundaries.
     *
     * @param lower the lower boundary document
     * @param upper the upper boundary document
     * @param keys the keys used by splitVector; since splitVector requires an index, these keys must be
     *             present in both lower and upper
     * @return the document containing the partition bounds
     */
    private Document keysToBounds(final Document lower, final Document upper, final Set<String> keys) {
        Document bounds = new Document();

        for (String key : keys) {
            bounds.append(key, new Document("$gte", lower == null ? new BsonMinKey() : lower.get(key))
                                    .append("$lt", upper == null ? new BsonMaxKey() : upper.get(key)));
        }

        return bounds;
    }
}
