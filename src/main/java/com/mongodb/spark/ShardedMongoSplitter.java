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

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A splitter for a sharded mongo.
 */
public class ShardedMongoSplitter implements MongoSplitter {
    private MongoCollectionFactory factory;
    private Bson keyPattern;

    /**
     * Constructs a new instance.
     *
     * @param factory the collection factory
     * @param <T> the type of objects in the collection
     */
    public <T> ShardedMongoSplitter(final MongoCollectionFactory<T> factory, final Bson keyPattern) {
        this.factory = factory;
        this.keyPattern = notNull("keyPattern", keyPattern);
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

        // get chunks for this namespace
        List<Document> chunks = new ArrayList<>();
        // may throw exception
        this.factory.getClient().getDatabase("config").getCollection("chunks").find().into(chunks);
        String ns = this.factory.getCollection().getNamespace().getFullName();
        chunks.removeIf(doc -> !doc.get("ns").equals(ns));

        // there will always be at least 1 chunk in a sharded collection e.g. {min: {key : {$minKey : 1}}, max : {key : {$maxKey : 1}}}
        List<Bson> splitBounds = new ArrayList<>(chunks.size());
        chunks.forEach(doc -> splitBounds.add(MongoSplitter.keysToBounds((Document) doc.get("min"), (Document) doc.get("max"), keys)));

        return splitBounds;
    }
}
