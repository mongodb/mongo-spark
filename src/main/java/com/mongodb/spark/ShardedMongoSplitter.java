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
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.spark.SplitterHelper.splitsToBounds;

/**
 * A splitter for a sharded mongo.
 */
class ShardedMongoSplitter {
    private MongoCollectionFactory factory;
    private String key;

    /**
     * Constructs a new instance.
     *
     * @param factory the collection factory
     * @param key the minimal prefix key of the index to be used for splitting
     * @param <T> the type of objects in the collection
     */
    <T> ShardedMongoSplitter(final MongoCollectionFactory<T> factory, final String key) {
        this.factory = notNull("factory", factory);
        this.key = notNull("key", key);
    }

    /**
     * Get the split bounds for a sharded collection.
     *
     * @return the split bounds as documents
     */
    List<Document> getSplitBounds() {
        MongoCollection collection = this.factory.getCollection();

        // get chunks for this namespace
        // may throw exception
        List<Document> chunks = this.factory.getClient()
                                            .getDatabase("config")
                                            .getCollection("chunks")
                                            .find(Filters.eq("ns", collection.getNamespace().getFullName()))
                                            .projection(Projections.include("min", "max"))
                                            .into(new ArrayList<>());

        // there will always be at least 1 chunk in a sharded collection
        // e.g. {min: {key : {$minKey : 1}}, max : {key : {$maxKey : 1}}}
        List<Document> splitBounds = new ArrayList<>(chunks.size());
        chunks.forEach(doc -> splitBounds.add(splitsToBounds(doc.get("min", Document.class), doc.get("max", Document.class), this.key)));

        return splitBounds;
    }
}
