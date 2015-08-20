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

import com.mongodb.MongoNotPrimaryException;
import com.mongodb.client.MongoCollection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.spark.SplitterHelper.splitsToBounds;

/**
 * A splitter for standalone mongo.
 */
class StandaloneMongoSplitter {
    private static final Log LOG = LogFactory.getLog(StandaloneMongoSplitter.class);

    private MongoCollectionProvider provider;
    private String key;
    private int maxChunkSize;

    /**
     * Constructs a new instance
     *
     * @param provider a mongo collection provider
     * @param key the minimal prefix key of the index to be used for splitting
     * @param maxChunkSize the max size (in MB) for each partition
     * @param <T> the type of documents in the collection
     */
    <T> StandaloneMongoSplitter(final MongoCollectionProvider<T> provider, final String key, final int maxChunkSize) {
        this.provider = notNull("provider", provider);
        this.key = notNull("key", key);
        this.maxChunkSize = maxChunkSize;
    }

    /**
     * Get the split bounds for a non-sharded collection.
     *
     * @return the split bounds as documents
     */
    @SuppressWarnings("unchecked")
    List<Document> getSplitBounds() {
        MongoCollection collection = this.provider.getCollection();

        String ns = collection.getNamespace().getFullName();

        LOG.debug("Getting split bounds for a non-sharded collection " + ns);

        Document keyPattern = new Document(this.key, 1);
        Document splitVectorCommand = new Document("splitVector", ns)
                                           .append("keyPattern", keyPattern)
                                           .append("maxChunkSize", this.maxChunkSize);
        Document result;
        try {
            result = this.provider.getDatabase()
                                  .runCommand(splitVectorCommand, Document.class);
        } catch (MongoNotPrimaryException e) {
            LOG.info("splitVector failed. " + e.getMessage() + "; continuing with no splitKeys!");

            result = new Document("ok", 1.0);
        }

        if (result.get("ok").equals(1.0)) {
            List<Document> splitKeys = (List<Document>) result.getOrDefault("splitKeys", Collections.emptyList());

            List<Document> splitBounds = new ArrayList<>(splitKeys.size() + 1);

            if (splitKeys.isEmpty()) {
                LOG.warn("WARNING: No splitKeys were calculated by the splitVector command. Proceeding with a single partition, "
                         + "which may be large. Try lowering 'maxChunkSize' if this is undesirable.");

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
