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

import static com.mongodb.spark.SplitterHelper.splitsToBounds;

/**
 * A splitter for standalone mongo. Calculates a list of splits on a single
 * collection by running the MongoDB internal command {@code splitVector} and
 * generating split boundaries from the index boundaries returned by the
 * command. Each chunk represented by the boundaries contains an approximate
 * amount of data specified by the maxChunkSize used, and corresponds to one
 * partition in an RDD.
 *
 * This splitter is the default implementation used for any collection which
 * is not sharded.
 *
 * WARNING: If the collection provider targets a secondary member of a replica
 * set, the {@code splitVector} command will fail. The program will proceed
 * with a single partition. If this is not desirable, target the primary and
 * set read preference accordingly.
 */
final class StandaloneMongoSplitter {
    private static final Log LOG = LogFactory.getLog(StandaloneMongoSplitter.class);

    /**
     * Gets the split bounds for a non-sharded collection.
     *
     * @param provider a mongo collection provider
     * @param key the minimal prefix key of the index to be used for splitting
     * @param maxChunkSize the max size (in MB) for each partition
     * @param <T> the type of documents in the collection
     * @return the split bounds as documents
     */
    @SuppressWarnings("unchecked")
    static <T> List<Document> getSplitBounds(final MongoCollectionProvider<T> provider, final String key, final int maxChunkSize) {
        MongoCollection collection = provider.getCollection();

        String ns = collection.getNamespace().getFullName();

        LOG.debug("Getting split bounds for a non-sharded collection " + ns);

        Document keyPattern = new Document(key, 1);
        Document splitVectorCommand = new Document("splitVector", ns)
                                           .append("keyPattern", keyPattern)
                                           .append("maxChunkSize", maxChunkSize);

        Document result;
        try {
            result = provider.getDatabase()
                             .runCommand(splitVectorCommand, Document.class);
        } catch (MongoNotPrimaryException e) {
            // this fails because no chunk manager on secondaries
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
                        splitsToBounds(new Document(key, new BsonMinKey()), new Document(key, new BsonMaxKey()), key));
            } else {
                splitBounds.add(splitsToBounds(new Document(key, new BsonMinKey()), splitKeys.get(0), key));

                for (int i = 0; i < splitKeys.size() - 1; i++) {
                    splitBounds.add(splitsToBounds(splitKeys.get(i), splitKeys.get(i + 1), key));
                }

                splitBounds.add(splitsToBounds(splitKeys.get(splitKeys.size() - 1), new Document(key, new BsonMaxKey()), key));
            }

            return splitBounds;
        } else {
            throw new SplitException("Could not calculate standalone splits. Server errmsg: " + result.get("errmsg"));
        }
    }

    private StandaloneMongoSplitter() {
    }
}
