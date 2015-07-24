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

import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.Set;

/**
 * A interface to split mongo collections for Spark partitioning.
 */
public interface MongoSplitter {
    /**
     * Get the split keys for a collection.
     *
     * @return the split bounds as documents
     */
    List<Bson> getSplitBounds();

    /**
     * Helper function to translate splitVector key:value pairs into partition boundaries.
     *
     * @param lower the lower boundary document
     * @param upper the upper boundary document
     * @param keys the keys used by splitVector; since splitVector requires an index, these keys must be
     *             present in both lower and upper
     * @return the document containing the partition bounds
     */
    static Bson keysToBounds(final Document lower, final Document upper, final Set<String> keys) {
        Document bounds = new Document();

        for (String key : keys) {
            bounds.append(key, new Document("$gte", lower == null ? new BsonMinKey() : lower.get(key))
                                    .append("$lt", upper == null ? new BsonMaxKey() : upper.get(key)));
        }

        return bounds;
    }
}
