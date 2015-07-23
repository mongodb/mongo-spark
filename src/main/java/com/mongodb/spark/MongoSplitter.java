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

import org.bson.conversions.Bson;

import java.util.List;

/**
 * A interface to split mongo collections for Spark partitioning.
 */
public interface MongoSplitter {
    /**
     * Get the split keys for a collection.
     *
     * @param maxChunkSize the max chunk size of each split
     * @return the split bounds as documents
     */
    List<Bson> getSplitBounds(final int maxChunkSize);
}
