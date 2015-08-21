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

/**
 * Utility class for mongo splitters.
 */
final class SplitterHelper {
    /**
     * Helper function to translate splitVector key:value pairs into partition boundaries.
     *
     * @param lower the lower boundary document
     * @param upper the upper boundary document
     * @param key the minimal prefix key for the index used by splitVector
     * @return the document containing the partition bounds
     */
    static Document splitsToBounds(final Document lower, final Document upper, final String key) {
        return new Document(key, new Document("$gte", lower.get(key))
                                      .append("$lt", upper.get(key)));
    }

    private SplitterHelper() {
    }
}
