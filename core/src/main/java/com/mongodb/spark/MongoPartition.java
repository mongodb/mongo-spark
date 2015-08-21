/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.mongodb.spark;

import org.apache.spark.Partition;
import org.bson.Document;

/**
 * An identifier for a partition in a MongoRDD.
 */
class MongoPartition implements Partition {
    private int index;
    private Document bounds;

    /**
     * Constructs a new instance.
     *
     * @param index index of the partition
     * @param bounds the bounds of the partition with respect to the mongo collection
     */
    public MongoPartition(final int index, final Document bounds) {
        this.index = index;
        this.bounds = bounds;
    }

    /**
     * Gets the partition's index within its parent RDD
     *
     * @return the index of the rdd
     */
    public int index() {
        return this.index;
    }

    /**
     * Gets the bounds of the partition.
     *
     * @return the bounds of the partition
     */
    public Document getBounds() {
        return this.bounds;
    }
}
