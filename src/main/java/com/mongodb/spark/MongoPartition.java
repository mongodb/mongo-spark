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
import org.bson.BsonValue;

/**
 * An identifier for a partition in a MongoRDD.
 */
class MongoPartition implements Partition {
    private int       index;
    private BsonValue lower;
    private BsonValue upper;

    /**
     * Constructs a new instance.
     *
     * @param indexValue index of the partition
     * @param lowerValue minKey of the partition
     * @param upperValue maxKey of the partition
     */
    public MongoPartition(final int indexValue, final BsonValue lowerValue, final BsonValue upperValue) {
        index = indexValue;
        lower = lowerValue;
        upper = upperValue;
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
     * Gets the minKey of the partition.
     *
     * @return the minKey of the partition
     */
    public BsonValue getLower() {
        return this.lower;
    }

    /**
     * Gets the maxKey of the partition.
     *
     * @return the maxKey of the partition
     */
    public BsonValue getUpper() {
        return this.upper;
    }
}
