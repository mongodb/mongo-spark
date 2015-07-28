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
import org.bson.conversions.Bson;

import java.util.Iterator;

/**
 * Writes an iterator, typically a spark partition, to the collection specified by the mongo collection factory.
 *
 * @param <T> the class of the objects in the partition
 */
class MongoSimpleWriter<T extends Bson> implements MongoWriter<T> {
    private MongoCollection<T> collection;

    /**
     * Constructs a class.
     *
     * @param factory the collection factory
     */
    public MongoSimpleWriter(final MongoCollectionFactory<T> factory) {
        this.collection = factory.getCollection();
    }

    @Override
    public void write(final Iterator<T> iterator) {
        while (iterator.hasNext()) {
            this.collection.insertOne(iterator.next());
        }
    }
}
