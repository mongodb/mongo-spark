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
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Bulk writes the elements of an iterator, typically a spark partition, to the
 * collection specified by the mongo collection factory.
 *
 * @param <T> the class of the objects in the partition
 */
public class MongoBulkWriter<T> implements MongoWriter<T> {
    private MongoCollection<T> collection;
    private BulkWriteOptions options;

    /**
     * Constructs a new instance.
     *
     * @param factory the collection factory
     */
    public MongoBulkWriter(final MongoCollectionFactory<T> factory) {
        notNull("factory", factory);
        this.collection = factory.getCollection();
    }

    /**
     * Constructs a new instance.
     *
     * @param factory the collection factory
     * @param ordered true if the writes should be ordered
     */
    public MongoBulkWriter(final MongoCollectionFactory<T> factory, final boolean ordered) {
        this(factory);
        this.options = new BulkWriteOptions().ordered(ordered);
    }

    @Override
    public void write(final Iterator<T> iterator) {
        List<InsertOneModel<T>> elements = new ArrayList<>();
        while (iterator.hasNext()) {
            elements.add(new InsertOneModel<>(iterator.next()));
        }

        if (this.options != null) {
            this.collection.bulkWrite(elements, this.options);
        }
        else {
            this.collection.bulkWrite(elements);
        }
    }
}
