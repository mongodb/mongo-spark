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
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Bulk writes the elements of an iterator, typically a spark partition, to the
 * collection specified by the mongo collection factory.
 *
 * @param <T> the class of the objects in the partition
 */
class MongoBulkWriter<T extends Bson> implements MongoWriter<T> {
    private MongoCollection<T> collection;
    private WriteMode mode;

    /**
     * Constructs a new instance.
     *
     * @param factory the collection factory
     * @param mode the write mode
     */
    public MongoBulkWriter(final MongoCollectionFactory<T> factory, final WriteMode mode) {
        notNull("factory", factory);
        this.collection = factory.getCollection();
        this.mode = mode;
    }

    @Override
    public void write(final Iterator<T> iterator) {
        List<WriteModel<T>> elements = new LinkedList<>();

        while (iterator.hasNext()) {
            elements.add(getWriteModel(iterator.next()));
        }

        boolean ordered = (this.mode == WriteMode.BULK_ORDERED_REPLACE || this.mode == WriteMode.BULK_ORDERED_UPDATE);

        this.collection.bulkWrite(elements, new BulkWriteOptions().ordered(ordered));
    }

    /**
     * Creates a write model for a document based on the write mode.
     *
     * @param element the element from which to create the write model
     * @return the write model for the element
     */
    private WriteModel<T> getWriteModel(final T element) {
        WriteModel<T> model;

        BsonDocument bsonDocument = element.toBsonDocument(this.collection.getDocumentClass(), this.collection.getCodecRegistry());

        if (bsonDocument.containsKey("_id")) {
            model = (this.mode == WriteMode.BULK_ORDERED_REPLACE || this.mode == WriteMode.BULK_UNORDERED_REPLACE)
                    ? new ReplaceOneModel<>(new BsonDocument("_id", bsonDocument.get("_id")), element)
                    : new UpdateOneModel<>(new BsonDocument("_id", bsonDocument.get("_id")), new BsonDocument("$set", bsonDocument));
        } else {
            model = new InsertOneModel<>(element);
        }

        return model;
    }
}
