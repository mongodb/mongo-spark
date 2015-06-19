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
 */

package com.mongodb.spark;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import org.bson.Document;

import java.util.ArrayList;

/**
 * A simple implementation of a Gradebook that allows for insertion, updates, and queries.
 * Gradebook methods wrap MongoDB Java Driver API calls.
 */
public final class Gradebook {
    /**
     * Utility class should always be final and have a private constructor.
     */
    private Gradebook() {}

    /**
     * Inserts a document into the specified collection.
     *
     * @param collection    the collection to insert into
     * @param document      the document to insert
     */
    static void insert(final MongoCollection<Document> collection, final Document document) {
        collection.insertOne(document);
    }

    /**
     * Updates a document specified by the filter. The filter should select a single document.
     *
     * @param collection    the collection to insert into
     * @param filter        the query filter
     * @param update        the update document to apply
     */
    static void updateOne(final MongoCollection<Document> collection, final Document filter, final Document update) {
        collection.updateMany(filter, update);
    }

    /**
     * Queries the specified collection for all documents.
     *
     * @param collection    the collection to query
     * @return              the results of the query
     */
    static ArrayList<Document> find(final MongoCollection<Document> collection) {
        ArrayList<Document> results = new ArrayList<>();

        MongoCursor<Document> cursor = collection.find().iterator();

        while (cursor.hasNext()) {
            results.add(cursor.next());
        }

        cursor.close();

        return results;
    }

    /**
     * Queries the specified collection for documents that match on the specified filter.
     *
     * @param collection    the collection to query
     * @param filter        the query filter
     * @return              the results of the query
     */
    static ArrayList<Document> find(final MongoCollection<Document> collection, final Document filter) {
        ArrayList<Document> results = new ArrayList<>();

        MongoCursor<Document> cursor = collection.find(filter).iterator();

        while (cursor.hasNext()) {
            results.add(cursor.next());
        }

        cursor.close();

        return results;
    }
}
