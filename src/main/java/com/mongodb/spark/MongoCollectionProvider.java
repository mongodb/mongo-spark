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

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.io.Serializable;

/**
 * Interface for a mongo collection provider. Used when computing partitions for RDDs.
 *
 * @param <T> type of objects in the collection
 */
public interface MongoCollectionProvider<T> extends Serializable {
    /**
     * Gets a mongo collection from the provider.
     *
     * @return a mongo collection
     */
    MongoCollection<T> getCollection();

    /**
     * Gets a mongo database from the provider.
     *
     * @return a mongo database
     */
    MongoDatabase getDatabase();

    /**
     * Gets a mongo client from the provider.
     *
     * @return a mongo client
     */
    MongoClient getClient();
}
